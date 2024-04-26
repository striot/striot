{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE TemplateHaskell #-} -- needed?

-- Figures/stats for Jon's thesis
module WearableStats where

import Algebra.Graph.Export.Dot
import Algebra.Graph
import Data.Function ((&), on)
import Data.List (nub,sort,nubBy,intercalate)
import Data.Maybe (fromJust, isJust)
import Data.Text.Format.Numbers
import Data.Text (unpack)
import System.Directory
import Striot.CompileIoT
import Striot.LogicalOptimiser -- debug
import Striot.Orchestration
import Striot.StreamGraph
import Striot.VizGraph -- debug
import Test.Framework

import qualified Data.Map as Map

import WearableExample hiding (main)

toSnd :: (a -> b) -> a -> (a, b)
toSnd f a = (a, f a)

lrules = defaultRewriteRules ++ reshapingRules

opts = defaultOpts { maxNodeUtil    = 3.0
                   , bandwidthLimit = 30
                   , rules          = lrules
                   }

---- Evaluation Stage 1: no program rewrites

-- how many partitionings of the default program? 127
norewritePlans = makePlans graph
norewritePartitionCount = length $ norewritePlans

-- how many of those are not eliminated by maxNodeUtil (112)
relaxedOpts = opts { bandwidthLimit = 999999999 }
norewritePassMaxNodeUtil = norewritePlans
                         & map (toSnd (planCost relaxedOpts))
                         & filter (isJust . snd)
norewritePassMaxNodeUtilCount = length norewritePassMaxNodeUtil
norewriteRejectedMaxNodeUtil = -- 15
  norewritePartitionCount - norewritePassMaxNodeUtilCount

-- by default, the bandwidth limit above rules out all program variants
test_defaultgraph_bwlimit = assertEmpty $ norewritePlans
                                        & map (toSnd (planCost opts))
                                        & filter (isJust . snd)

---- Evaluation Stage 2: logical optimiser/rewrites

-- how many program variants are derived?
rewrites            = nubBy (on graphEq variantGraph) (rewriteGraph lrules graph)

rewriteVariantCount = length rewrites - 1 -- 57
plans               = concatMap makePlans (map variantGraph rewrites)
rewritePlanCount    = length plans -- 5718

-- This time, the \textit{maximum node utilisation} filter removes 4662 options,
numplans2_maxnode = plans
                  & map (toSnd (planCost relaxedOpts))
                  & filter (isJust . snd)
rewriteMaxNodePassCount   = length numplans2_maxnode-- 1056
rewriteMaxNodeRejectCount = rewritePlanCount - rewriteMaxNodePassCount -- 4662

  -- and the \textit{bandwidth} filter removes 1032, leaving $rewriteBandwidthPassCount
numplans3_maxnode :: [(Plan, Cost)]
numplans3_maxnode = plans & map (toSnd (planCost opts)) & filter (isJust . snd)
rewriteBandwidthPassCount   = length numplans3_maxnode -- 24
-- 1056 - 24 = 1032
rewriteBandwidthRejectCount = rewriteMaxNodePassCount - rewriteBandwidthPassCount

-- These score between X and Y (bar chart of score distribution)
scores = map (fromJust . snd) numplans3_maxnode :: [Int]
freqs  = foldr (\k -> Map.insertWith (+) k 1) Map.empty scores
chart  = let
  mostCommon    = (show . last . sort . Map.elems) freqs
  bar cost freq = "  \\bcbar[text="++(show cost)++"]{"++(show freq)++"}\n"
  bars          = map (uncurry bar) (Map.assocs freqs)
  n             = length bars `div` 2
  in "% this file is auto-generated by WearableExample.hs in Striot\n"
     ++ "\\begin{bchart}[step=1,max="++mostCommon++"]\n"
     ++ concat (take n bars)
     ++ "  \\bclabel{calculated cost}\n"
     ++ concat (drop n bars)
     ++ "  \\bcxlabel{number of plans}\n\
     \\\end{bchart}\n"

-- with Z plans all sharing the lowest-score of $wearableCountBestScoring
joint_winners :: [(Plan, Cost)]
joint_winners = let
  lowestCost = numplans3_maxnode & filter (isJust.snd) & map (fromJust.snd) & sort & head
  in filter ((==lowestCost).fromJust.snd) numplans3_maxnode

wearableCountBestScoring = length joint_winners

winner = case joint_winners of
  [] -> error "joint_winners is empty"
  ws -> ((\(Plan s p) -> createPartitions s p) .fst.head) ws

wearableWinnerDot = partitionedGraphToDot winner

-- what's the bandwidth for each?
-- via whatBandwidthWeighted
-- 70,35,31

latexCommand name val = "\\newcommand{\\" ++ name ++ "}{"++ humanInt val ++"}\n"

humanInt :: Int -> String
humanInt = unpack . prettyI (Just ',')

wearableStats = "% this file is auto-generated by WearableExample.hs in Striot\n"
  ++ latexCommand "norewritePartitionCount"            norewritePartitionCount
  ++ latexCommand "norewriteRejectedMaxNodeUtil"       norewriteRejectedMaxNodeUtil
  ++ latexCommand "norewritePassMaxNodeUtilCount"      norewritePassMaxNodeUtilCount
  ++ latexCommand "rewriteVariantCount"                rewriteVariantCount
  ++ latexCommand "rewritePlanCount"                   rewritePlanCount
  ++ latexCommand "rewriteMaxNodeRejectCount"          rewriteMaxNodeRejectCount
  ++ latexCommand "rewriteBandwidthRejectCount"        rewriteBandwidthRejectCount
  ++ latexCommand "rewriteBandwidthPassCount"          rewriteBandwidthPassCount
  ++ latexCommand "wearableCountBestScoring"           wearableCountBestScoring
  ++ latexCommand "wearableMinutesPerPlan"             wearableMinutesPerPlan
  ++ latexCommand "wearableManualTime"                 wearableManualTime

wearableMinutesPerPlan = 15
wearableManualTime = let
  minutes = wearableMinutesPerPlan * rewritePlanCount
  hours   = div minutes 60
  ftdays  = div hours 8
  in ftdays

-- outputs for Jon's thesis (graphs, statistics)
main = do
  writeFile "wearableStats.tex" wearableStats
  writeFile "wearable.dot" $ export (bandwidthStyle graph) graph
  writeGraph (export (bandwidthStyle graph)) graph "wearable.png"
  writeFile "wearable_winner.dot" wearableWinnerDot
  writeGraph id wearableWinnerDot "wearable_winner.png"
  writeFile "scoreBarChart.tex" chart
  createDirectoryIfMissing False "rewritten"
  mapM_ (\(i,v) -> 
     writeGraph streamGraphToDot v ("rewritten/"++(show i)++".png")
     ) (zip [1..] (map variantGraph rewrites))

  writeFile "wearableAppendix.tex" $ concat $ appendixHead : map (\(n,v) ->
    let s = intercalate ", " (variantSequence v)
        c = (length . vertexList . variantGraph) v
    in appendixFig n s c
    ) (zip [1..] rewrites)

  htfMain htf_thisModulesTests

appendixHead = "\\chapter{Wearable Example rewritten programs}\n\
\\\label{Appendix:WearableExample}\n"

appendixFig n s c = "\\newpage\n\\begin{figure}[H]\n\
\    \\centering\n\
\    "++(show c)++" nodes\\\\\n\
\    \\includegraphics[width=1.0\\linewidth]{wearableVariants/"++(show n)++"}\n\
\      \\caption{wearableVariants/"++(show n)++"\n"
     ++ s ++ "}\n\
\       \\label{fig:wearableVariants"++(show n)++"}\n\
\  \\end{figure}\n"
