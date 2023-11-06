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
import Striot.CompileIoT
import Striot.LogicalOptimiser -- debug
import Striot.Orchestration
import Striot.VizGraph -- debug
import Test.Framework

import qualified Data.Map as Map

import WearableExample

toSnd :: (a -> b) -> a -> (a, b)
toSnd f a = (a, f a)

lrules = LabelledRewriteRule "filterAccWindow" filterAccWindow : defaultRewriteRules

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
rewrites            = nubBy (on (==) variantGraph) (rewriteGraph lrules graph)

rewriteVariantCount = length rewrites - 1 -- 57
plans               = concatMap makePlans (map variantGraph rewrites)
rewritePlanCount    = length plans -- 5718

-- This time, the \textit{maximum node utilisation} filter removes 4662 options,
numplans2_maxnode = plans
                  & map (toSnd (planCost relaxedOpts))
                  & filter (isJust . snd)
rewriteMaxNodePassCount   = length numplans2_maxnode-- 1056
rewriteMaxNodeRejectCount = rewritePlanCount - rewriteMaxNodePassCount -- 4662

  -- and the \textit{bandwidth} filter removes 1032, leaving 24
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

-- with Z plans all sharing the lowest-score of 2.
joint_winners :: [(Plan, Cost)]
joint_winners = filter ((==2).fromJust.snd) numplans3_maxnode
-- length joint_winners = 2

winner = ((\(Plan s p) -> createPartitions s p) .fst.head) joint_winners
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

-- outputs for Jon's thesis (graphs, statistics)
generateThesisArtefacts = do
  writeFile "wearableStats.tex" wearableStats
  writeGraph (export enumGraphStyle) graph "wearable.png"
  writeFile "wearable_winner.dot" wearableWinnerDot
  writeGraph id wearableWinnerDot "wearable_winner.png"
  writeFile "scoreBarChart.tex" chart
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
