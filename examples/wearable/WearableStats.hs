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

thr = 1250 :: Int

graph = path
  [ StreamVertex 1 (Source avgArrivalRate) [[| session1Input |]]
     "IO ()" "PebbleMode60" 6666666.667

  , StreamVertex 2 (Filter vibeFrequency) [[| ((==0) . snd) |]]
    "PebbleMode60"  "PebbleMode60"  1293661.061

  -- edEvent
  , StreamVertex 3 Map [[| \((x,y,z),_) -> (x*x,y*y,z*z) |]]
    "PebbleMode60"  "(Int,Int,Int)" 1088139.282
  , StreamVertex 4 Map [[| \(x,y,z) -> intSqrt (x+y+z) |]]
    "(Int,Int,Int)" "Int" 294117.6471

  -- stepEvent
  , StreamVertex 5 (FilterAcc 0.020272) [[| (\_ n-> n) |], [| 0 |], [| (\new last ->(last>thr) && (new<=thr)) |]]
    "Int" "Int" 625000

  -- stepCount
  , StreamVertex 6 Window [[| chopTime 120 |]]
    "a" "[a]" 806451.6129
  , StreamVertex 7 Map [[| length |]]
    "[Int]" "Int" 6666666.667

  , StreamVertex 8 Sink [[| mapM_ print |]]
    "Int" "IO ()" 6666666.667
  ]

---- Evaluation Stage 1: no program rewrites

-- how many partitionings of the default program? 127
norewritePlans = makePlans graph
norewritePartitionCount = length $ norewritePlans

-- how many of those are not eliminated by maxNodeUtil (112)
relaxedOpts = opts { bandwidthLimit = 999999999 }
norewritePassMaxNodeUtil' o = norewritePlans
                            & map (toSnd (planCost o))
                            & filter (isJust . snd)

norewritePassMaxNodeUtil = norewritePassMaxNodeUtil' relaxedOpts
norewritePassMaxNodeUtilCount = length norewritePassMaxNodeUtil
norewriteRejectedMaxNodeUtil = -- 15
  norewritePartitionCount - norewritePassMaxNodeUtilCount

-- by default, the bandwidth limit above rules out all program variants
norewriteSurvivors    = norewritePlans
                      & map (toSnd (planCost opts))
                      & filter (isJust . snd)
norewriteSurviveCount = length norewriteSurvivors
test_defaultgraph_bwlimit = assertEqual 0 norewriteSurviveCount

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
  bar cost freq = "  \\bcbar[label="++(show cost)++"]{"++(show freq)++"}\n"
  bars          = map (uncurry bar) (Map.assocs freqs)
  n             = length bars `div` 2
  in "% this file is auto-generated by WearableExample.hs in Striot\n"
     ++ "\\begin{bchart}[step=10,max="++mostCommon++"]\n"
     ++ concat (take n bars)
     ++ "  \\bclabel{cost~~~~~}\n"
     ++ concat (drop n bars)
     ++ "  \\bcxlabel{number of plans}\n\
     \\\end{bchart}\n"

-- with Z plans all sharing the lowest-score of $wearableCountBestScoring
joint_winners :: [(Plan, Cost)]
joint_winners = let
  lowestCost = numplans3_maxnode & filter (isJust.snd) & map (fromJust.snd) & sort & head
  in filter ((==lowestCost).fromJust.snd) numplans3_maxnode

wearableCountBestScoring = length joint_winners

test_thereAreWinners = assertNotEmpty joint_winners

-- this is the winning rewrite we describe in the thesis text. The functions
-- below ensure that this appears in the output of the Logical Optimiser.
gvariant2 = graph
          & applyRule filterAccWindow
          & applyRule mapWindow

-- we want to select a variant equivalent to gvariant2. Therefore it must
-- be in the set of winners
-- joint_winners is non-empty (test_thereAreWinner will fail otherwise)
winner :: PartitionedGraph
winner = joint_winners
        & map fst
        & filter ((== gvariant2) . planStreamGraph)
        & head -- runtime error if gvariant2 is not in the list
        & (\(Plan s p) -> createPartitions s p)

wearableWinnerDot = partitionedGraphToDot winner

latexCommand name val = "\\newcommand{\\" ++ name ++ "}{"++ humanInt val ++"}\n"

humanInt :: Int -> String
humanInt = unpack . prettyI (Just ',')

wearableStats = "% this file is auto-generated by WearableExample.hs in Striot\n"
  ++ latexCommand "norewritePartitionCount"            norewritePartitionCount
  ++ latexCommand "norewriteRejectedMaxNodeUtil"       norewriteRejectedMaxNodeUtil
  ++ latexCommand "norewritePassMaxNodeUtilCount"      norewritePassMaxNodeUtilCount
  ++ latexCommand "norewriteSurviveCount"              norewriteSurviveCount
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

wearableParams = "  \\begin{tabular}{ll}\n"
 ++ "    Parameter      & Value         \\\\\n"
 ++ "    \\midrule\n"
 ++ "    maxNodeUtil    & "++(show (100*(maxNodeUtil opts)))++"\\%         \\\\\n"
 ++ "    bandwidthLimit & "++(show (bandwidthLimit opts))++"bytes/s \\\\\n"
 ++ "    rules          & defaultRewriteRules ++ reshapingRules \\\\\n"
 ++ "  \\end{tabular}\n"

-- outputs for Jon's thesis (graphs, statistics)
main = do
  writeFile "wearableStats.tex" wearableStats
  writeFile "wearable.dot" $ export (bandwidthStyle graph) graph
  writeGraph (export (bandwidthStyle graph)) graph "wearable.png"
  writeFile "wearable_winner.dot" wearableWinnerDot
  writeGraph id wearableWinnerDot "wearable_winner.png"
  writeFile "scoreBarChart.tex" chart
  writeFile "wearableParams.tex" wearableParams
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
