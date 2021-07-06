{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}
{-|
Module      : Striot.Orchestration
Description : StrIoT Orchestration end-user interface
Copyright   : © StrIoT maintainers, 2021
License     : Apache 2.0
Maintainer  : StrIoT maintainers
Stability   : experimental

The highest-level StrIoT interfaces, for transforming and partitioning
programs described as StreamGraphs.

-}
module Striot.Orchestration ( distributeProgram
                            , distributeProgram'
                            , viableRewrites
                            , deriveRewritesAndPartitionings
                            , allPartitionsPaired
                            , sumUtility

                            -- $fromCompileIoT
                            , simpleStream
                            , partitionGraph

                            , htf_thisModulesTests
                            ) where

import Algebra.Graph
import Data.List (nub, sortOn)
import Data.Maybe (fromJust, isJust)
import Test.Framework
import Data.Function ((&))
import Control.Arrow ((>>>))

import Striot.CompileIoT
import Striot.Jackson
import Striot.LogicalOptimiser
import Striot.Partition
import Striot.StreamGraph
import Striot.VizGraph

{- $fromCompileIoT
Functions re-exported from `Striot.CompileIoT`.
 -}

-- | The Score from applying a cost model (lower is better), wrapped in
-- 'Maybe' where 'Nothing' represents when the ('StreamGraph','PartitionMap')
-- is not viable.
type Score = Maybe (Double, Double)

-- TODO: a type for ('StreamGraph','PartitionMap')?


-- | Apply `distributeProgram'` to the supplied 'StreamGraph' to yield a
-- (possibly rewritten) 'StreamGraph' and 'PartitionMap' pair; partition
-- the graph accordingly; write out the generated source code files for
-- deployment.
distributeProgram :: StreamGraph -> GenerateOpts -> IO ()
distributeProgram sg opts = let
    (best,partMap) = distributeProgram' sg
    in partitionGraph best partMap opts

-- | apply 'viableRewrites' to the supplied 'StreamGraph'. Throw an error if
-- the result is an empty list. Otherwise, sort the results by their costings
-- and return the lowest-cost ('StreamGraph','PartitionMap') pairing.
distributeProgram' :: StreamGraph -> (StreamGraph, PartitionMap)
distributeProgram' sg = case viableRewrites sg of
    [] -> error "distributeProgram: no viable rewrites"
    rs -> rs & sortOn snd & head & fst

-- | Given a stream processing program encoded in a 'StreamGraph':
--
--   * generate derivative graphs via rewrite rules.
--   * generate all possible partitionings for each graph
--   * for each of these pairs:
--
--       * reject any pairings which are not viable
--       * Apply a cost model, based on queueing theory and minimising
--       the number of partitions
--
--   * return the ('StreamGraph','PartitionMap') pairings, paired with
--   with the score from applying the cost model.
viableRewrites :: StreamGraph -> [((StreamGraph, PartitionMap), Score)]
viableRewrites = deriveRewritesAndPartitionings
             >>> map (toSnd (uncurry sumUtility))
             >>> filter (isJust . snd)

test_viableRewrites_graph = assertNotEmpty $ viableRewrites graph
test_viableRewrites_tooMuch = assertEmpty  $ viableRewrites tooMuch

toSnd :: (a -> b) -> a -> (a, b)
toSnd f a = (a, f a)

-- | given a StreamGraph, derives further graphs by applying rewrite
-- rules and pairs them with all their potential partitionings
deriveRewritesAndPartitionings :: StreamGraph -> [(StreamGraph,PartitionMap)]
deriveRewritesAndPartitionings = concatMap allPartitionsPaired . nub . applyRules 5

-- | given a StreamGraph, generate all partitionings of it and pair
-- them individually with the StreamGraph.
allPartitionsPaired :: StreamGraph -> [(StreamGraph,PartitionMap)]
allPartitionsPaired sg = map (\pm -> (sg,pm)) (allPartitions sg)

-- | Return a score for a 'StreamGraph'/'PartitionMap' pair representing a
-- "cost" for the pair, where lower is better. The score is wrapped in
-- a 'Maybe'. 'sumUtility' returns Nothing if the pairing is not viable.
--
-- TODO what about no viable partitionings?? Can this happen?
sumUtility :: StreamGraph -> PartitionMap -> Score
sumUtility sg pm = let
    oi = calcAllSg sg
    in if isOverUtilised oi
       then Nothing
       else let graphScore = sum . map util $ oi
                numOps     = length oi
                partScore  = fromIntegral (length pm) / fromIntegral numOps
                in Just    $ (graphScore, partScore)

-- | fitness function for StreamGraphs. Is this a viable program?
viableStreamGraph :: [OperatorInfo] -> Bool
viableStreamGraph = not . isOverUtilised

------------------------------------------------------------------------------
-- test program taken from examples/filter/generate.hs

opts = defaultOpts { imports = imports defaultOpts ++ [ "System.Random" ] }

source = [| do
    i <- getStdRandom (randomR (1,10)) :: IO Int
    threadDelay 1000000
    putStrLn $ "client sending " ++ (show i)
    return i
    |]

v1 = StreamVertex 0 (Source 1)   [source]            "Int" "Int" 0
v2 = StreamVertex 1 (Source 1)   [source]            "Int" "Int" 0
v3 = StreamVertex 2 Merge        []                  "Int" "Int" 0
v4 = StreamVertex 3 (Filter 0.5) [[| (>3) |]]        "Int" "Int" 1
v5 = StreamVertex 4 Sink         [[| mapM_ print |]] "Int" "Int" 0

-- this graph is overutilised: arrival rate at filter = 2 events/tu, service time
-- 1 event/tu
graph = overlay (path [v1,v3,v4,v5]) (path [v2,v3])

tooMuch = simpleStream
    [ (Source 1, [[| return 1 |]],    "Int", 0)
    , (Map,      [[| (+1)     |]],    "Int", 2)
    , (Sink,     [[| mapM_ print |]], "Int", 0)
    ]

test_tooMuch_notviable = assertBool (not . viableStreamGraph . calcAllSg $ tooMuch)

main = htfMain htf_thisModulesTests
