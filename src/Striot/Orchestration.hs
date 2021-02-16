{-# OPTIONS_GHC -F -pgmF htfpp #-}
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
                            , partitionGraph

                            , htf_thisModulesTests
                            ) where

import Algebra.Graph
import Data.List (nub)
import Data.Maybe (listToMaybe)
import Test.Framework
import Data.Function ((&))

import Striot.CompileIoT
import Striot.Jackson
import Striot.LogicalOptimiser
import Striot.Partition
import Striot.StreamGraph
import Striot.VizGraph

-- | Given a stream processing program encoded in a StreamGraph:
-- - * generate derivative programs via rewrite rules.
-- - * Apply queuing analysis to the resulting graphs,
-- - * reject any graphs which are not viable
-- - * pick a program with the lowest overall utility.
-- - * determine the lowest number of nodes the program could be deployed upon,
-- - * partition the program accordingly.

distributeProgram :: StreamGraph -> GenerateOpts -> IO ()
distributeProgram sg opts = let
    (u,best) = case filterViableBestUtility sg of
        Nothing -> error "distributeProgram: no viable StreamGraphs"
        Just x  -> x

    partMap = case bestPartitioning (ceiling u) best of
        Nothing -> error "distributeProgram: no viable Partitionings"
        Just x  -> x

    in partitionGraph best partMap opts

-- | runs calculations over supplied streamgraphs and filters them for viability
viableRewrites :: StreamGraph -> [([OperatorInfo],StreamGraph)]
viableRewrites = filter (viableStreamGraph . fst)
               . map (\s -> (calcAllSg s, s))
               . nub
               . applyRules 5

test_viableRewrites_graph = assertNotEmpty $ viableRewrites graph 
test_viableRewrites_tooMuch = assertEmpty  $ viableRewrites tooMuch

-- | Generate viable rewrites of the supplied StreamGraph. If there aren't
-- any, return Nothing. Otherwise, return the best according to our Cost
-- Model (lowest total utility), paired with its total utility.
filterViableBestUtility :: StreamGraph -> Maybe (Double, StreamGraph)
filterViableBestUtility sg = let
    graphs = map (\(ois,s) -> (sumUtility ois, s)) (viableRewrites sg)
    in if [] == graphs
        then Nothing
        else Just (minimum graphs)

test_filterViableBestUtility_graph   = assertJust    $ filterViableBestUtility graph
test_filterViableBestUtility_tooMuch = assertNothing $ filterViableBestUtility tooMuch

-- | Given a minimum number of nodes and a StreamGraph, calculate a
-- minimum-node PartitionMap of least minNodes nodes for the supplied
-- StreamGraph.
bestPartitioning :: Int -> StreamGraph -> Maybe PartitionMap
bestPartitioning minNodes sg = let
    parts = validPartitionings minNodes sg
    in if [] == parts
       then Nothing
       else (Just . minimum) parts

test_bestPartitioning_result  = assertJust    $ bestPartitioning n graph
    where n = length (vertexList graph)
test_bestPartitioning_nothing = assertNothing $ bestPartitioning (n+1) graph
    where n = length (vertexList graph)

-- | given a minimum number of nodes `minNodes` and a StreamGraph, return a
-- list of possible valid partitionings that are at least `minNodes` in length.
validPartitionings :: Int -> StreamGraph -> [PartitionMap]
validPartitionings minNodes = filter ((<=) minNodes . length) . allPartitions

-- allPartitions returns at least one mapping of size <4…
test_validPartitionings_preamble = assertNotEmpty
    (filter (<4) . map length . allPartitions $ graph)
-- …but validPartitionings 4 does not.
test_validPartitionings_minNodes = assertEmpty
    (filter (<4) . map length . validPartitionings 4 $ graph)

-- cost model (lower is better)
type CostModel = [OperatorInfo] -> Double

sumUtility :: CostModel
sumUtility = sum . map util

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

-- distributeProgram should find a rewrite which is not over-utilised,
-- and then a lowest-number-of-partitions partitioning, before performing
-- the code generation.
main = distributeProgram graph opts
