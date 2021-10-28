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
import Data.List (nub, sortOn, sort)
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

toFst :: (a -> b) -> a -> (b, a)
toFst f a = (f a, a)

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
-- a 'Maybe'. 'sumUtility' returns Nothing if the pairing is not viable,
-- either due to an over-utilised operator or an over-utilised Partition.
--
sumUtility :: StreamGraph -> PartitionMap -> Score
sumUtility sg pm = let
    oi = calcAllSg sg
    in if isOverUtilised oi
       then Nothing

       -- filter our Partition over-utilisation
       else if   any (>partitionUtilThreshold) (sumUtilPartitions oi pm)
            then Nothing

            else let graphScore = sum . map util $ oi
                     partScore  = fromIntegral (length pm)
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

------------------------------------------------------------------------------
-- Rejecting over-utilised Partitions

partitionUtilThreshold = 3.0 -- finger in the air

sumUtilPartitions :: [OperatorInfo] -> PartitionMap -> [Double]
sumUtilPartitions oi pm =
    let oi' = map (toFst opId) oi -- :: [(Int, OperatorInfo)]
    in map (sumUtilPartition oi') pm

sumUtilPartition :: [(Int, OperatorInfo)] -> [Partition] -> Double
sumUtilPartition opInfo =
        sum . map (util . fromJust . (flip lookup) opInfo)

partUtilGraph = simpleStream
    [ ( Source 1, [[| tempSensor  |]], "Int",    1 )
    , ( Map,      [[| farToCels   |]], "Int",    1 )
    , ( Filter 1, [[| over100     |]], "Int",    1 )

    , ( Map,      [[| farToCels   |]], "Int",    1 )
    , ( Filter 1, [[| over100     |]], "Int",    1 )
    , ( Map,      [[| farToCels   |]], "Int",    1 )

    , ( Filter 1, [[| over100     |]], "Int",    1 )
    , ( Map,      [[| farToCels   |]], "Int",    1 )
    , ( Sink,     [[| mapM_ print |]], "IO ()",  1 )
    ]

-- when partition over-utilisation is not considered, the cost model would
-- prefer a PartitionMap with the fewest partitions, which would be 2.
-- When Partition utilisation is capped by partitionUtilThreshold, the
-- minimum number of partitions of partUtilGraph is 3.

-- expensive to evaluate (1911ms)
test_overUtilisedPartition_minThreePartitions = assertBool $
    (not . any (<3) . map (length.snd.fst) . viableRewrites) partUtilGraph

test_overUtilisedPartition_rejected = -- example of an over-utilised partition
    assertNothing (sumUtility partUtilGraph [[1,2],[3,4,5,6,7,8,9]])

-- example of an acceptable PartitionMap
test_overUtilisedPartition_acceptable = assertElem [[1,2,3],[4,5,6],[7,8,9]]
    $ map (sort . (map sort))
    $ (map (snd.fst) . viableRewrites) partUtilGraph -- :: [PartitionMap]
