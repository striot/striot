{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}

{-
 - Generate a docker-compose compose.yml file
 -}

module Striot.CompileIoT.Compose
  ( generateDockerCompose
  , getConsumers
  ) where

import Striot.StreamGraph
import Striot.CompileIoT
import Algebra.Graph
import Data.List (intersect, null)
import Test.Framework
import Data.Maybe (fromJust)

-- | Generate a docker-compose-format compose.yml in a String, encoding the
-- Node inter-connections from the supplied PartitionedGraph as dependencies,
-- such that consumer Nodes will be started prior to the producer Nodes that
-- connect to them.
generateDockerCompose :: PartitionedGraph -> String
generateDockerCompose pg@(sgs,_) = "services:\n" ++
    concatMap (nodeToCompose pg) [1 .. (length sgs)]

-- | Generate a YAML snippet in a String corresponding to the docker-compose
-- stanza for the Node in the PartitionedGraph at index i (using 1-indexing).
nodeToCompose :: PartitionedGraph -> Int -> String
nodeToCompose pg i = concat
  [ "    ", n, ":\n",
    "        build: ", n, "\n",
    "        tty: true\n",
    deps
  ] where
    n = "node" ++ (show i)
    deps = concatMap (\d -> "        depends_on:\n"
                         ++ "        - node" ++ (show d) ++ "\n")
                     (map fst (getConsumers pg i))
                     -- this relies on length getConsumers ∈ [0,1]

-- | Return a list of numbered StreamGraphs which are downstream from the
-- StreamGraph at the given index. This list can only be 0 or 1 element long.
getConsumers :: PartitionedGraph -> Int -> [(Int, StreamGraph)]
getConsumers (sgs,cuts) i = let
  i' = i - 1 -- i is 1-indexed; we need 0-indexed
  vs = vertexList (sgs !! i')
  receivingVertices =
    (map snd . filter (\(a,b) -> a `elem` vs) . edgeList) cuts
  nsgs = zip [1..] sgs
  in filter (not . null . intersect receivingVertices . vertexList . snd) nsgs

-- test data, derived from examples/merge
v1 = StreamVertex 1 (Source 1) [] "String" "String" 0
v2 = StreamVertex 2 (Source 1) [] "String" "String" 0
v3 = StreamVertex 3 (Source 1) [] "String" "String" 0
v4 = StreamVertex 4 Merge [] "String" "String" 1
-- XXX: ^ we lie about the input type here, because the generated function has split-out arguments
v5 = StreamVertex 5 Sink [[| mapM_ print |]] "String" "IO ()" 0

graph = (overlays (map vertex [v1,v2,v3]) `connect` (vertex v4)) `overlay` path [v4,v5]
parts = [[1],[2],[3],[4,5]]
pg    = createPartitions graph parts

test_getConsumers_1 = assertNotEmpty $ getConsumers pg 1
test_getConsumers_2 = assertNotEmpty $ getConsumers pg 2
test_getConsumers_3 = assertNotEmpty $ getConsumers pg 3
test_getConsumers_4 = assertEmpty    $ getConsumers pg 4

test_getConsumers_2a = assertEqual [4] $ map fst (getConsumers pg 2)
