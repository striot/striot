{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE TemplateHaskell #-}

module Striot.Partition ( allPartitions
                        , singleton

                        , htf_thisModulesTests
                        ) where

import Striot.StreamGraph

import Algebra.Graph
import Algebra.Graph.ToGraph (reachable)
import Test.Framework

-- | Return a list of all possible valid partitionings of the `StreamGraph`.
-- A partitioning is a list of groupings of `Int`s corresponding to `vertexID`s
-- for the `StreamGraph`.
--
-- A valid partitioning is any which does not violate the following constraints:
--  * only one `Source` or `Sink` node can exist in each partition
--  * `Merge` operators must be the first in their partition
--
-- The validity rules are encoded in the `extendPartitioning` function.
allPartitions :: StreamGraph -> [[[Int]]]
allPartitions = (map.map.map) vertexId . foldgl fun [] . transpose
    where
        fun []      n = [[[n]]]
        fun choices n = concatMap (extendPartitioning n) choices

-- | A left-fold over Graphs. Unlike 'foldg', the traversal order follows
-- edges from a root node.
--
-- XXX could we rework this in terms of subGraph, instead of subGraphs?
-- Caveat: when the incoming graph is e.g. overlay x y, getRoot returns
-- one of x or y, and subGraphs returns [], so we lose the other branch
foldgl :: Eq a => Ord a =>
               (b -> a -> b) -> b -> Graph a -> b
foldgl f z g =
    if isEmpty g then z
    else let x  = getRoot g     -- :: a
             xs = subGraphs x g -- [Graph a]
         in foldl (\b g -> foldgl f b g) (f z x) xs

test_foldgl1 = assertEqual "ABC" $
    foldgl (\b a -> b++[a]) "" (path "ABC")
test_foldgl2 = assertEqual "ABCD" $
    foldgl (\b a -> b++[a]) "" (overlay (path "ABC") (path "BD"))

getRoots :: Eq a => Ord a =>
            Graph a -> [a]
getRoots g = let
    edges = edgeList g
    dests = map snd edges
    roots = filter (not.(`elem`dests)) (vertexList g)
    in roots

getRoot :: Eq a => Ord a =>
           Graph a -> a
getRoot = head . getRoots

subGraphs :: Ord a =>
             a -> Graph a -> [Graph a]
subGraphs n g = map (\k -> subGraph k g) (childrenOf n g)

test_subGraphs1 = assertEqual (subGraphs 2 t2) [Vertex v | v <- [3,4]]
test_subGraphs2 = assertEqual (subGraphs 3 t2) []
test_subGraphs3 = assertEqual (subGraphs 1 t1) [path [2,3]]
test_subGraphs4 = assertEqual (subGraphs 1 t2) [removeVertex 1 t2]

v0 = StreamVertex 0 (Source 1) [] "" "" 0
v1 = StreamVertex 1 Map [] "" "" 1
v2 = StreamVertex 2 Sink [] "" "" 2 
v3 = StreamVertex 3 (Source 1) [] "" "" 3
v4 = StreamVertex 4 Merge [] "" "" 4
v5 = StreamVertex 5 Map [] "" "" 5
g3 = overlay (path [v0, v1, v4, v2]) (path [v3, v5, v4])
g4 = transpose g3

test_subGraphs5 = let
    g = head $ subGraphs (getRoot g4) g4
    in assertBool $ v0 `elem` (vertexList g)

subGraph :: Eq a => Ord a =>
            a -> Graph a -> Graph a
subGraph n g = induce (`elem` reachable n g) g

t1 = path [1,2,3]
test_subGraph1 = assertEqual (subGraph 1 t1) $ t1
test_subGraph2 = assertEqual (subGraph 2 t1) $ path [2,3]
test_subGraph3 = assertEqual (subGraph 3 t1) $ Vertex 3

t2 = t1 + path [2,4]
test_subGraph4 = assertEqual (subGraph 4 t2) $ Vertex 4
test_subGraph5 = assertEqual (subGraph 2 t2) $ path [2,4] + path [2,3]

childrenOf :: Ord a =>
              a -> Graph a -> [a]
childrenOf n = map snd . filter ((==n).fst) . edgeList

-- | Given an existing partitioning and a new operator to consider: We can
-- start a new partition; in some circumstances we can also append the operator
-- to the last partition.
extendPartitioning :: StreamVertex -> [[StreamVertex]] -> [[[StreamVertex]]]
extendPartitioning n choice = let
  lastNode = last . last $ choice
  in if 1 < (length (filter singleton (n:(last choice))))
     || (operator lastNode == Merge || isSource (operator lastNode))
     then [ choice ++ [[n]] ]
     else [ choice ++ [[n]]
          , init choice ++ [last choice ++ [n]]
          ]

singleton v = operator v == Sink || isSource (operator v)

g' = path [ v0 , v1 , v2 ]
test_g' = assertEqual [ [[2],[1],[0]]
                      , [[2],[1,0]]
                      , [[2,1],[0]]]
    $ allPartitions g'

g2 = overlay (path [v0, v4, v2]) (path [v3, v4])

test_g2 = assertEqual [ [[2],[4],[0],[3]]
                      , [[2,4],[0],[3]]]
    $ allPartitions g2

test_g3 = assertEqual
    [ [[2],[4],[1],[0],[5],[3]]
    , [[2],[4],[1],[0],[5,3]]
    , [[2],[4],[1,0],[5],[3]]
    , [[2],[4],[1,0],[5,3]]
    , [[2,4],[1],[0],[5],[3]]
    , [[2,4],[1],[0],[5,3]]
    , [[2,4],[1,0],[5],[3]]
    , [[2,4],[1,0],[5,3]]
    ] $ allPartitions g3

main = htfMain htf_thisModulesTests