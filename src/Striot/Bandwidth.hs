{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}
{-|
Module      : Striot.Bandwidth
Description : StrIoT Bandwidth Cost Model calculations
Copyright   : © StrIoT maintainers, 2022
License     : Apache 2.0
Maintainer  : StrIoT maintainers
Stability   : experimental

Experimental routines for reasoning about bandwidth.

-}
module Striot.Bandwidth ( howBig
                        , knownEventSizes
                        , departRate
                        , chopSize
                        , whatBandwidth
                        , whatBandwidthWeighted
                        , connectedToSources
                        , overBandwidthLimit
                        ) where

import Striot.CompileIoT
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.StreamGraph
import Striot.Jackson

import Algebra.Graph
import Data.Maybe (fromJust, mapMaybe, catMaybes)
import Data.Time (addUTCTime) -- UTCTime (..),addUTCTime,diffUTCTime,NominalDiffTime,picosecondsToDiffTime, Day (..))
import Data.Store (Store)
import Test.Framework
import Data.Function ((&))
import Data.List.Unicode

import qualified Data.Store.Streaming as SS
import qualified Data.ByteString as B

--import Data.Time

-- Given an Event, how big is it in bytes for on-wire
-- transfer?
howBig :: Store a => Event a -> Int
howBig = B.length . SS.encodeMessage . SS.Message

-- except StreamGraph does not have Events in it, the types are in
-- Strings.
-- from the POV of a StreamGraph we do not know if Events have timestamps
-- so assume they do. The following figures calculated using the above

e :: Store a => a -> Int
e = howBig . Event (Just (addUTCTime 0 (read "2013-01-01 00:00:00 +0000"))) . Just

-- these are types copied from examples/wearable. Longer term we should
-- accept user-provided event sizes
type AccelVal = Int
type Accelerometer = (AccelVal,AccelVal,AccelVal) -- X,Y,Z accelerometer values
type Vibe = Int
type PebbleMode60 = (Accelerometer,Vibe)

-- limited to boxed types, and non-list types. Very limited!
knownEventSizes =
  [ ("Int",           e (2 :: Int))
  , ("Double",        e (2 :: Double))
  , ("Char",          e 'c')
  , ("String1",       e "c")
  , ("String2",       e "cc")
  , ("String3",       e "ccc")
  , ("(Int,Int,Int)", e ((1,2,3)::(Int,Int,Int)))
  , ("PebbleMode60",  e (((0,0,0),0) :: PebbleMode60))
  ]

-- XXX copy in Orchestration too. put in Util
toFst :: (a -> b) -> a -> (b, a)
toFst f a = (f a, a)

------------------------------------------------------------------------------
-- test data

v1 = StreamVertex 1 (Source 2) []    "Int" "Int" 0
v2 = StreamVertex 2 Map    [[| id |]]        "Int" "Int" 1
v3 = StreamVertex 3 (Source 1) []    "Int" "Int" 2
v4 = StreamVertex 4 Map    [[| id |]]        "Int" "Int" 3
v5 = StreamVertex 5 Merge  []                "[Int]" "Int" 4
v6 = StreamVertex 6 Sink   [[| mapM_ print|]] "Int" "IO ()" 5
graph :: StreamGraph
graph = overlay (path [v3, v4, v5]) (path [v1, v2, v5, v6])

------------------------------------------------------------------------------
-- arrival/departure time

parentsOf :: StreamGraph -> Int -> [Int]
parentsOf = flip f where
  f i = map (vertexId . fst) . filter ((==i) . vertexId . snd) . edgeList

departRate :: StreamGraph -> Int -> Double
departRate g i = let
  vs = map (toFst vertexId) (vertexList g)
  v  = (fromJust . lookup i) vs

  ps = parentsOf g i
  p  = head ps
  p2 = last ps

  in case operator v of
    Source d    -> d
    Merge       -> sum (map (departRate g) ps)
    Join        -> min (departRate g p) (departRate g p2)
    Filter r    -> r * (departRate g p)
    FilterAcc r -> r * (departRate g p)

    Window      -> let params = (words . showParam . head . parameters) v in
                   if   "chopTime" == head params
                   then let ms = read (params !! 1)
                            s  = ms / 1000
                        in  1 / s
                   else departRate g p

    _           -> departRate g p

v7 = StreamVertex 7 (Filter 0.5) [] "Int" "Int" 7
v8 = StreamVertex 8 Join [] "Int" "(Int, Int)" 8
graph2 = overlay (path [v3, v4, v8]) (path [v1, v2, v8, v7, v6])

test_departRate_merge = assertEqual 3.0 $ departRate graph 6
test_departRate_join  = assertEqual 1.0 $ departRate graph2 8
test_departRate_filter= assertEqual 0.5 $ departRate graph2 7

v9 = StreamVertex 9 Window [[| chopTime 120 |]] "a" "[a]" 9

graph3 = path [v1, v2, v9, v7, v6]

test_departRate_window = assertEqual (1/0.12) $ departRate graph3 9

------------------------------------------------------------------------------
-- how big is the payload for a streamWindow (chopTime)?
-- could I leverage whatBandwidth to simplify parent calcs?
-- XXX no accounting for any serialization overhead for the list structure
chopSize :: StreamGraph -> Int -> Int -> Maybe Double
chopSize g i ms = let
  pid            = parentsOf g i & head
  pv             = (head . filter ((==pid) . vertexId) . vertexList) g
  maybePsize     = lookup (outtype pv) knownEventSizes
  prate          = departRate g pid -- Double
  pcount         = 1.0/prate
  s              = (fromIntegral ms) / 1000
  eventsInWindow = s/pcount

  in fmap ((*eventsInWindow).fromIntegral) maybePsize

-- what is the bandwidth out of a StreamVertex
whatBandwidth :: StreamGraph -> Int -> Maybe Double
whatBandwidth g i =
  let v       = (head . filter ((==i) . vertexId) . vertexList) g
      outrate = departRate g i
      params  = (words . showParam . head . parameters) v
      outsize = if   operator v == Window && "chopTime" == head params
                then chopSize g i (read (params !! 1))
                else fmap fromIntegral $ lookup (outtype v) knownEventSizes
  in fmap ((*outrate)) outsize

-- applies a rate-based overhead weighting
-- TCP header size is 20-40 bytes; IP header size is 20-40 bytes
weighting = 60.0
whatBandwidthWeighted g i = fmap (+ (departRate g i * weighting)) (whatBandwidth g i)

-- XXX: write an "departSize"? we could estimate window sizes for fixed-length
-- or time-bound windows for example. Joins approx double, etc

-- | Does this 'Plan' breach a bandwidth limit?
overBandwidthLimit :: Plan -> Double -> Bool
overBandwidthLimit (Plan sg pm) bandwidthLimit = let
  sourceIds = (map vertexId . filter (isSource . operator) . vertexList) sg
  connected = connectedToSources sourceIds pm

  in edgeList sg
    & filter ((connected ∋) . vertexId . fst) -- edges from source Partition
    & filter ((connected ∌) . vertexId . snd) -- not terminating there
    & map fst -- source vertex
    & mapMaybe (fmap (>bandwidthLimit) . whatBandwidthWeighted sg . vertexId)
    & or

-- XXX: tests or overBandwidthLimit

test_overBandwidthLimit = assertBool $ overBandwidthLimit (Plan graph [[1,2],[3,4],[5,6]]) 29

-- | Provide a flattened list of node IDs from a PartitionMap which are
-- connected to a source node within a partition.
connectedToSources :: [Partition] -> PartitionMap -> [Partition]
connectedToSources sources =
  concat . filter (not . null . filter (sources ∋))

test_connectedToSources = assertEqual [1,2,3,4] $ connectedToSources [1,3] [[1,2],[3,4],[5,6,7]]
test_connectedToSources2= assertEqual [1,2]     $ connectedToSources   [1] [[1,2],[3,4],[5,6,7]]
test_connectedToSources3= assertEqual [5,6,7]   $ connectedToSources   [7] [[1,2],[3,4],[5,6,7]]
test_connectedToSources4= assertEqual []        $ connectedToSources   [0] [[1,2],[3,4],[5,6,7]]
