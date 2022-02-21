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
                        , whatBandwidth
                        ) where

import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.StreamGraph
import Striot.Jackson

import Algebra.Graph
import Data.Maybe (fromJust)
import Data.Time (addUTCTime) -- UTCTime (..),addUTCTime,diffUTCTime,NominalDiffTime,picosecondsToDiffTime, Day (..))
import Data.Store (Store)
import Test.Framework

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
e = howBig . Event (Just (addUTCTime 0 (read "2013-01-01 00:00:00"))) . Just

-- limited to boxed types, and non-list types. Very limited!
knownEventSizes =
  [ ("Int",           e (2 :: Int))
  , ("Double",        e (2 :: Double))
  , ("Char",          e 'c')
  , ("String1",       e "c")
  , ("String2",       e "cc")
  , ("String3",       e "ccc")
  , ("(Int,Int,Int)", e ((1,2,3)::(Int,Int,Int)))
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
                   then 1 / read (params !! 1)
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

test_departRate_window = assertEqual (1/120) $ departRate graph3 9

------------------------------------------------------------------------------

-- what is the bandwidth out of a StreamVertex
whatBandwidth :: StreamGraph -> Int -> Maybe Double
whatBandwidth g i = let
  outrate = departRate g i
  outType = (outtype . fromJust . lookup i . map (toFst vertexId) . vertexList) g
  in fmap ((*outrate) . fromIntegral) (lookup outType knownEventSizes)

-- XXX: write an "departSize"? we could estimate window sizes for fixed-length
-- or time-bound windows for example. Joins approx double, etc
