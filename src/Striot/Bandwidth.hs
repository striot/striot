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
                        , departTime
                        , whatBandwidth
                        ) where

import Striot.FunctionalIoTtypes
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

-- what is the bandwidth out of a StreamGraph source

--whatBandwidth :: StreamGraph -> Int -> ?
whatBandwidth g sId = let
  vs = map (toFst vertexId) (vertexList g)
  mm = fromJust $ lookup sId vs  -- :: StreamVertex
  in mm
-- we need to do the matrix from Jackson since we might be looking for
-- rates downstream of a filter


------------------------------------------------------------------------------
-- test data

v1 = StreamVertex 1 (Source 1) []    "String" "String" 0
v2 = StreamVertex 2 Map    [[| id |]]        "String" "String" 1
v3 = StreamVertex 3 (Source 1) []    "String" "String" 2
v4 = StreamVertex 4 Map    [[| id |]]        "String" "String" 3
v5 = StreamVertex 5 Merge  []                "[String]" "String" 4
v6 = StreamVertex 6 Sink   [[| mapM_ print|]] "String" "IO ()" 5
graph :: StreamGraph
graph = overlay (path [v3, v4, v5]) (path [v1, v2, v5, v6])
