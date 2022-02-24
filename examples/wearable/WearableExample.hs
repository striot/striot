{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE TemplateHaskell #-}

module WearableUseCaseCloudCom2 where

import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Bandwidth
import Striot.Orchestration
import Striot.StreamGraph
import Striot.CompileIoT (createPartitions)
import Striot.VizGraph -- debug
import Striot.LogicalOptimiser -- debug

import Algebra.Graph
import Test.Framework
import System.Random
import Data.Time (UTCTime)
import Data.Time.Calendar
import Data.Time.Clock
import Data.Char (isLower)

import Data.Function ((&))
import Data.List (sort, nub)
import Data.Maybe (fromJust)

type AccelVal = Int

type Accelerometer = (AccelVal,AccelVal,AccelVal) -- X,Y,Z accelerometer values
type Vibe = Int
type PebbleMode60 = (Accelerometer,Vibe)

{-
Query 1 includes a user-defined function (UDF) to read the data from the accelerometer. It has two parameters: the
sampling frequency and the mode of operation. The mode of operation is 6 bits that determine which of the full set of
possible data fields should be generated on the device. Mode 60, used in the
running example, projects the first four event fields (x,y,z, and vibe).

INSERT INTO AccelEvent
SELECT getAccelData(25, 60)
FROM AccelEventSource
-}

{-
Query 2 calculates the Euclidean distance from the raw accelerometer data stream for all of the data samples where
the vibe parameter is set to 0

INSERT INTO EdEvent
SELECT Math.pow(x*x + y*y + z*z, 0.5) AS ed, ts
FROM AccelEvent WHERE vibe = 0
-}
edEvent :: Stream PebbleMode60 -> Stream Int -- output is ed
edEvent s = streamMap (\((x,y,z),vibe)-> intSqrt (x*x+y*y+z*z)) $ streamFilter (\((x,y,z),vibe)->vibe == 0) s

intSqrt :: Int -> Int
intSqrt = round . sqrt . fromIntegral

{-
Query 3 detects any spike crossing the specified threshold.

INSERT INTO StepEvent
SELECT ed1(’ts’) as ts FROM EdEvent
MATCH RECOGNIZE (MEASURES A AS ed1, B AS ed2 PATTERN (A B) DEFINE A AS (A.ed > THR),B AS (B.ed <= THR))
-}

threshold :: Int
threshold = 5000 -- made up number

stepEvent :: Stream Int -> Stream Int -- input is (ed,ts)
stepEvent s = streamFilterAcc (\last new -> new) 0 (\new last ->(last>threshold) && (new<=threshold)) s

{-
Query 4 aggregates the input information and - based on a tumbling
window regularly sends to Query 5 an event containing the
number of steps taken.

INSERT INTO StepCount
SELECT count(*) AS steps
FROM StepEvent.win:time batch(120 sec)
-}
stepCount :: Stream Int -> Stream Int
stepCount s = streamMap length $ streamWindow (chopTime 120) s -- this is in milli seconds
{-
5)
SELECT persistResult(‘steps’, ‘step sum’,‘time series’) FROM StepCount
-}

--- Test on examples
-- First generate random events
-- Create Events with timestamps
-- https://stackoverflow.com/questions/33117152/haskell-convert-time-milliseconds-to-utctime

jan_1_1900_day :: Day
jan_1_1900_day = fromGregorian 1900 1 1
-- 1/1/1900 as a UTCTime
jan_1_1900_time :: UTCTime
jan_1_1900_time = UTCTime jan_1_1900_day 0 -- gives example time for the first event


sampleDataGenerator :: UTCTime -> Int -> [Int] -> Stream PebbleMode60 -- Start Time -> Interval between events in ms ->
                                                                          -- List of random numbers -> Events
sampleDataGenerator start interval rands =
         Event
            (Just start)
            (Just ((rands !! 0, rands !! 1, rands !! 2)
            , if rands !! 3 < 10 then 1 :: Int else 0 :: Int))
        : sampleDataGenerator
              (addUTCTime (toEnum (interval * 10 ^ 9)) start)
              interval
              (drop 5 rands)

---
main :: IO ()
main = do
  g <- getStdGen
  let rs = randomRs (0,99) g :: [Int]
  print.take 100 $ stepCount $ stepEvent $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs

------------- Tests for debugging----------------------------------
main2 :: IO ()
main2 = do
  g <- getStdGen
  let rs = randomRs (0,99) g :: [Int]
  print.take 100 $ sampleDataGenerator jan_1_1900_time 10 rs

main3 :: IO ()
main3 = do
  g <- getStdGen
  let rs = randomRs (0,99) g :: [Int]
  print.take 100 $ stepEvent $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs

main4 :: IO ()
main4 = do
  g <- getStdGen
  let rs = randomRs (0,99) g :: [Int]
  print.take 100 $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs

main5 :: IO ()
main5 = do
  g <- getStdGen
  let rs = randomRs (0,99) g :: [Int]
  print.take 100 $ streamFilter (\[ed1,ed2]-> (ed1>threshold) && (ed2<=threshold)) $ streamWindow (sliding 2) $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs

main6 :: IO ()
main6 = do
  g <- getStdGen
  let rs = randomRs (0,99) g :: [Int]
  print.take 100 $ streamWindow (chopTime 120) $ stepEvent $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs


-- corresponding to "main"
graph = path
  [ StreamVertex 1 (Source 1)      [[|sampleDataGenerator jan_1_1900_time 10 rs|]]
                                                                             "IO ()"        "PebbleMode60"   1
    -- edEvent (euclidean distance)
  , StreamVertex 2 (Filter 0.5)    [[| (\((x,y,z),vibe)->vibe == 0) |]]      "PebbleMode60"  "PebbleMode60"  1
  , StreamVertex 3 Map             [[| \((x,y,z),_) -> (x*x,y*y,z*z)     |]] "PebbleMode60"  "(Int,Int,Int)" 1
  , StreamVertex 4 Map             [[| \(x,y,z)     -> intSqrt (x+y+z)   |]] "(Int,Int,Int)" "Int"           2

    -- stepEvent
  , StreamVertex 5 (FilterAcc 0.5) [[| (\last new -> new) |]
                                   ,[| 0 |]
                                   ,[| (\new last ->(last>threshold) && (new<=threshold)) |]
                                   ]                                         "Int"           "Int"           0.1
    -- stepCount
  , StreamVertex 6 Window          [[| chopTime 120 |]]                      "a"             "[a]"           0
  , StreamVertex 7 Map             [[| length |]]                            "[Int]"         "Int"           0

  , StreamVertex 8 Sink            [[| print.take 100 |]]                    "Int"           "IO ()"         0
  ]

plan9 = applyRule mapWindow
      . applyRule filterAccWindow
      $ graph

-- what the current optimiser chooses
opt    = fst $ chopAndChange defaultOpts graph
--XXX draw partitioned graph

-- hand-coded version of what we want
plan9part = [[1,2,3,4],[5,6,7,8,9]]
plan9p =createPartitions plan9 plan9part
plan9cost = planCost defaultOpts plan9 plan9part

opts = defaultOpts { rules = filterAccWindow : rules defaultOpts }

------------------------------------------------------------------------------
-- here temporarily

-- adapted from a version in LogicalOptimiser
isTypeVariable :: String -> Bool
isTypeVariable [] = False
isTypeVariable (c:cs) | c `elem` "([" = isTypeVariable cs
                      | otherwise     = isLower c

hasTypeVars :: StreamGraph -> Bool
hasTypeVars g = vertexList g
              & map (\v -> (intype v, outtype v))
              & map (\(a,b) -> isTypeVariable a || isTypeVariable b)
              & or

-- rewrite rule approach
removeTypeVariables :: RewriteRule
removeTypeVariables (Connect (Vertex a) (Vertex b)) =
    if   isTypeVariable (intype b) && (not . isTypeVariable . outtype) a
    then Just (replaceVertex b b { intype = outtype a })
    -- we might be copying a type var from outtype a here. Whether this works
    -- is going to depend on what order the edges are traversed
    else Nothing

removeTypeVariables _ = Nothing

------------------------------------------------------------------------------
-- confirm that plan9 is a winning rewrite

test_plan9_winner = assertElem plan9 $ (map (fst.fst) . filter ((<= plan9cost) . snd) . viableRewrites opts) graph
