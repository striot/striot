{-# LANGUAGE TemplateHaskell #-}

module WearableExample ( sampleDataGenerator
                       , PebbleMode60
                       , sampleInput
                       , intSqrt
                       , threshold

                       -- used by WearableStream
                       , edEvent
                       , stepEvent

                       -- used by Criterion for benchmarking
                       , parseTimeField
                       , parseSessionLine

                       -- used by Main.hs for StreamGraph
                       , pebbleTimes
                       , wearablePreSource

                       -- used by Main and WearableStats
                       , session1Input
                       , avgArrivalRate
                       , vibeFrequency

                       ) where

import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing

import Control.Concurrent
import Control.Monad (replicateM)
import System.Random
import System.IO
import System.Posix.IO -- openFd, OpenFileFlags, stdInput
import Data.Time
import Data.Time.Calendar
import Data.Time.Clock
import Data.Fixed

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
threshold = 100 -- reasonable number for the sample data generator

stepEvent :: Int -> Stream Int -> Stream Int -- input is (ed,ts)
stepEvent thr = streamFilterAcc (\last new -> new) 0 (\new last ->(last>thr) && (new<=thr))

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
  print.take 100 $ stepCount $ stepEvent threshold $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs

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
  print.take 100 $ stepEvent threshold $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs

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
  print.take 100 $ streamWindow (chopTime 120) $ stepEvent threshold $ edEvent $ sampleDataGenerator jan_1_1900_time 10 rs

sampleInput :: IO PebbleMode60
sampleInput = do
  rands <- replicateM 4 (getStdRandom (randomR (0,99)) :: IO Int)
  let xyz = (rands !! 0, rands !! 1, rands !! 2)
      vibe = fromEnum (rands !! 3 < 10)
      payload = (xyz, vibe)
  print $ "emitting " ++ (show payload)
  threadDelay (1000*1000 `div` 25) -- sleep to approximate 25Hz emission rate
  return payload

------------------------------------------------------------------------------
-- CSV processing

-- convert CSV timestamp field (ms since epoch) to Timestamp
-- See also https://www.williamyaoh.com/posts/2019-09-16-time-cheatsheet.html
parseTimeField :: String -> UTCTime
parseTimeField timestamp = let -- s is e.g. "1529718763606"
  (sS,mS) = splitAt 10 timestamp
  ts = parseTimeOrError True defaultTimeLocale "%s" sS  :: UTCTime
  ms = parseTimeOrError True defaultTimeLocale "%s" ("0.00"++mS)
  in addUTCTime ms ts


-- special window maker to set event timestamps from source data
-- used by Main.hs for StreamGraph and Criterion for benchmarking
pebbleTimes :: Stream (Timestamp,PebbleMode60) -> [Stream (Timestamp,PebbleMode60)]
pebbleTimes = map (\(Event _ v) -> [Event (fmap fst v) v])

-- Exported for the StreamGraph example (Main.hs)
wearablePreSource = do
    fd <- openFd "session1.csv" ReadOnly Nothing (OpenFileFlags False False False False False)
    dupTo fd stdInput

-- parses a line from "session"-format CSV: timestamp,((x,y,z),vibe)
-- format:  one reading per line, unix epoch timestamp,((x,y,z),vibe)
-- example: 1529598787929,((-8,-16,-1000),0)
-- Exported for Criterion benchmarking
parseSessionLine :: String -> (UTCTime, PebbleMode60)
parseSessionLine line = let
  ts = parseTimeField (take 13 line)
  p  = read (drop 14 line) :: PebbleMode60
  in (ts,p)

-- reading from stdin
-- Exported for the StreamGraph example (Main.hs)
session1Input :: IO (Timestamp,PebbleMode60)
session1Input = do
  threadDelay 1000 -- µs
  getLine >>= return . parseSessionLine

-- pre-calculated properties of the "session1" data-set
avgArrivalRate = 20.947272312257475 -- calculated by arrivalRate''
vibeFrequency  = 1-(5/275310) -- length $ filter (=='1') $ map (\s ->s!!((length s)-2)) (csvLines)
