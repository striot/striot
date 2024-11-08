{-# LANGUAGE TemplateHaskell #-}

module WearableExample ( sampleDataGenerator
                       , PebbleMode60
                       , graph
                       , sampleInput
                       , intSqrt
                       , threshold

                       , preSource
                       , source
                       , csvLineToPebbleMode60s
                       , pebbleTimes

                       , pebbleStream

                       ) where

import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.StreamGraph
import Striot.Partition

import Striot.Simple

import Algebra.Graph
import Control.Concurrent
import Control.Monad (replicateM)
import System.Random
import System.IO
import System.Posix.IO -- openFd, OpenFileFlags, stdInput
import Data.Function ((&))
import Data.List -- intercalate
import Data.List.Split
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
threshold = 100 -- made up number

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

sampleInput :: IO PebbleMode60
sampleInput = do
  rands <- replicateM 4 (getStdRandom (randomR (0,99)) :: IO Int)
  let xyz = (rands !! 0, rands !! 1, rands !! 2)
      vibe = fromEnum (rands !! 3 < 10)
      payload = (xyz, vibe)
  print $ "emitting " ++ (show payload)
  threadDelay (1000*1000 `div` 25) -- sleep to approximate 25Hz emission rate
  return payload

-- corresponding to "main"
graph = path            -- 25 Hz, per Path2IOT paper
  [ StreamVertex 1 (Source 25)      [[| sampleInput |]]
                                                                             "IO ()"        "PebbleMode60"   25
    -- from sample dataset, 11 vibe events in 918150 samples
  , StreamVertex 2 (Filter (1-(11/918150))) [[| (\((x,y,z),vibe)->vibe == 0) |]] "PebbleMode60"  "PebbleMode60"  25

    -- edEvent (euclidean distance)
  , StreamVertex 3 Map             [[| \((x,y,z),_) -> (x*x,y*y,z*z)     |]] "PebbleMode60"  "(Int,Int,Int)" 25
  , StreamVertex 4 Map             [[| \(x,y,z)     -> intSqrt (x+y+z)   |]] "(Int,Int,Int)" "Int"           25

    -- stepEvent
  , StreamVertex 5 (FilterAcc 0.5) [[| (\last new -> new) |]
                                   ,[| 0 |]
                                   ,[| (\new last ->(last>threshold) && (new<=threshold)) |]
                                   ]                                         "Int"           "Int"           25
    -- stepCount
  , StreamVertex 6 Window          [[| chopTime 120 |]]                      "a"             "[a]"           25
  , StreamVertex 7 Map             [[| length |]]                            "[Int]"         "Int"           25

  , StreamVertex 8 Sink            [[| mapM_ print |]]                    "Int"           "IO ()"         25
  ]

------------------------------------------------------------------------------
-- CSV stuff

csv = "pebbleRawAccel_1806-02.csv"

preSource = do
    fd <- openFd csv ReadOnly Nothing (OpenFileFlags False False False False False)
    dupTo fd stdInput

-- each line has a timestamp followed by ten readings
csvLineToPebbleMode60s :: [String] -> [(Timestamp,PebbleMode60)]
csvLineToPebbleMode60s (timestamp:fields) =
  let ts = parseTimeField timestamp
  in take 10
    $ map (\(x:y:z:w:[]) -> (ts,((x,y,z),w)))
    $ chunksOf 4
    $ map (read :: String -> Int)
    $ fields

-- csvLines :: [String]
-- csvLines <- return . lines =<< readFile csv

-- produce one-record-per-line CSV for easier debugging
csvLineToRecordLines :: String -> [[String]]
csvLineToRecordLines line = let
  ts:fields = splitOn "," line
  in fields & chunksOf 4 & take 10 & map (ts:)

recordLineToPebbleMode60 csv = let
  [x,y,z,v] = map (read :: String -> Int) (tail csv)
  ts = parseTimeField (head csv)
  in (ts,((x,y,z),v))

{- 1529718763606 in milliseconds =
   Saturday, 23 June 2018 01:52:43.606 UTC
   See also
   https://www.williamyaoh.com/posts/2019-09-16-time-cheatsheet.html
 -}

-- convert CSV timestamp field (ms since epoch) to Timestamp
parseTimeField :: String -> UTCTime
parseTimeField timestamp = let -- s is e.g. "1529718763606"
  (sS,mS) = splitAt 10 timestamp
  ts = parseTimeOrError True defaultTimeLocale "%s" sS  :: UTCTime
  ms = parseTimeOrError True defaultTimeLocale "%s" ("0.00"++mS)
  in addUTCTime ms ts

source = do
  threadDelay 1000 -- µs
  getLine >>= return . csvLineToPebbleMode60s . splitOn ","

-- special window maker to set event timestamps from source data
pebbleTimes :: WindowMaker (Timestamp,PebbleMode60)
pebbleTimes = map (\(Event _ v) -> [Event (fmap fst v) v])

------------------------------------------------------------------------------
-- Explore the data-set using the Simple interface
--
-- mkStream :: [a] -> Stream a
-- unStream :: Stream a -> [a]

-- GHCi:
-- csvFile <- readFile csv

-- common bit of stream processing for the functions that follow.
-- this unpacks the ten readings on each CSV line into separate
-- `Event PebbleMode60` with Event timestamps corresponding to the
-- dataset.`
pebbleStream :: String -> Stream (Timestamp, PebbleMode60)
pebbleStream str = str
          & lines
          & concatMap csvLineToRecordLines
          & mkStream
          & streamMap recordLineToPebbleMode60
          & streamWindow pebbleTimes         -- :: [(Timestamp,PebbleMode60)]
          & streamExpand                     -- :: (Timestamp,PebbleMode60)
 
-- λ> pebbleStream csvFile & numberOfSamples 
-- 918150
numberOfSamples :: Stream (Timestamp, PebbleMode60) -> Int
numberOfSamples str = str
                    & streamScan (\c _ -> c+1) 0
                    & unStream
                    & last

-- this reports on the number of events grouped into windows of 1s.
-- TODO better would be a rolling recalculated average across the
-- whole data-set
-- this reports 20Hz
arrivalRate :: Stream a -> Int
arrivalRate str = str
                & streamWindow (chopTime 1000)
                & streamMap length
                & unStream
                & last

-- running average Hz
-- λ> pebbleStream csvFile & arrivalRate'
-- 23.51336816226183
-- chopTime emits 0-length lists for time intervals which do not have
-- any events in them. I.e.,
--  [10,10,10,40] & streamWindow (chopTime 10) => [[10,10,10],[],[],[40]]
-- two empty lists emitted for the time intervals +20 and +30. These
-- were emitted when the Event for interval +40 arrived.
-- filtering out the empty lists means we are measuring
-- the combined arrival rate of disjoint "sessions".
arrivalRate' :: Stream a -> Double
arrivalRate' str = str
                 & streamWindow (chopTime 1000)
                 & streamFilter (not . null) -- see above
                 & streamMap length
                 & streamScan (\(count,sum,_,_) n -> let
                   count' = count+1
                   sum' = sum+n
                   avg' = (fromIntegral sum') / (fromIntegral count')
                   in (count',sum',n,avg')) (0,0,0,0.0::Double)
                 & streamMap fth4
                 & unStream
                 & last

-- another arrivalRate, this time don't filter out empty windows.
-- For the whole CSV: 7.651951428880981
-- For example sub-"sessions":
--    1 20.947272312257475
--    2 20.89397496087637
--    3 18.902333621434746
--    4 23.577981651376145
--    5 20.6850436681222
--    6 12.249488752556237
arrivalRate'' :: Stream a -> Double
arrivalRate'' s = s
                & streamWindow (chopTime 1000)
                & streamMap length
                & streamScan (\(count,sum,_,_) n -> let
                  count' = count+1
                  sum' = sum+n
                  avg' = (fromIntegral sum') / (fromIntegral count')
                  in (count',sum',n,avg')) (0,0,0,0.0::Double)
                & streamMap fth4
                & unStream
                & last

-- add a session ID label to each sample. Sessions are delineated by
-- intervals of 15 minutes or longer between successive samples.
addSession :: Stream (Timestamp, PebbleMode60) -> Stream (Int, Timestamp, PebbleMode60)
addSession = streamScan
    (\(oldSId, oldTS, _) (ts,payload) -> let
      interval = 15 * 60 -- 15 minutes
      sId      = if   diffUTCTime ts oldTS > interval
                 then oldSId + 1
                 else oldSId

      in (sId, ts, payload)
    )
    (0, dummyTS, ((0,0,0),0))

selectSession sId = streamFilter ((==sId) . fst3)


sessionLength :: Stream (Int, Timestamp, PebbleMode60) -> Stream (Timestamp, Timestamp)
sessionLength s@((Event (Just startTS) _):_) = streamMap ((,) startTS . snd3) s

------------------------------------------------------------------------------
-- functions that operate on batches of samples

-- convert csvLines into one-PebbleMode60-per-line with corrected
-- Event timestamps, collects 1000s windows and assigned a 'session
-- ID' (0-length windows break sessions up). Discard empty windows.
windowSession :: [String] -> Stream (Int, [(Timestamp, PebbleMode60)])
windowSession lines = lines
                    & concatMap csvLineToRecordLines
                    & mkStream
                    & streamMap recordLineToPebbleMode60

                    -- copy data timestamp to Event
                    & streamWindow pebbleTimes & streamExpand

                    & streamWindow (chopTime 1000)
                    -- NOTE : not filtering empty lists this time

                    -- give each window a 'session' ID. Empty windows
                    -- are assigned session 0. Accumulator type:
                    --  - current window ID
                    --  - latest non-0 window ID
                    --  - payload
                    & streamScan (\(i,lasti,lastw) w -> let
                      thisWindowEmpty = null w
                      lastWindowEmpty = null lastw
                      xor = (/=)

                      sId = if thisWindowEmpty `xor` lastWindowEmpty
                            then if   thisWindowEmpty
                                 then 0
                                 else lasti+1
                            else i
                      in (sId,max lasti sId,w))
                      (0,0,[])

                    & streamMap (\(sId, oldSiD, w) -> (sId,w))
                    -- discard empty windows
                    & streamFilter ((>0) . fst)

dummyTS = read "0000-01-01 00:00:00.00000 +0000" :: Timestamp

getFirstTs :: (a, [(Timestamp, PebbleMode60)]) -> Timestamp
getFirstTs (_, []) = dummyTS
getFirstTs (_, ws) = fst (head ws)

getLastTs :: (a, [(Timestamp, PebbleMode60)]) -> Timestamp
getLastTs (_, []) = dummyTS
getLastTs (_, ws) = fst (last ws)

fst3 (x,_,_) = x
snd3 (_,x,_) = x
thd3 (_,_,x) = x

fth4 (_,_,_,x) = x

groupBySessionId :: WindowMaker (Int, Timestamp, Timestamp)
groupBySessionId = groupBy (\e f -> fmap fst3 (value e) == fmap fst3 (value f))

-- receive session-ID-labelled windows; determine how long each
-- session spans.
windowSessionLength :: Stream (Int, [(Timestamp, PebbleMode60)]) -> Stream (Int, Timestamp, Timestamp)
windowSessionLength ws = ws
                 -- temporarily throw away the actual data
                 & streamScan (\oldw w -> let
                   -- is this a new session?
                   start = if   fst3 oldw /= fst w
                           then getFirstTs w
                           else snd3 oldw
                   end = getLastTs w
                   in  (fst w, start, end)
                   )
                   (0, undefined, undefined)

                 -- only emit the last event for a given session ID
                 & streamWindow groupBySessionId
                 & streamMap last

-- sessionDelta: calculate the time difference since the previous
-- session closed. Value for first session will be bogus

sessionDelta  :: Stream (Int, Timestamp, Timestamp) -> Stream (Int, Int)
sessionDelta ws = ws
                & streamScan (\oldw w -> let
                  lastSessionEnd = fth4 oldw
                  sessionEnd     = thd3 w
                  sessionStart   = snd3 w
                  delta          = ( picoToInt
                                   . nominalDiffTimeToSeconds
                                   . diffUTCTime sessionStart) lastSessionEnd
                  sId            = fst3 w
                  in (sId, delta, sessionStart, sessionEnd)
                )
                ( let sId          = 0
                      delta        = 0 :: Int
                      sessionStart = dummyTS
                      sessionEnd   = dummyTS
                  in (sId, delta, sessionStart, sessionEnd))

                -- temporarily throw away some fields for clarity
                & streamMap (\(w,x,y,z) -> (w,x))
 
picoToInt :: Pico -> Int
picoToInt p = let
  (i, f) = properFraction p
  in if   f >= 0.5
     then i + 1
     else i

------------------------------------------------------------------------------
