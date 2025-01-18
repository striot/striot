{- simple stream experiments with wearable data
 -}


module WearableStreams where

import qualified Data.IntMap.Strict as M
import Data.Function ((&))
import Data.List (intercalate)
import Data.List.Split (chunksOf, splitOn)
import Data.Time
import System.Random
import Striot.Simple
import WearableExample

------------------------------------------------------------------------------
-- Explore the data-set using the Simple Stream interface

-- for consuming from pebbleRawAccel_1806-02.csv, format: 10 groups of four readings
-- per line, prefixed by timestamp as unix epoch, suffixed with some number of 0s
-- produce one-record-per-line CSV for easier debugging
csvLineToRecordLines :: String -> [[String]]
csvLineToRecordLines line = let
  ts:fields = splitOn "," line
  in fields & chunksOf 4 & take 10 & map (ts:)

recordLineToPebbleMode60 csv = let
  [x,y,z,v] = map (read :: String -> Int) (tail csv)
  ts = parseTimeField (head csv)
  in (ts,((x,y,z),v))

-- common bit of stream processing for building streams that consume
-- data in pebbleRawAccel_1806-02.csv format:
-- unix epoch timestamp, followed by ten readings as groups of
-- four integers, followed by a number of zero fields.
-- unpacks the readings to one per item in a Stream, with Event timestamps
-- set to match the reading.
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
-- Better would be a rolling recalculated average across the
-- whole data-set (see iterations below)
-- this reports 20Hz
arrivalRate :: Stream a -> Int
arrivalRate str = str
                & streamWindow (chopTime 1000)
                & streamMap length
                & unStream
                & last

-- running average Hz
-- λ> pebbleStream csvFile & arrivalRateRunning & unStream & last
-- 23.51336816226183
-- chopTime emits 0-length lists for time intervals which do not have
-- any events in them. I.e.,
--  [10,10,10,40] & streamWindow (chopTime 10) => [[10,10,10],[],[],[40]]
-- two empty lists emitted for the time intervals +20 and +30.
-- filtering out the empty lists means we are measuring
-- the combined arrival rate of disjoint "sessions".
arrivalRateRunning :: Stream a -> Stream Double
arrivalRateRunning str = str
                 & streamWindow (chopTime 1000)
                 & streamFilter (not . null) -- see above
                 & streamMap length
                 & streamScan (\(count,sum,_,_) n -> let
                   count' = count+1
                   sum' = sum+n
                   avg' = (fromIntegral sum') / (fromIntegral count')
                   in (count',sum',n,avg')) (0,0,0,0.0::Double)
                 & streamMap fth4

-- another arrivalRate, this time don't filter out empty windows.
-- For the whole CSV: 7.651951428880981
-- For example sub-"sessions":
--    1 20.947272312257475
--    2 20.89397496087637
--    3 18.902333621434746
--    4 23.577981651376145
--    5 20.6850436681222
--    6 12.249488752556237
-- TODO: rename arrivalRate''
arrivalRate'' :: Stream a -> Stream Double
arrivalRate'' s = s
                & streamWindow (chopTime 1000)
                & streamMap length
                & streamScan (\(count,sum,_,_) n -> let
                  count' = count+1
                  sum' = sum+n
                  avg' = (fromIntegral sum') / (fromIntegral count')
                  in (count',sum',n,avg')) (0,0,0,0.0::Double)
                & streamMap fth4

-- frequency distribution of arrival rates as the stream progresses
-- str & arrivalRate'' & rateFreq
rateFreq :: Stream Double -> Stream (M.IntMap Int)
rateFreq s = s
           & streamMap (round :: Double -> Int)
           & streamScan (\m i -> M.insertWith (+) i 1 m) M.empty

-- alternative of pebbleStream which operates on session1.csv format
pebbleStream' :: String -> Stream (Timestamp, PebbleMode60)
pebbleStream' csvFile = csvFile
                  & lines
                  & map parseSessionLine
                  & mkStream
                  & streamWindow pebbleTimes
                  & streamExpand

-- output of filterAcc
-- what is the frequency distribution of the calculated vectors over the dataset?
movementFreq file = file
                  & pebbleStream'
                  & streamMap snd
                  & streamFilter ((==0) . snd) -- vibe off
                  & streamMap (\((x,y,z),_) -> (x*x,y*y,z*z))
                  & streamMap (\(x,y,z) -> intSqrt (x+y+z))
                  & streamScan (\m i -> M.insertWith (+) i 1 m) M.empty

-- e.g. length $ stepEvents 2000 csvFile
stepEvents thr file = file
                & pebbleStream'
                & streamMap snd
                & edEvent
                & stepEvent thr

------------------------------------------------------------------------------
-- identifying 'sessions' from a stream (without batching into windows)

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
-- utility functions and test data

dummyTS = read "0000-01-01 00:00:00.00000 +0000" :: Timestamp

fst3 (x,_,_)   = x
snd3 (_,x,_)   = x
thd3 (_,_,x)   = x
fth4 (_,_,_,x) = x

jan_1_1900_day :: Day
jan_1_1900_day = fromGregorian 1900 1 1
-- 1/1/1900 as a UTCTime
jan_1_1900_time :: UTCTime
jan_1_1900_time = UTCTime jan_1_1900_day 0 -- gives example time for the first event


-- generate a stand-in for session1.csv
generateSampleData :: IO String
generateSampleData = do
  g <- getStdGen :: IO StdGen
  let rs = randomRs (0,99) g :: [Int]
  let r = rs & sampleDataGenerator jan_1_1900_time 10 
             & unStream 
             & zip (incr 1529598787929) 
             & take 275309
             & map (\(x,y) -> (show x) ++ "," ++ (show y)) 
             & intercalate "\n"
  return r
  where incr x = take 10 (repeat x) ++ incr (x + 400)

writeSampleData = generateSampleData >>= writeFile "session1.csv"

main = writeSampleData
