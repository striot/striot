module Caching where
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import qualified Data.Map as Map
import System.Random
import Data.Time (UTCTime,NominalDiffTime,getCurrentTime)
import Data.Time.Calendar
import Data.Time.Clock

-- creates a cache to avoid calling the map function where possible...
--- it uses a map to implement the cache, so there's the problem that it will get bigger and bigger
--- it would be better use a fixed size hash table.. but it illustrates the point

streamMapCache:: Ord a=> (a->b) -> Stream a -> Stream b
streamMapCache mf s = streamMap fst $ streamScan (cacheMap mf) (undefined,Map.empty) s

cacheMap:: Ord a=> (a->b) -> (b,Map.Map a b) -> a -> (b,Map.Map a b)
cacheMap mf (lastval,cache) val = case Map.lookup val cache of
                                                     Nothing     -> let res = mf val in (res,Map.insert val res cache)
                                                     Just    val ->                     (val,cache)
                                                     
---- Test
jan_1_1900_day = fromGregorian 1900 1 1 :: Day
-- 1/1/1900 as a UTCTime
jan_1_1900_time = UTCTime jan_1_1900_day 0 -- gives example time for the first event
                                           --- probably the earliest IoT event in history!

sampleDataGenerator:: UTCTime -> Int -> [Int] -> Stream Int -- Start Time -> Interval between events in ms -> 
                                                            --- List of random numbers -> Events
sampleDataGenerator start interval rands = (E (rands!!0) start (rands!!0)):
                                           (sampleDataGenerator (addUTCTime (toEnum (interval*10^9)) start) interval (drop 1 rands))

nfib:: Int -> Int
nfib 0 = 1
nfib 1 = 1
nfib n = 1+(nfib (n-1))+(nfib (n-2))
               
--two tests: identical except 1 has no caching, 2 has caching            
cachetest1 = do
               g <- getStdGen
               let rs = randomRs (30,31) g :: [Int] 
               print.take 100 $ streamMap      nfib $ sampleDataGenerator jan_1_1900_time 10 rs

cachetest2 = do
               g <- getStdGen
               let rs = randomRs (30,31) g :: [Int] 
               print.take 100 $ streamMapCache nfib $ sampleDataGenerator jan_1_1900_time 10 rs

main = cachetest2
