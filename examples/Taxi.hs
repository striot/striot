module Taxi where
import FunctionalIoTtypes
import FunctionalProcessing
import qualified Data.Map as Map
import Data.List
import System.IO
import Data.List.Split
import Data.Time (UTCTime,NominalDiffTime)

-- A solution to: http://www.debs2015.org/call-grand-challenge.html
-- The winner was: https://vgulisano.files.wordpress.com/2015/06/debs2015gc_tr.pdf
-- Needs Parallel top-K ... see http://www.cs.yale.edu/homes/dongqu/PTA.pdf 

type MD5Sum  = String
type Dollars = Float

type Degrees = Float
data Location = Location 
                {lat :: Degrees
                ,long:: Degrees}
      deriving (Eq, Ord, Show)

data Payment_Type = Card | Cash
      deriving (Eq, Ord, Show)

type Medallion  = MD5Sum

data Trip = Trip 
   { medallion         :: Medallion
   , hack_license      :: MD5Sum
   , pickup_datetime   :: UTCTime
   , dropoff_datetime  :: UTCTime
   , trip_time_in_secs :: Int
   , trip_distance     :: Float
   , pickup            :: Location
   , dropoff           :: Location
   , payment_type      :: Payment_Type
   , fare_amount       :: Dollars
   , surcharge         :: Dollars
   , mta_tax           :: Dollars
   , tip_amount        :: Dollars
   , tolls_amount      :: Dollars
   , total_amount      :: Dollars}
      deriving (Eq, Ord, Show)
  
data Cell = Cell
   { clat  :: Int
   , clong :: Int}
   deriving (Eq, Ord)

instance Show Cell where
    show c = show (clat c) ++ "." ++ show (clong c)
   
data Journey  = Journey 
   { start :: Cell
   , end   :: Cell}
   deriving (Eq, Ord)

instance Show Journey where
    show j = show (start j) ++ "->" ++ show (end j)

-- Query 1: Frequent Routes
cellLatLength   = 0.004491556
cellLongLength  = 0.005986
-- The coordinate 41.474937, -74.913585 marks the center of the first cell
cell1p1CentreLat  =  41.474937
cell1p1CentreLong = -74.913585

cell11Origin = Location (cell1p1CentreLat+(cellLatLength/2)) (cell1p1CentreLong -(cellLongLength/2))

inRange:: Int -> Int -> Cell -> Bool
inRange maxLat maxLong cell = clat cell<=maxLat && clong cell<=maxLong && clat cell>=1 && clong cell>=1

-- Cell Definitions
toCell:: Location -> Location -> Location -> Cell
toCell cell11Origin cellSideLength l = Cell (floor (((lat cell11Origin)-(lat  l) ) / (lat  cellSideLength))+1)
                                            (floor (((long l)-(long cell11Origin)) / (long cellSideLength))+1)

toCellQ1:: Location -> Cell
toCellQ1 loc = toCell cell11Origin (Location 0.004491556 0.005986) loc

toCellQ2:: Location -> Cell
toCellQ2 loc = toCell cell11Origin (Location (0.004491556/2) (0.005986/2)) loc

inRangeQ1:: Cell -> Bool
inRangeQ1 = inRange 300 300

inRangeQ2:: Cell -> Bool
inRangeQ2 = inRange 600 600

type TripCounts = Map.Map Journey Int

-- a generally useful function
changes:: Eq alpha=> Stream alpha -> Stream alpha
changes s = streamFilterAcc (\acc h-> if (h==acc) then acc else h) (value $ head s) (\h acc->(h/=acc)) (tail s)

type Q1Output = [(Journey,Int)]
frequentRoutes:: Stream Trip -> Stream Q1Output
frequentRoutes s = changes 
                 $ streamWindowAggregate (slidingTime 1800) (mostFrequent 10)
                 $ streamFilter (\j-> inRangeQ1 (start j) && inRangeQ1 (end j))
                 $ streamMap    (\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}) s

-- alternative to expose stream types:
-- frequentRoutes:: Stream Trip -> Stream Q1Output
-- frequentRoutes s = let sm =                
                 
mostFrequent:: Ord alpha => Int -> [alpha] -> [(alpha,Int)]
mostFrequent i l = take i $ sortBy (\(k1,v1)(k2,v2)->compare v2 v1) 
                 $ Map.toList $ foldr (\e->Map.insertWith (+) e 1) Map.empty l

--foldrEx::TripCounts
tripSource:: String -> Source Trip -- parse input file into a Stream of Trips
tripSource s = map (\t->E (dropoff_datetime t) t) 
                   (map stringsToTrip (map (Data.List.Split.splitOn ",") (lines s)))

stringsToTrip:: [String] -> Trip
stringsToTrip [med,hack,pickupDateTime,dropoffDateTime,trip_time,trip_dist,pickup_long,pickup_lat,
               dropoff_long,dropoff_lat,pay_type,fare,sur,mta,tip,tolls,total] =
   Trip med hack (read pickupDateTime) (read dropoffDateTime) (read trip_time) (read trip_dist) 
                 (Location (read pickup_lat)  (read pickup_long))
                 (Location (read dropoff_lat) (read dropoff_long))
                 (if pay_type=="CRD" then Card else Cash)
                 (read fare) (read sur) (read mta) (read tip) (read tolls) (read total)

--some tests
main1 = do contents <- readFile "sorteddata.csv"
           putStr $ show $ take 10 $ tripSource contents

main2 = do contents <- readFile "sorteddata.csv"
           putStr $ show $ take 1 $ tripSource contents

main3 = do contents <- readFile "sorteddata.csv"
           putStr $ show $ take 10 $ streamFilter (\j->(inRangeQ1 (start j) && inRangeQ1 (end j))) $ streamMap (\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}) $ tripSource contents

main4 = do contents <- readFile "sorteddata.csv"
           putStr $ show $ take 10 $ FunctionalProcessing.chop 10 $ streamFilter (\j->(inRangeQ1 (start j) && inRangeQ1 (end j))) $ streamMap (\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}) $ tripSource contents
-- to run Q1....
main = do contents <- readFile "sorteddata.csv"
          putStr $ show $ frequentRoutes $ tripSource contents

-- Query 2

pickupHistory:: [(Trip,Journey)] -> Map.Map Cell [Trip]
pickupHistory ts = foldr (\t->Map.insertWith (++) (start $ snd t) [(fst t)]) Map.empty ts 

newestPickup:: [(Trip,Journey)] -> Map.Map (Cell,Medallion) UTCTime
newestPickup ts = foldr (\t->Map.insertWith (\newt existing->if newt>existing then newt else existing)
                         (start $ snd t, medallion $ fst t) (pickup_datetime $ fst t)) Map.empty ts

oldestDropoff :: [(Trip,Journey)] -> Map.Map (Cell,Medallion) UTCTime
oldestDropoff ts = foldr (\t->Map.insertWith (\newt existing->if newt<existing then newt else existing)
                          (end $ snd t, medallion $ fst t) (dropoff_datetime $ fst t)) Map.empty ts

--"The profit that originates from an area is computed by calculating the median fare + tip for trips that started in the area and ended within the last 15 minutes."
profit :: [Trip] -> Dollars
profit ts = median $ map (\t->fare_amount t + tip_amount t) ts

median:: Ord alpha=> [alpha] -> alpha
median l =  let sl = sort l in
                sl!!(floor (fromIntegral (length sl) / 2.0))

cellProfit:: [(Trip,Journey)] -> Map.Map Cell Dollars
cellProfit tjs = Map.map profit $ pickupHistory tjs

--"The number of empty taxis in an area is the sum of taxis that had a drop-off location in that area less than 30 minutes ago and had no following pickup yet."
 
taxisDroppedOffandNotPickedUp:: Map.Map (Cell,Medallion) UTCTime -> Map.Map (Cell,Medallion) UTCTime -> [(Trip,Journey)] -> [Cell] 
taxisDroppedOffandNotPickedUp np od ts = map (\(t,j)->start j) $ filter (\(t,j)-> if Map.notMember (start j,medallion t) np  
                                                                                  then True
                                                                                  else np Map.! (start j,medallion t) < dropoff_datetime t) ts 

emptyTaxisPerCell::  [(Trip,Journey)] -> Map.Map Cell Int
emptyTaxisPerCell ts = foldl (\m c->Map.insertWith (+) c 1 m) Map.empty (taxisDroppedOffandNotPickedUp (newestPickup ts) (oldestDropoff ts) ts)

profitability:: Map.Map Cell Int -> Map.Map Cell Dollars -> Map.Map Cell Dollars
profitability emptyTaxis cellProf = let allCells = [Cell lat long|lat<-[1..600],long<-[1..600]] in
                                        foldl (\m c->Map.insert c (cellProf Map.! c / fromIntegral (emptyTaxis Map.! c)) m) Map.empty allCells    

--profitableCells:: Stream Trip -> Stream Q2Output
profitableCells s = changes 
                  $ streamWindowAggregate (slidingTime 1800)
                      (\es-> take 10
                           $ sortBy (\(k1,v1)(k2,v2)->compare v2 v1)
                           $ Map.toList
                           $ foldl (\m t->Map.insertWith (+) t 1 m) Map.empty es)
                  $ streamJoinW (slidingTime 900) (slidingTime 1800)
                                (\a b->profitability (emptyTaxisPerCell b)(cellProfit a)) processedStream processedStream
                       where processedStream = streamFilter (\(t,j)-> (inRangeQ2 $ start j) && (inRangeQ2 $ end j)) 
                                             $ streamMap (\t-> (t,Journey (toCellQ2 $ pickup t) (toCellQ2 $ dropoff t))) s

mainQ2 = do contents <- readFile "sorteddata.csv"
            putStr $ show $ profitableCells $ tripSource contents
