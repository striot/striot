module Taxi where
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import qualified Data.Map as Map
import Data.List
import Data.List.Split
import Data.Time (UTCTime)

-- A solution to: http://www.debs2015.org/call-grand-challenge.html
-- The winner was: https://vgulisano.files.wordpress.com/2015/06/debs2015gc_tr.pdf
-- Needs Parallel top-K ... see http://www.cs.yale.edu/homes/dongqu/PTA.pdf

-- Define the types and data structures needed by the application
-- Define the types and data structures needed by the application
type MD5Sum  = String
type Dollars = Float

type Degrees = Float
data Location = Location         -- taxi's latitude and longitude
                { lat  :: Degrees
                , long :: Degrees
                } deriving (Eq, Ord, Show)

data PaymentType = Card | Cash
     deriving (Eq, Ord, Show)

type Medallion  = MD5Sum

data Trip = Trip
   { medallion        :: Medallion
   , hackLicense      :: MD5Sum
   , pickupDatetime   :: UTCTime
   , dropoffDatetime  :: UTCTime
   , tripTimeInSecs   :: Int
   , tripDistance     :: Float
   , pickup           :: Location
   , dropoff          :: Location
   , paymentType      :: PaymentType
   , fareAmount       :: Dollars
   , surcharge        :: Dollars
   , mtaTax           :: Dollars
   , tipAmount        :: Dollars
   , tollsAmount      :: Dollars
   , totalAmount      :: Dollars
   } deriving (Eq, Ord, Show)

data Cell = Cell  -- the Cell in which the Taxi is located
   { clat  :: Int
   , clong :: Int}
   deriving (Eq, Ord)

instance Show Cell where
    show c = show (clat c) ++ "." ++ show (clong c)

data Journey  = Journey -- a taxi journey from one cell to another
   { start       :: Cell
   , end         :: Cell
   , pickupTime  :: UTCTime
   , dropoffTime :: UTCTime}
   deriving (Eq, Ord)

instance Show Journey where
    show j = show (start j) ++ "->" ++ show (end j)

-- Query 1: Frequent Routes
cellLatLength :: Float
cellLatLength   = 0.004491556 -- the cell sizes for query 1
cellLongLength :: Float
cellLongLength  = 0.005986
-- The coordinate 41.474937, -74.913585 marks the center of the first cell
cell1p1CentreLat :: Float
cell1p1CentreLat  =  41.474937
cell1p1CentreLong :: Float
cell1p1CentreLong = -74.913585

-- calculate the origin of the grid system
cell11Origin :: Location
cell11Origin = Location (cell1p1CentreLat + (cellLatLength / 2)) (cell1p1CentreLong - (cellLongLength / 2))

-- some taxis report locations outside the grid specified by the Q1 problem. The inRange function checks for this.
inRange :: Int -> Int -> Cell -> Bool
inRange maxLat maxLong cell = clat cell <= maxLat && clong cell <= maxLong && clat cell >= 1 && clong cell >= 1

-- tranforms a location into a cell given the origin of the grid and the cell side length
toCell :: Location -> Location -> Location -> Cell
toCell cellOrigin cellSideLength l = Cell (floor ((lat cellOrigin - lat  l) / lat  cellSideLength) + 1)
                                          (floor ((long l - long cell11Origin) / long cellSideLength) + 1)

-- Q1 and Q2 use differnt grids, so these functions transform a location into a cell for each grid
toCellQ1 :: Location -> Cell
toCellQ1 = toCell cell11Origin (Location 0.004491556 0.005986)

toCellQ2 :: Location -> Cell
toCellQ2 = toCell cell11Origin (Location (0.004491556 / 2) (0.005986 / 2))

-- checks if a cell is in the range specified in the problem definition
inRangeQ1 :: Cell -> Bool
inRangeQ1 = inRange 300 300

inRangeQ2 :: Cell -> Bool
inRangeQ2 = inRange 600 600

-- type TripCounts = Map.Map Journey Int

--- Parse the input file --------------------------------------------------------------------------------------
tripSource :: String -> Stream Trip -- parse input file into a Stream of Trips
tripSource s = map ((\t -> Event 0 (Just (dropoffDatetime t)) (Just t))
                   . stringsToTrip . Data.List.Split.splitOn ",") (lines s)

-- turns a line from the input file (already split into a list of fields) into a Trip datastructure
stringsToTrip :: [String] -> Trip
stringsToTrip [med, hack, pickupDateTime, dropoffDateTime, trip_time, trip_dist, pickup_long, pickup_lat,
               dropoff_long, dropoff_lat, pay_type, fare, sur, mta, tip, tolls, total] =
   Trip med hack (read pickupDateTime) (read dropoffDateTime) (read trip_time) (read trip_dist)
                 (Location (read pickup_lat)  (read pickup_long))
                 (Location (read dropoff_lat) (read dropoff_long))
                 (if pay_type == "CRD" then Card else Cash)
                 (read fare) (read sur) (read mta) (read tip) (read tolls) (read total)
stringsToTrip s = error ("error in input: " ++ intercalate "," s)

----------------------------------------------------------------------------------------------------------------

journeyChanges :: Stream ((UTCTime, UTCTime),[(Journey, Int)]) -> Stream ((UTCTime, UTCTime),[(Journey, Int)])
journeyChanges (Event _ _ (Just val):r) = streamFilterAcc (\acc h -> if snd h == snd acc then acc else h) val (\h acc -> snd h /= snd acc) r

--- removes consecutive repeated values from a stream, leaving only the changes
changes :: Eq alpha => Stream alpha -> Stream alpha
changes (e@(Event _ _ (Just val)):r) = e : streamFilterAcc (\_ h -> h) val (/=) r

-- produces an ordered list of the i most frequent elements from list a list
topk :: (Num freq, Ord freq, Ord alpha) => Int -> [alpha] -> [(alpha, freq)]
topk i = topkMap i . freqMap

-- generates frequency map for occurrences of elements in a list
freqMap :: (Num freq, Ord freq, Ord alpha) => [alpha] -> Map.Map alpha freq
freqMap = foldr (\k -> Map.insertWith (+) k 1) Map.empty

-- produces topk ordered list from a Map, where the value is the criteria we are maximising
topkMap :: (Num freq, Ord freq, Ord alpha) => Int -> Map.Map alpha freq -> [(alpha, freq)]
topkMap i = take i . sortBy (\(_, v1) (_, v2) -> compare v2 v1) . Map.toList

------------------------ Query 1 --------------------------------------------------------------------------------------
type Q1Output = ((UTCTime, UTCTime), [(Journey, Int)])
frequentRoutes :: Stream Trip -> Stream Q1Output
frequentRoutes s = journeyChanges
                 $ streamMap (\w -> (let lj = last w in (pickupTime lj, dropoffTime lj), topk 10 w))
                 $ streamWindow (slidingTime 1800000)
                 $ streamFilter (\j -> inRangeQ1 (start j) && inRangeQ1 (end j))
                 $ streamMap    tripToJourney s

tripToJourney :: Trip -> Journey
tripToJourney t = Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t), pickupTime=pickupDatetime t, dropoffTime=dropoffDatetime t}

-- to run Q1....
mainQ1 :: IO ()
mainQ1 = do
    contents <- readFile "sorteddata.csv"
    mapM_ (print . value)
        $ frequentRoutes
        $ tripSource contents

--some tests ---------------------------------------------------------------------------------------------------
main1 :: IO ()
main1 = do contents <- readFile "sorteddata.csv"
           putStr $ show $ take 10 $ tripSource contents

main2 :: IO ()
main2 = do contents <- readFile "sorteddata.csv"
           putStr $ show $ take 1 $ tripSource contents

main3 :: IO ()
main3 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ take 10
           $ streamFilter (\j -> inRangeQ1 (start j) && inRangeQ1 (end j))
           $ streamMap tripToJourney
           $ tripSource contents

main4 :: IO ()
main4 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ take 10
           $ Striot.FunctionalProcessing.chop 10
           $ streamFilter (\j -> inRangeQ1 (start j) && inRangeQ1 (end j))
           $ streamMap tripToJourney
           $ tripSource contents

q1map :: Stream Trip -> Stream Journey
q1map    = streamMap    tripToJourney

q1filter :: Stream Journey -> Stream Journey
q1filter = streamFilter (\j -> inRangeQ1 (start j) && inRangeQ1 (end j))

q1window :: Stream alpha -> Stream [alpha]
q1window = streamWindow (slidingTime 1800000)

q1map2 :: Stream [Journey] -> Stream [(Journey, Int)]
q1map2   = streamMap    (topk 10)

main5 :: IO ()
main5 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ q1window
           $ q1filter
           $ q1map
           $ tripSource contents

main6 :: IO ()
main6 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ q1map2
           $ q1window
           $ q1filter
           $ q1map
           $ tripSource contents

testQ1 :: Show alpha => (Stream Trip -> alpha) -> IO()
testQ1 f = do
    contents <- readFile "sorteddata.csv"
    putStr $ show $ f $ tripSource contents

-- Query 2

pickupHistory :: [(Trip, Journey)] -> Map.Map Cell [Trip]
pickupHistory = foldr (\t -> Map.insertWith (++) (start $ snd t) [fst t]) Map.empty

newestPickup :: [(Trip, Journey)] -> Map.Map (Cell, Medallion) UTCTime
newestPickup = foldr (\t -> Map.insertWith (\newt existing -> if newt > existing then newt else existing)
                         (start $ snd t, medallion $ fst t) (pickupDatetime $ fst t)) Map.empty

oldestDropoff :: [(Trip, Journey)] -> Map.Map (Cell, Medallion) UTCTime
oldestDropoff = foldr (\t -> Map.insertWith (\newt existing -> if newt < existing then newt else existing)
                          (end $ snd t, medallion $ fst t) (dropoffDatetime $ fst t)) Map.empty

--"The profit that originates from an area is computed by calculating the median fare + tip for trips that started in the area and ended within the last 15 minutes."
profit :: [Trip] -> Dollars
profit ts = median $ map (\t -> fareAmount t + tipAmount t) ts

median :: Ord alpha => [alpha] -> alpha
median l =  let sl = sort l in
                sl !! floor (fromIntegral (length sl) / (2.0 :: Double))

cellProfit :: [(Trip, Journey)] -> Map.Map Cell Dollars
cellProfit tjs = Map.map profit $ pickupHistory tjs

--"The number of empty taxis in an area is the sum of taxis that had a drop-off location in that area less than 30 minutes ago and had no following pickup yet."

taxisDroppedOffandNotPickedUp :: Map.Map (Cell, Medallion) UTCTime -> [(Trip, Journey)] -> [Cell]
taxisDroppedOffandNotPickedUp np ts = map (\(_, j) -> start j)
                                    $ filter (\(t, j) -> Map.notMember (start j, medallion t) np ||
                                                         (np Map.! (start j, medallion t) < dropoffDatetime t)) ts

emptyTaxisPerCell ::  [(Trip, Journey)] -> Map.Map Cell Int
emptyTaxisPerCell ts = foldl (\m c -> Map.insertWith (+) c 1 m) Map.empty (taxisDroppedOffandNotPickedUp (newestPickup ts) ts)

allCells :: Int -> Int -> [Cell]
allCells latMax longMax = [Cell lat' long' | lat' <- [1..latMax], long' <- [1..longMax]]

initCellMap :: Int -> Int -> a -> Map.Map Cell a
initCellMap latMax longMax val = Map.fromList (zip (allCells latMax longMax) (repeat val))

profitability :: Map.Map Cell Int -> Map.Map Cell Dollars -> Map.Map Cell Dollars
profitability emptyTaxis cellProf = foldl (\m c -> Map.insert c (Map.findWithDefault 0 c cellProf / fromIntegral (Map.findWithDefault 0 c emptyTaxis)) m) Map.empty (Map.keys emptyTaxis)


profitableCells :: Stream Trip -> Stream [(Cell, Dollars)]
profitableCells s = changes
                  $ streamMap (topkMap 10)
                  $ streamJoinW (slidingTime 900000) (slidingTime 1800000)
                                (\a b -> profitability (emptyTaxisPerCell b) (cellProfit a)) processedStream processedStream
                       where processedStream = streamFilter (\(_, j) -> inRangeQ2 (start j) && inRangeQ2 (end j))
                                             $ streamMap (\t -> (t, tripToJourney t)) s

mainQ2 :: IO ()
mainQ2 = do contents <- readFile "sorteddata.csv"
            mapM_ (print . value) $ profitableCells $ tripSource contents

---------------- Tests of Q2 ------------------------------------------------------
q2processedStream :: Stream Trip -> Stream (Trip, Journey)
q2processedStream s = streamFilter (\(_, j) -> inRangeQ2 (start j) && inRangeQ2 (end j))
                    $ streamMap (\t -> (t, tripToJourney t)) s

q2Join :: Stream (Trip, Journey) -> Stream (Map.Map Cell Dollars)
q2Join s = streamJoinW (slidingTime 900000) (slidingTime 1800000)
                       (\a b -> profitability (emptyTaxisPerCell b) (cellProfit a)) s s

q2Agg :: (Num a, Ord t, Ord a) => Stream t -> Stream [(t, a)]
q2Agg = streamWindowAggregate (slidingTime 1800000) (topk 10)

q2TripSourceTest :: IO ()
q2TripSourceTest =  do
    contents <- readFile "sorteddata.csv"
    putStr $ show $ take 50 $ tripSource contents

q2TripSourceTest2 :: IO ()
q2TripSourceTest2 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show $ q2processedStream $ take 50 $ tripSource contents

q2TripSourceTest3 :: IO ()
q2TripSourceTest3 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show $ q2Join $ q2processedStream $ take 50 $ tripSource contents

q2TripSourceTest4 :: IO ()
q2TripSourceTest4 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ streamMap emptyTaxisPerCell
           $ streamWindow (slidingTime 900000)
           $ q2processedStream
           $ take 50
           $ tripSource contents

q2TripSourceTest5 :: IO ()
q2TripSourceTest5 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ streamMap cellProfit
           $ streamWindow (slidingTime 1800000)
           $ q2processedStream
           $ take 50
           $ tripSource contents

q2TripSourceTest6 :: IO ()
q2TripSourceTest6 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ streamMap pickupHistory
           $ streamWindow (slidingTime 1800000)
           $ q2processedStream
           $ take 50
           $ tripSource contents

q2TripSourceTest7 :: IO ()
q2TripSourceTest7 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show
           $ q2Agg
           $ q2Join
           $ q2processedStream
           $ take 50
           $ tripSource contents

q2TripSourceTest8 :: IO ()
q2TripSourceTest8 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show $ profitableCells $ take 50 $ tripSource contents

q2TripSourceTest9 :: Int -> IO ()
q2TripSourceTest9 trips = do
    contents <- readFile "sorteddata.csv"
    putStr $ show $ profitableCells $ take trips $ tripSource contents

q2TripSourceTest10 :: IO ()
q2TripSourceTest10 = do
    contents <- readFile "sorteddata.csv"
    putStr $ show $ length $ tripSource contents
