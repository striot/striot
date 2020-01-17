module Jackson where
-- import FunctionalIoTtypes
-- import FunctionalProcessing
import Data.Array -- cabal install array
import Matrix.LU -- cabal install dsp
import Matrix.Matrix
import Data.List

-- References & Manuals
-- https://en.wikipedia.org/wiki/Jackson_network
-- http://www.ece.virginia.edu/mv/edu/715/lectures/QNet.pdf
-- https://hackage.haskell.org/package/dsp 
-- http://haskelldsp.sourceforge.net/doc/Matrix.LU.html
-- https://hackage.haskell.org/package/array-0.5.1.1/docs/Data-Array.html
-- http://haskelldsp.sourceforge.net/doc/Matrix.Matrix.html

identity:: (Ix a, Integral a, Num b) => Array (a,a) b -> Array (a,a) b
identity p = listArray (bounds p) $ [if row==column then 1 else 0| row<-[1..size],column<-[1..size]]
                        where size = fst $ snd $ bounds p
                        
mm_subtract:: (Ix a, Integral a, Num b) => Array (a, a) b -> Array (a, a) b -> Array (a, a) b
mm_subtract x y = listArray (bounds x) $ [(x Data.Array.! (row,column))-(y Data.Array.! (row,column))| row<-[1..size],column<-[1..size]] 
                          where size = fst $ snd $ bounds x
                          
                          
ma_mult:: (Ix a, Integral a, Num b) => Array (a, a) b -> b -> Array (a, a) b 
ma_mult x v   = listArray (bounds x) $ [v*(x Data.Array.! (row,column))| row<-[1..size],column<-[1..size]] 
                          where size = fst $ snd $ bounds x
                          
va_mult:: (Ix a, Integral a, Num b) => Array a b -> b -> Array a b 
va_mult x val   = listArray (bounds x) $ [val*(x Data.Array.! row)| row<-[1..size]] 
                          where size = snd $ bounds x
                          
vv_mult:: (Ix a, Integral a, Num b) => Array a b -> Array a b -> Array a b
vv_mult v1 v2 = listArray (bounds v1) $ [(v1 Data.Array.! row)*(v2 Data.Array.!row) |row <- [1..size]]
                          where size = snd $ bounds v1

v_take:: Int -> Array Int b -> Array Int b
v_take max v = listArray (1,max) $ [v Data.Array.! row |row <- [1..max]]

-- Jackson Network: lambda = (I-P')^(-1)a where a = (alpha.p0i)i=1..J
arrivalRate:: Array (Int, Int) Double -> Array Int Double -> Double -> Array Int Double  
arrivalRate p p0i alpha =  mv_mult (inverse $ mm_subtract (identity p) (m_trans p)) aa
                               where aa = va_mult p0i alpha
  
-- ρ = λ/μ is the utilization of the buffer (the average proportion of time which the server is occupied.  
utilisation:: Array Int Double -> Array Int Double -> Array Int Double
utilisation arrivalRates meanServiceTimes = vv_mult arrivalRates meanServiceTimes

-- the average number of customers in the system is ρ/(1 − ρ)
avgeNumberOfCustomersInSystem:: Array Int Double -> Array Int Double
avgeNumberOfCustomersInSystem utilisations = listArray (bounds utilisations) $ 
                                                       [(utilisations Data.Array.! row)/(1.0- (utilisations Data.Array.!row)) |row <- [1..size]]
                                                 where size = snd $ bounds utilisations
                                                 
-- the average response time (total time a customer spends in the system) is 1/(μ − λ)
avgeResponseTime:: Array Int Double -> Array Int Double -> Array Int Double
avgeResponseTime arrivalRates meanServiceTimes = listArray (bounds arrivalRates) $ 
                                                       [1.0/((1.0/(meanServiceTimes Data.Array.! row))-(arrivalRates Data.Array.!row)) |row <- [1..size]]
                                                 where size = snd $ bounds arrivalRates

stable:: Array Int Double -> Array Int Double -> Array Int Bool
stable arrivalRates meanServiceTimes = let utils = utilisation arrivalRates meanServiceTimes in
                                           listArray (bounds arrivalRates) $ 
                                                       [(utils Data.Array.! row) < 1/0 |row <- [1..size]]
                                                 where size = snd $ bounds arrivalRates

--	the average time spent waiting in the queue is ρ/(μ – λ)
avgeTimeInQueue:: Array Int Double -> Array Int Double -> Array Int Double
avgeTimeInQueue arrivalRates meanServiceTimes = let utils = utilisation arrivalRates meanServiceTimes in
                                                       listArray (bounds arrivalRates) $ 
                                                       [(utils Data.Array.! row)/
                                                        ((1.0/(meanServiceTimes Data.Array.! row))-(arrivalRates Data.Array.!row))  |row <- [1..size]]
                                                    where size = snd $ bounds arrivalRates


------ example from wikipedia page on Jackson networks
wikiExample:: Array Int Double
wikiExample = let p     = listArray ((1,1),(3,3)) $ [0  ,0.5,0.5,     -- node 1
                                                     0  ,0  ,0  ,     -- node 2
                                                     0  ,0  ,0   ] in -- node 3
              let alpha = 5 in                                        -- 5 events per second arrive into the system
              let p0i   = listArray (1,3) $         [0.5,0.5,0   ] in -- the input events are distributed evenly across nodes 1 and 2
                  arrivalRate p p0i alpha

--- Taxi Q1 example
{--
type Q1Output = ((UTCTime, UTCTime), [(Journey, Int)])
frequentRoutes :: Stream Trip -> Stream Q1Output                                                          -- node 6  Input rate 1.188*0.1 = 0.1188 
frequentRoutes s = streamFilterAcc (\_ h -> (False,h)) (True,undefined) testSndChange s                   -- node 5  Input rate 1.188 Selectivity (est. 0.1)
                 $ streamMap (\w -> (let lj = last w in (pickupTime lj, dropoffTime lj), topk 10 w))      -- node 4  Input rate 1.188
                 $ streamWindow (slidingTime 1800000)                                                     -- node 3  Input rate 1.2*0.99 = 1.188
                 $ streamFilter (\j -> inRangeQ1 (start j) && inRangeQ1 (end j))                          -- node 2  Input rate 1.2/s Selectivity (est.) 0.95
                 $ streamMap    tripToJourney s                                                           -- node 1  Input rate 1.2/s         

Node, InputFrom, Input Rate, Selectivity, Output Rate
0     -          -           -            1.2
1     0 (1)      1.2         1            1.2
2     1 (1)      1.2         0.95         1.188
3     2 (0.95)   1.188       1            1.188
4     3 (1)      1.188       1            1.188
5     4 (1)      1.188       0.1          0.1188
6     5 (0.1)    0.118       -            - 

This is represented as follows:      
-}
       
taxiQ1Array:: Array (Int,Int) Double
taxiQ1Array = listArray ((1,1),(6,6)) $
              -- Output Node
              --  1    2    3    4    5     6
               [  0   ,1   ,0   ,0   ,0    ,0  ,     -- node 1 streamMap
                  0   ,0   ,0.95,0   ,0    ,0  ,     -- node 2 streamFilter
                  0   ,0   ,0   ,1   ,0    ,0  ,     -- node 3 streamWindow
                  0   ,0   ,0   ,0   ,1    ,0  ,     -- node 4 streamMap
                  0   ,0   ,0   ,0   ,0    ,0.1,     -- node 5 streamFilterAcc
                  0   ,0   ,0   ,0   ,0    ,0  ]     -- node 6 the output of Q1

taxiQ1Inputs = listArray (1,6) $ [1,0,0,0,0,0] -- all events in the input stream are sent to node 1

taxiQ1meanServiceTimes:: Array Int Double
taxiQ1meanServiceTimes = listArray (1,6) [0.0001,0.0001,0.0001,0.01,0.0001,0.0001]

 
taxiQ1arrivalRates:: Array Int Double
taxiQ1arrivalRates = arrivalRate taxiQ1Array taxiQ1Inputs 1.2 -- the 1.2 is the arrival rate (in events per second) into the system



taxiQ1utilisation = utilisation taxiQ1arrivalRates taxiQ1meanServiceTimes 

taxiQ1avgeNumberCustomersInSystem = avgeNumberOfCustomersInSystem taxiQ1utilisation

taxiQ1avgeResponseTime = avgeResponseTime taxiQ1arrivalRates taxiQ1meanServiceTimes

taxiQ1avgeTimeInQueue = avgeTimeInQueue   taxiQ1arrivalRates taxiQ1meanServiceTimes

data OperatorInfo = OperatorInfo { opId        :: Int
                                 , arrRate     :: Double
                                 , serviceTime :: Double
                                 , util        :: Double
                                 , stab        :: Bool
                                 , custInSys   :: Double
                                 , respTime    :: Double
                                 , queueTime   :: Double
                                 }
                                 deriving (Show)
                                 
calcAll:: Array (Int,Int) Double -> Array Int Double -> Double -> Array Int Double -> [OperatorInfo]
calcAll connections inputs alpha meanServiceTimes = let arrivalRates             = arrivalRate connections inputs alpha in
                                                    let utilisations             = utilisation arrivalRates meanServiceTimes                in
                                                    let stability                = stable      arrivalRates meanServiceTimes                in
                                                    let avgeNumberOfCustInSystem = avgeNumberOfCustomersInSystem utilisations               in
                                                    let avgeResponseTimes        = avgeResponseTime arrivalRates meanServiceTimes           in
                                                    let avgeTimesInQueue         = avgeTimeInQueue  arrivalRates meanServiceTimes           in
                                                           map (\id -> OperatorInfo id (arrivalRates             Data.Array.! id)
                                                                                       (meanServiceTimes         Data.Array.! id)
                                                                                       (utilisations             Data.Array.! id)
                                                                                       (stability                Data.Array.! id)
                                                                                       (avgeNumberOfCustInSystem Data.Array.! id)                                                                                               
                                                                                       (avgeResponseTimes        Data.Array.! id)
                                                                                       (avgeTimesInQueue         Data.Array.! id))
                                                               [1.. (snd $ bounds arrivalRates)]
                                                                              
taxiQ1Calc:: [OperatorInfo]
taxiQ1Calc = calcAll taxiQ1Array taxiQ1Inputs 1.2 taxiQ1meanServiceTimes


-- basic tests
ex1   = listArray ((1,1),(3,3)) $ [1,0,0,-0.5,1,0,-0.5,0,1]                    
test1 = (listArray ((1,1), (3,3)) $ [1,0,0,0,1,0,0,0,1]) Data.Array.! (1,1)
test2 = print $ inverse $ listArray ((1,1), (3,3)) $ [1,0,0,0,1,0,0,0,1]
test3 = print $ inverse $ listArray ((1,1), (3,3)) $ [1,0,0,-0.5,1,0,-0.5,0,1]
test4 = print $ mv_mult (inverse $ listArray ((1,1), (3,3)) $ [1,0,0,-0.5,1,0,-0.5,0,1]) 
                        (listArray (1,3) $ [2.5,2.5,0])
test5 = identity ex1
test6 = mm_subtract ex1 ex1
test7 = m_trans ex1
test8 = mm_subtract (identity ex1) (m_trans ex1)
