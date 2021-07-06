{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}

module Striot.Jackson ( OperatorInfo(..)
                      , calcAll

                      , arrivalRate
                      , arrivalRate'

                      , derivePropagationArray
                      , deriveServiceTimes
                      , deriveInputsArray
                      , calcAllSg
                      , isOverUtilised

                      -- defined
                      , taxiQ1Array
                      , taxiQ1Inputs
                      , taxiQ1meanServiceTimes
                      -- calculated
                      , taxiQ1arrivalRates
                      , taxiQ1utilisation
                      , taxiQ1Calc


                      , htf_thisModulesTests) where

-- import FunctionalIoTtypes
-- import FunctionalProcessing
import Data.Array -- cabal install array
import Matrix.LU -- cabal install dsp
import Matrix.Matrix
import Data.List
import Test.Framework
import Data.Maybe (fromMaybe, fromJust)

import Striot.StreamGraph
import Algebra.Graph

-- References & Manuals
-- https://en.wikipedia.org/wiki/Jackson_network
-- http://www.ece.virginia.edu/mv/edu/715/lectures/QNet.pdf
-- https://hackage.haskell.org/package/dsp 
-- http://haskelldsp.sourceforge.net/doc/Matrix.LU.html
-- https://hackage.haskell.org/package/array-0.5.1.1/docs/Data-Array.html
-- http://haskelldsp.sourceforge.net/doc/Matrix.Matrix.html

-- |Derive the identity matrix from a 2D Array
identity:: (Ix a, Integral a, Num b) => Array (a,a) b -> Array (a,a) b
identity p = listArray (bounds p) $ [if row==column then 1 else 0| row<-[xfrom..xto],column<-[yfrom..yto]]
                        where ((xfrom,yfrom),(xto,yto)) = bounds p
                        
test_identity = assertEqual (identity (listArray ((1,1),(3,3)) $ [0 | x <- [1..9]])) $
    (array ((1,1),(3,3)) [((1,1),1.0), ((1,2),0.0), ((1,3),0.0),
                          ((2,1),0.0), ((2,2),1.0), ((2,3),0.0),
                          ((3,1),0.0), ((3,2),0.0), ((3,3),1.0)] :: Array (Int,Int) Double)

test_identity2= assertEqual (identity (listArray ((0,0),(2,2)) $ [0 | x <- [1..9]])) $
    (array ((0,0),(2,2)) [((0,0),1.0), ((0,1),0.0), ((0,2),0.0),
                          ((1,0),0.0), ((1,1),1.0), ((1,2),0.0),
                          ((2,0),0.0), ((2,1),0.0), ((2,2),1.0)] :: Array (Int,Int) Double)

-- |Matrix subtraction.
mm_subtract:: (Ix a, Integral a, Num b) => Array (a, a) b -> Array (a, a) b -> Array (a, a) b
mm_subtract x y = listArray (bounds x) $ [(x Data.Array.! (row,column))-(y Data.Array.! (row,column))| row<-[xfrom..xto],column<-[yfrom..yto]]
                        where ((xfrom,yfrom),(xto,yto)) = bounds x

test_mm_subtract1 = assertEqual (mm_subtract a b) c
    where
        a = listArray ((1,1),(3,3)) [3.0::Double | x <- [1..9]]
        b = listArray ((1,1),(3,3)) [2.0::Double | x <- [1..9]]
        c = listArray ((1,1),(3,3)) [1.0::Double | x <- [1..9]]

test_mm_subtract2 = assertEqual (mm_subtract a b) c
    where
        a = listArray ((0,0),(2,2)) [3.0::Double | x <- [1..9]]
        b = listArray ((0,0),(2,2)) [2.0::Double | x <- [1..9]]
        c = listArray ((0,0),(2,2)) [1.0::Double | x <- [1..9]]

-- | Matrix multiplication.
-- The indexes must begin at 1.
ma_mult:: (Ix a, Integral a, Num b) => Array (a, a) b -> b -> Array (a, a) b 
ma_mult x v   = listArray (bounds x) $ [v*(x Data.Array.! (row,column))| row<-[1..size],column<-[1..size]] 
                          where size = fst $ snd $ bounds x
                          
-- | Vector (1D Array) multiplication by value.
va_mult:: (Ix a, Integral a, Num b) => Array a b -> b -> Array a b 
va_mult x val   = listArray (bounds x) [val*(x ! row) | row <- [from..to]]
                          where (from,to) = bounds x

-- | Vector (1D Array) multiplication.
-- The indexes must begin at 1.
vv_mult:: (Ix a, Integral a, Num b) => Array a b -> Array a b -> Array a b
vv_mult v1 v2 = listArray (bounds v1) $ [(v1 Data.Array.! row)*(v2 Data.Array.!row) |row <- [1..size]]
                          where size = snd $ bounds v1

-- | Vector (1D Array) equivalent of `take`
v_take:: Int -> Array Int b -> Array Int b
v_take max v = listArray (1,max) $ [v Data.Array.! row |row <- [1..max]]

-- Jackson Network: lambda = (I-P')^(-1)a where a = (alpha.p0i)i=1..J
arrivalRate:: Array (Int, Int) Double -> Array Int Double -> Double -> Array Int Double  
-- p - selectivities of filters
-- p0i - distribution of input events into the system (i.e. to which nodes, which are the source nodes)
-- alpha- arrival rate into the system
-- XXX throwing a run-time exception when called via calcAllSg, for zeroth element
arrivalRate p p0i alpha = arrivalRate' p aa
                              where aa = va_mult p0i alpha

arrivalRate' p aa = mv_mult (inverse $ mm_subtract (identity p) (m_trans p)) aa
 
  
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
       
taxiQ1Array :: Array (Int,Int) Double
taxiQ1Array  = listArray ((1,1),(7,7)) $
              -- Output Node
              --  0   ,1    2    3    4    5     6
               [  0   ,1   ,0   ,0   ,0   ,0    ,0  ,     -- node 1 Source
                  0   ,0   ,1   ,0   ,0   ,0    ,0  ,     -- node 2 streamMap
                  0   ,0   ,0   ,0.95,0   ,0    ,0  ,     -- node 3 streamFilter
                  0   ,0   ,0   ,0   ,1   ,0    ,0  ,     -- node 4 streamWindow
                  0   ,0   ,0   ,0   ,0   ,1    ,0  ,     -- node 5 streamMap
                  0   ,0   ,0   ,0   ,0   ,0    ,0.1,     -- node 6 streamFilterAcc
                  0   ,0   ,0   ,0   ,0   ,0    ,0  ]     -- node 7 the output of Q1

 
taxiQ1Inputs = listArray (1,7) $ [1,0,0,0,0,0,0] -- all events in the input stream are sent to node 1

taxiQ1meanServiceTimes:: Array Int Double
taxiQ1meanServiceTimes = listArray (1,7) [0,0.0001,0.0001,0.0001,0.01,0.0001,0.0001]
 
taxiQ1arrivalRates:: Array Int Double
taxiQ1arrivalRates = arrivalRate taxiQ1Array taxiQ1Inputs 1.2 -- the 1.2 is the arrival rate (in events per second) into the system

test_taxiQ1arrivalRates = assertEqual taxiQ1arrivalRates $
    array (1,7) [(1,1.2),(2,1.2),(3,1.2),(4,1.14),(5,1.14),(6,1.14),(7,0.11399999999999999)]

taxiQ1utilisation = utilisation taxiQ1arrivalRates taxiQ1meanServiceTimes 

taxiQ1avgeNumberCustomersInSystem = avgeNumberOfCustomersInSystem taxiQ1utilisation

taxiQ1avgeResponseTime = avgeResponseTime taxiQ1arrivalRates taxiQ1meanServiceTimes

taxiQ1avgeTimeInQueue = avgeTimeInQueue   taxiQ1arrivalRates taxiQ1meanServiceTimes

data OperatorInfo = OperatorInfo { opId        :: Int
                                 , arrRate     :: Double
                                 , serviceTime :: Double -- XXX rename, clashes with StreamGraph
                                 , util        :: Double
                                 , stab        :: Bool
                                 , custInSys   :: Double
                                 , respTime    :: Double
                                 , queueTime   :: Double
                                 }
                                 deriving (Show,Eq)
                                 
calcAll:: Array (Int,Int) Double -> Array Int Double -> Array Int Double -> [OperatorInfo]
calcAll connections arrivalRates meanServiceTimes = let
    utilisations             = utilisation arrivalRates meanServiceTimes
    stability                = stable arrivalRates meanServiceTimes
    avgeNumberOfCustInSystem = avgeNumberOfCustomersInSystem utilisations
    avgeResponseTimes        = avgeResponseTime arrivalRates meanServiceTimes
    avgeTimesInQueue         = avgeTimeInQueue  arrivalRates meanServiceTimes

    in map (\id -> OperatorInfo id (arrivalRates             ! id)
                                   (meanServiceTimes         ! id)
                                   (utilisations             ! id)
                                   (stability                ! id)
                                   (avgeNumberOfCustInSystem ! id)
                                   (avgeResponseTimes        ! id)
                                   (avgeTimesInQueue         ! id))
           [1.. (snd $ bounds arrivalRates)]
                              
taxiQ1Calc:: [OperatorInfo]
taxiQ1Calc = calcAll taxiQ1Array (arrivalRate taxiQ1Array taxiQ1Inputs 1.2) taxiQ1meanServiceTimes

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

------------------------------------------------------------------------------
-- HTF tests (TODO: convert the above)

prop_identity = do
    n <- vectorOf 9 arbitrary :: Gen [Double]
    return $ identity (listArray shape n)
        == listArray shape
           ([1, 0, 0
            ,0, 1, 0
            ,0, 0, 1
            ] :: [Double])
    where shape = ((1,1),(3,3))

main = htfMain htf_thisModulesTests

------------------------------------------------------------------------------
-- derive* functions to convert the Jackson parameters embedded in the
-- StreamGraph into a form that Jackson code accepts. These should be
-- temporary, and merged/refactored as part of the Jackson code at a
-- later date.
--
-- | Calculate the P propagation array for a StreamGraph based on its
-- filter selectivities.
derivePropagationArray :: StreamGraph -> Array (Int, Int) Double
derivePropagationArray g = let
    vl = map (\v -> (vertexId v, v)) (vertexList g)
    el = map (\(x,y) -> (vertexId x, vertexId y)) (edgeList g)
    m  = fst (head vl)
    n  = fst (last vl)
    prop x y = if (x, y) `elem` el
               then let v = fromJust (lookup x vl)
                    in case operator v of
                       (Filter x)    -> x
                       (FilterAcc x) -> x
                       _             -> 1
               else 0

    in bumpIndex2 (1 - m) $ listArray ((m,m),(n,n)) $ [ prop x y | x <- [m..n], y <- [m..n]]

-- | calculate an array of external input arrival probabilities
deriveInputsArray :: StreamGraph -> Double -> Array Int Double
deriveInputsArray sg totalArrivalRate = let
    vl = map (\v -> (vertexId v, v)) (vertexList sg)
    m  = fst (head vl)
    n  = fst (last vl)

    srcProp x = case lookup x vl of
        Nothing -> 0
        Just v  -> case operator v of
                Source x -> x / totalArrivalRate
                _        -> 0

    in bumpIndex (1 - m) $ listArray (m,n) $ map srcProp [m..n]


-- | derive an Array of service times from a StreamGraph
deriveServiceTimes :: StreamGraph -> Array Int Double
deriveServiceTimes sg = let
    vl = map (\v -> (vertexId v, v)) (vertexList sg)
    m  = fst (head vl)
    n  = fst (last vl)
    prop x = case lookup x vl of
        Nothing -> 0
        Just v  -> (Striot.StreamGraph.serviceTime) v

    in bumpIndex (1 - m) $ listArray (m,n) $ map prop [m..n]

calcAllSg :: StreamGraph -> [OperatorInfo]
calcAllSg sg = deBump $ calcAll propagation arrivals services
    where
        propagation      = derivePropagationArray sg
        totalArrivalRate = sum $ map (\(Source x) -> x) $ filter isSource $ map operator $ vertexList sg
        inputs           = deriveInputsArray sg totalArrivalRate
        services         = deriveServiceTimes sg
        arrivals         = arrivalRate propagation inputs totalArrivalRate

        -- re-adjust vertexIds down to the original range if it began <1
        -- and filter out any dummy vertices that were added to fill the range
        vIds             = map vertexId (vertexList sg)
        m                = head vIds
        adj              = 1 - m
        deBump           = filter (\oi -> opId oi `elem` vIds) . map (\oi -> oi { opId = opId oi - adj })

-- | Determine whether the supplied list of OperatorInfo describes a
-- StreamGraph which is over-utilised: at least one node receives events faster
-- than it can process them.
isOverUtilised :: [OperatorInfo] -> Bool
isOverUtilised = any (>1) . map util

graph = path
    [ StreamVertex 0 (Source 8)     [[| return 0   |]] "Int" "Int" 0
    , StreamVertex 4 Merge          []                 "Int" "Int" (1/5)
    , StreamVertex 1 (Filter (1/2)) [[|(>5)        |]] "Int" "Int" 0
    , StreamVertex 5 Sink           [[|mapM_ print |]] "Int" "Int" 0
    ]

test_isOverUtilised = assertBool $ isOverUtilised (calcAllSg graph)

------------------------------------------------------------------------------
-- Matrix.LU.inverse fails with 0-indexed arrays. bumpIndex and bumpIndex2 are
-- used to adjust the indexing of matrices so they begin at 1 in all dimensions.

both f (x,y) = (f x, f y)

-- 1D bumpIndex
bumpIndex  n a = ixmap (both (+n) (bounds a)) (\i -> i - n) a

-- 2D bumpIndex
bumpIndex2 n a = ixmap (both (both (+n)) (bounds a)) (both (\i -> i - n)) a

-- fails if NaN creeps in since NaN ≠ NaN
test_bumpInverse = assertEqual (inverse b) (inverse (bumpIndex2 1 a))
    where a = listArray ((0,0),(1,1)) [4,7,2,6]
          b = listArray ((1,1),(2,2)) [4,7,2,6]
