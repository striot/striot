{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-|
Module      : Striot.FunctionalProcessing
Description : StrIoT operators for processing Streams
Copyright   : © StrIoT maintainers, 2021
License     : Apache 2.0
Maintainer  : StrIoT maintainers
Stability   : experimental

The eight StrIoT low-level operators and related functions, as
well as some composite operators.
-}

module Striot.FunctionalProcessing (
    -- * The eight fundamental operators
     streamFilter
   , streamFilterAcc
   , streamMap
   , streamScan
   , streamWindow
   , streamExpand
   , streamMerge
   , streamJoin

   -- * Utility functions and convenience types
   , WindowMaker
   , WindowAggregator
   , sliding
   , slidingTime
   , chop
   , chopTime

   -- * Example composite operators
   , streamWindowAggregate
   , streamJoinE
   , streamJoinW

   -- * ???
   , complete
   , EventFilter
   , EventMap
   , JoinFilter
   , JoinMap

   , htf_thisModulesTests) where

import Striot.FunctionalIoTtypes
import Data.Time (UTCTime (..),addUTCTime,diffUTCTime,NominalDiffTime,picosecondsToDiffTime, Day (..))
import Test.Framework

-- Define the Basic IoT Stream Functions

-- Filter a Stream ...
type EventFilter alpha = alpha -> Bool                                 -- the type of the user-supplied function

streamFilter :: EventFilter alpha -> Stream alpha -> Stream alpha      -- if the value in the event meets the criteria then it can pass through
streamFilter ff s = filter (\(Event t v) -> case v of
                                                  Just val -> ff val
                                                  Nothing  -> True)      -- allow timestamped events to pass through for time-based windowing
                           s

type EventMap alpha beta = alpha -> beta
-- Map a Stream ...
streamMap :: EventMap alpha beta -> Stream alpha -> Stream beta
streamMap fm s = map (\(Event t v) -> case v of
                                            Just val -> Event t (Just (fm val))
                                            Nothing  -> Event t Nothing       ) -- allow timestamped events to pass through for time-based windowing
                     s

-- create and aggregate windows
type WindowMaker alpha = Stream alpha -> [Stream alpha]
type WindowAggregator alpha beta = [alpha] -> beta

streamWindow :: WindowMaker alpha -> Stream alpha -> Stream [alpha]
streamWindow fwm s = mapWindowId (fwm s)
           where getVals :: Stream alpha -> [alpha]
                 getVals s' = map (\(Event _ (Just val))->val) $ filter dataEvent s'
                 mapWindowId :: [Stream alpha] -> Stream [alpha]
                 mapWindowId [] = []
                 mapWindowId (x:xs) =
                     case x of
                         Event t _ : _ -> Event t       (Just (getVals x)) : mapWindowId xs
                         []            -> Event Nothing (Just [])          : mapWindowId xs

-- a useful function building on streamWindow and streamMap
streamWindowAggregate :: WindowMaker alpha -> WindowAggregator alpha beta -> Stream alpha -> Stream beta
streamWindowAggregate fwm fwa s = streamMap fwa $ streamWindow fwm s

-- some examples of window functions
sliding :: Int -> WindowMaker alpha
sliding wLength s = sliding' wLength $ filter dataEvent s
           where sliding':: Int -> WindowMaker alpha
                 sliding' wLength []      = []
                 sliding' wLength s@(h:t) = (take wLength s) : sliding' wLength t

slidingTime:: Int -> WindowMaker alpha -- the first argument is the window length in milliseconds
slidingTime tLength s = slidingTime' (milliToTimeDiff tLength) $ filter timedEvent s
                        where slidingTime':: NominalDiffTime -> Stream alpha -> [Stream alpha]
                              slidingTime' tLen []                        = []
                              slidingTime' tLen s@(Event (Just t) _:xs) = (takeTime (addUTCTime tLen t) s) : slidingTime' tLen xs

takeTime:: UTCTime -> Stream alpha -> Stream alpha
takeTime endTime []                                        = []
takeTime endTime (e@(Event (Just t) _):xs) | t < endTime = e : takeTime endTime xs
                                             | otherwise   = []

milliToTimeDiff :: Int -> NominalDiffTime
milliToTimeDiff x = toEnum (x * 10 ^ 9)

chop :: Int -> WindowMaker alpha
chop wLength s = chop' wLength $ filter dataEvent s
     where chop' wLength [] = []
           chop' wLength s  = w:(chop' wLength r) where (w,r) = splitAt wLength s

chopTime :: Int -> WindowMaker alpha -- N.B. discards events without a timestamp
chopTime _       []                         = []
chopTime tLength s@((Event (Just t) _):_) = chopTime' (milliToTimeDiff tLength) t $ filter timedEvent s
    where chopTime' :: NominalDiffTime -> UTCTime -> WindowMaker alpha -- the first argument is in milliseconds
          chopTime' _    _      []    = []
          chopTime' tLen tStart s     = let endTime           = addUTCTime tLen tStart
                                            (fstBuffer, rest) = timeTake endTime s
                                        in  fstBuffer : chopTime' tLen endTime rest

timeTake :: UTCTime -> Stream alpha -> (Stream alpha, Stream alpha)
timeTake endTime s = span (\(Event (Just t) _) -> t < endTime) s
                                        
complete :: WindowMaker alpha
complete s = [s]

-- Merge a set of streams that are of the same type. Preserve time ordering
streamMerge:: [Stream alpha]-> Stream alpha
streamMerge []     = []
streamMerge (x:[]) = x
streamMerge (x:xs) = merge' x (streamMerge xs)
    where merge':: Stream alpha -> Stream alpha -> Stream alpha
          merge' xs                               []                                       = xs
          merge' []                               ys                                       = ys
          merge' s1@(e1@(Event (Just t1) _):xs) s2@(e2@(Event (Just t2) _):ys) | t1 < t2   = e1: merge' s2 xs
                                                                               | otherwise = e2: merge' ys s1
          merge' (e1:xs)                          s2                                       = e1: merge' s2 xs  -- arbitrary ordering if 1 or 2 of the events aren't timed
                                                                                                                   -- swap order of streams so as to interleave

-- Join 2 streams by combining elements
streamJoin :: Stream alpha -> Stream beta -> Stream (alpha,beta)
streamJoin []                               []                               = []
streamJoin _                                []                               = []
streamJoin []                               _                                = []
streamJoin    ((Event t1 (Just v1)):r1)    ((Event _  (Just v2)):r2) = (Event t1 (Just(v1,v2))):(streamJoin r1 r2)
streamJoin    ((Event _  Nothing  ):r1) s2@((Event _  (Just v2)):_ ) = streamJoin r1 s2
streamJoin s1@((Event _  (Just v1)):_ )    ((Event _  Nothing  ):r2) = streamJoin s1 r2
streamJoin    ((Event _  Nothing  ):r1)    ((Event _  Nothing  ):r2) = streamJoin r1 r2

-- Join 2 streams by combining windows - some useful functions that build on streamJoin
type JoinFilter alpha beta        = alpha -> beta -> Bool
type JoinMap    alpha beta gamma  = alpha -> beta -> gamma

streamJoinE :: WindowMaker alpha ->
               WindowMaker beta ->
               JoinFilter alpha beta ->
               JoinMap alpha beta gamma ->
               Stream alpha ->
               Stream beta  ->
               Stream gamma
streamJoinE fwm1 fwm2 fwj fwm s1 s2 = streamExpand $ streamMap (cartesianJoin fwj fwm) $ streamJoin (streamWindow fwm1 s1) (streamWindow fwm2 s2)
    where cartesianJoin :: JoinFilter alpha beta -> JoinMap alpha beta gamma -> ([alpha],[beta]) -> [gamma]
          cartesianJoin jf jm (w1,w2) = map (\(e1,e2)->jm e1 e2) $ filter (\(e1,e2)->jf e1 e2) $ cartesianProduct w1 w2

          cartesianProduct:: [alpha] -> [beta] -> [(alpha,beta)]
          cartesianProduct s1 s2 = [(a,b)|a<-s1,b<-s2]

streamJoinW :: WindowMaker alpha ->
               WindowMaker beta  ->
              ([alpha] -> [beta] -> gamma)      -> Stream alpha -> Stream beta  -> Stream gamma
streamJoinW fwm1 fwm2 fwj s1 s2 = streamMap (\(w1,w2)->fwj w1 w2) $ streamJoin (streamWindow fwm1 s1) (streamWindow fwm2 s2)

-- Stream Filter with accumulating parameter
streamFilterAcc:: (beta -> alpha -> beta) -> beta -> (alpha -> beta -> Bool) -> Stream alpha -> Stream alpha
streamFilterAcc accfn acc ff []                         = []
streamFilterAcc accfn acc ff (e@(Event _ (Just v)):r) | ff v acc  = e:(streamFilterAcc accfn (accfn acc v) ff r)
                                                        | otherwise =    streamFilterAcc accfn (accfn acc v) ff r
streamFilterAcc accfn acc ff (e@(Event _ Nothing ):r)             = e:(streamFilterAcc accfn acc           ff r) -- allow events without data to pass through

-- Stream map with accumulating parameter
streamScan:: (beta -> alpha -> beta) -> beta -> Stream alpha -> Stream beta
streamScan _  _   []                       = []
streamScan mf acc (Event t (Just v):r) = Event t (Just newacc):streamScan mf newacc r where newacc = mf acc v
streamScan mf acc (Event t Nothing :r) = Event t Nothing      :streamScan mf acc    r -- allow events without data to pass through

instance Arbitrary UTCTime where
    arbitrary = do
        NonNegative d <- arbitrary :: Gen (NonNegative Integer)
        NonNegative i <- arbitrary :: Gen (NonNegative Integer)
        return $ UTCTime (ModifiedJulianDay d) (picosecondsToDiffTime i)

instance Arbitrary a => Arbitrary (Event a) where
    arbitrary = do
        i <- arbitrary
        t <- arbitrary
        return $ Event (Just t) (Just i)

prop_streamScan_samelength :: Stream Int -> Bool
prop_streamScan_samelength s = length s == length (streamScan (\_ x-> x) 0 s)

-- Map a Stream to a set of events
streamExpand :: Stream [alpha] -> Stream alpha
streamExpand s = concatMap eventExpand s
      where eventExpand :: Event [alpha] -> [Event alpha]
            eventExpand (Event t (Just v)) = map (\nv->Event t (Just nv)) v
            eventExpand (Event t Nothing ) = [Event t Nothing]

--streamSource :: Stream alpha -> Stream alpha
--streamSource ss = ss

-- streamSink:: (Stream alpha -> beta) -> Stream alpha -> beta
-- streamSink ssink s = ssink s

--- Tests ------
--t1 :: Int -> Int -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
--t1 tLen sLen s = splitAtValuedEvents tLen (take sLen s)

s1 :: Stream Int
s1 = [(Event (Just (addUTCTime i (read "2013-01-01 00:00:00"))) (Just 999))|i<-[0..]]

s2 :: Stream Int
s2 = [Event (Just (addUTCTime i (read "2013-01-01 00:00:00"))) Nothing |i<-[0..]]

s3 :: Stream Int
s3 = streamMerge [s1,s2]

s4 :: Stream Int
s4 = [Event Nothing (Just i)|i<-[0..]]

s5 :: Stream Int
s5 = streamMerge [s2,s4]

s6 :: Stream Int
s6 = [Event Nothing (Just i)|i<-[100..]]

ex1 i = streamWindow (sliding i) s3

ex2 i = streamWindow (chop i) s3

ex3 i = streamWindow (sliding i) s4

ex4 i = streamWindow (chop i) s4

ex5 = streamFilter (\v->v>1000) s1

ex6 = streamFilter (\v->v<1000) s1

sample :: Int -> Stream alpha -> Stream alpha
sample n s = streamFilterAcc (\acc h -> if acc==0 then n else acc-1) n (\h acc -> acc==0) s

ex7 = streamJoin s1 s4
ex8 = streamJoinW (chop 2) (chop 2) (\a b->(sum a)+(sum b)) s4 s6
ex9 = streamJoinE (chop 2) (chop 2) (\a b->a<b) (\a b->a+b) s4 s6
