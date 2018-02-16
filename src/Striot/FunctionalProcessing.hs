module Striot.FunctionalProcessing ( Source
                                   , streamFilter
                                   , streamMap
                                   , streamWindow
                                   , streamWindowAggregate
                                   --, joinWindowsE
                                   --, joinWindowsW
                                   , streamMerge
                                   , streamJoin
                                   , streamJoinE
                                   , streamJoinW
                                   , streamFilterAcc
                                   , streamScan
                                   , streamExpand
                                   , WindowMaker
                                   , WindowAggregator
                                   , sliding
                                   , slidingTime
                                   , chop
                                   , chopTime
                                   , complete
                                   , EventFilter
                                   , EventMap
                                   , JoinFilter
                                   , JoinMap) where

import Striot.FunctionalIoTtypes
import Data.Time (UTCTime,addUTCTime,diffUTCTime,NominalDiffTime)

type Source alpha = Stream alpha -- a source of data

-- Define the Basic IoT Stream Functions

-- Filter a Stream ...
type EventFilter alpha = alpha -> Bool                                     -- the type of the user-supplied function

streamFilter :: EventFilter alpha -> Stream alpha -> Stream alpha           -- if the value in the event meets the criteria then it can pass through
streamFilter ff []                      = []
streamFilter ff (e@(E id t v):r) | ff v      = e         : streamFilter ff r
                                 | otherwise = (T id t  ): streamFilter ff r     -- always allow timestamps to pass through for use in time-based windowing
streamFilter ff (e@(V id   v):r) | ff v      = e         : streamFilter ff r
                                 | otherwise =             streamFilter ff r     -- needs a T to be generated?
streamFilter ff (e@(T id t  ):r)             = e         : streamFilter ff r

-- Map a Stream ...
streamMap :: EventMap alpha beta -> Stream alpha -> Stream beta
streamMap fm s = map (eventMap fm) s

type EventMap alpha beta = alpha -> beta
eventMap :: EventMap alpha beta -> Event alpha -> Event beta
eventMap fm (E id t v) = E id t (fm v)
eventMap fm (V id   v) = V id   (fm v)
eventMap fm (T id t  ) = T id t        -- allow timestamps to pass through untouched

-- create and aggregate windows
type WindowMaker alpha = Stream alpha -> [Stream alpha]
type WindowAggregator alpha beta = [alpha] -> beta

streamWindow :: WindowMaker alpha -> Stream alpha -> Stream [alpha]
streamWindow fwm s = map (\win-> if timedEvent $ head win
                                 then E 0 (time $ head win) (getVals win)
                                 else V 0                   (getVals win))
                         (fwm s)

streamWindowAggregate :: WindowMaker alpha -> WindowAggregator alpha beta -> Stream alpha -> Stream beta
streamWindowAggregate fwm fwa s = streamMap fwa $ streamWindow fwm s

getVals :: Stream alpha -> [alpha]
getVals s = map value $ filter dataEvent s

splitAtValuedEvents :: Int -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
splitAtValuedEvents length s = splitAtValuedEvents' length [] s

splitAtValuedEvents':: Int -> Stream alpha -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
splitAtValuedEvents' 0      acc s                     = (True ,acc, s)
splitAtValuedEvents' length acc []                    = (False,[] ,[])
splitAtValuedEvents' length acc (h:t) | dataEvent h = splitAtValuedEvents' (length-1) (acc++[h]) t
                                      | otherwise   = splitAtValuedEvents' length     acc        t

-- Examples of WindowMaker functions
-- A sliding window of specified length : a new window is created for every event received
sliding :: Int -> WindowMaker alpha
sliding wLength s =
    let (validWindow, fstWindow, rest) = splitAtValuedEvents wLength s
    in   -- ignores events with no value
        if validWindow then fstWindow : sliding' fstWindow rest else []

sliding' :: Stream alpha -> WindowMaker alpha
sliding' s@(_:tb) (h:t)
    | dataEvent h = let newWindow = tb ++ [h]
                    in  newWindow : sliding' newWindow t
    | otherwise = sliding' s t
sliding' _  [] = []
sliding' [] _  = []

slidingTime :: Int -> WindowMaker alpha -- needs checking
slidingTime tLength s@(h:t) =
    let endTime           = addUTCTime (milliToTimeDiff tLength) (time h)
        (fstBuffer, rest) = timeTake endTime s
        revfstBuffer      = reverse fstBuffer
        validWindow       = time (head revfstBuffer) < endTime
    in  if validWindow
            then fstBuffer : slidingTime' tLength revfstBuffer rest
            else []
slidingTime _ [] = []

slidingTime' :: Int -> Stream alpha -> WindowMaker alpha
slidingTime' tLength buffer s@(h:t) =
    let
        (sametEvents, rest) = span (\e -> time e == time h) t -- find all next events with same time
        startTime           = addUTCTime (-(milliToTimeDiff tLength)) (time h)
        (revnewWindow, remaining) =
            span (\e -> time e > startTime) (h : sametEvents ++ buffer)
        newWindow = reverse revnewWindow
        validWindow =
            (time (last revnewWindow) == startTime) || not (null remaining)
    in
        if validWindow
            then newWindow : slidingTime' tLength revnewWindow rest
            else []
slidingTime' _ _ [] = []

timeTake :: UTCTime -> Stream alpha -> (Stream alpha, Stream alpha)
timeTake endTime = span (\h -> time h < endTime) -- changed from <=

milliToTimeDiff :: Int -> NominalDiffTime
milliToTimeDiff x = toEnum (x * 10 ^ 9)

chop :: Int -> WindowMaker alpha
chop wLength s =
    let (validWindow, fstWindow, rest) = splitAtValuedEvents wLength s
    in  -- remove events with no value
        if validWindow then fstWindow : chop wLength rest else []

chopTime :: Int -> WindowMaker alpha -- assumes all events have a timestamp; the first argument is in milli seconds
chopTime tLength s@(h:t) = chopTime' tLength (time h) s
chopTime _       []      = []

chopTime' :: Int -> UTCTime -> WindowMaker alpha -- assumes all events have a timestamp; the first argument is in milliseconds
chopTime' tLength start s@(h:t) =
    let endTime = addUTCTime (milliToTimeDiff tLength) start
    in  let (fstBuffer, rest) = timeTake endTime s
        in  fstBuffer : chopTime' tLength endTime rest
chopTime' _ _ [] = []

complete :: WindowMaker alpha
complete s = [s]

-- Merge a set of streams that are of the same type. Preserve time ordering
streamMerge:: [Stream alpha]-> Stream alpha
streamMerge []     = []
streamMerge (x:[]) = x
streamMerge (x:xs) = merge' x (streamMerge xs)

merge':: Stream alpha -> Stream alpha -> Stream alpha
merge' xs         []                                          = xs
merge' []         ys                                          = ys
merge' s1@(e1:xs) s2@(e2:ys) | timedEvent e1 && timedEvent e2 = if   time e1 < time e2
                                                                then e1: merge' s2 xs
                                                                else e2: merge' ys s1
                             | otherwise                      = e1: merge' s2 xs  -- arbitrary ordering if 1 or 2 of the events aren't timed
                                                                                  -- swap order of streams so as to interleave

-- Join 2 streams by combining elements
streamJoin :: Stream alpha -> Stream beta -> Stream (alpha,beta)
streamJoin []                 []                 = []
streamJoin _                  []                 = []
streamJoin []                 _                  = []
streamJoin ((E id1 t1 v1):r1) ((E id2 t2 v2):r2) = (E id1 t1 (v1,v2)):(streamJoin r1 r2)
streamJoin ((E id1 t1 v1):r1) ((V id2    v2):r2) = (E id1 t1 (v1,v2)):(streamJoin r1 r2)
streamJoin ((V id1    v1):r1) ((E id2 t2 v2):r2) = (E id1 t2 (v1,v2)):(streamJoin r1 r2)
streamJoin ((V id1    v1):r1) ((V id2    v2):r2) = (V id2    (v1,v2)):(streamJoin r1 r2)
streamJoin s1                 ((T id2 t2   ):r2) = (T id2 t2)        :(streamJoin s1 r2)
streamJoin ((T id1 t1   ):r1) s2                 = (T id1 t1)        :(streamJoin r1 s2)

-- Join 2 streams of different types by combining windows
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

--was joinWindowsE fwj fwm $ combineStreamWindows fwm1 fwm2 s1 s2

cartesianJoin :: JoinFilter alpha beta -> JoinMap alpha beta gamma -> ([alpha],[beta]) -> [gamma]
cartesianJoin jf jm (w1,w2) = map (\(e1,e2)->jm e1 e2) $ filter (\(e1,e2)->jf e1 e2) $ cartesianProduct w1 w2

cartesianProduct:: [alpha] -> [beta] -> [(alpha,beta)]
cartesianProduct s1 s2 = [(a,b)|a<-s1,b<-s2]

streamJoinW :: WindowMaker alpha ->
              WindowMaker beta  ->
              ([alpha] -> [beta] -> gamma)      -> Stream alpha -> Stream beta  -> Stream gamma
streamJoinW fwm1 fwm2 fwj s1 s2 = streamMap (\(w1,w2)->fwj w1 w2) $ streamJoin (streamWindow fwm1 s1) (streamWindow fwm2 s2)

-- Stream Filter with accumulating parameter
streamFilterAcc:: (beta -> alpha -> beta) -> beta -> (alpha -> beta -> Bool) -> Stream alpha -> Stream alpha
streamFilterAcc accfn acc filterfn []              = []
streamFilterAcc accfn acc filterfn ((T id t):rest) =         streamFilterAcc accfn    acc filterfn rest
streamFilterAcc accfn acc filterfn (e       :rest) = let  newAcc = accfn acc (value e) in
                                                        if   filterfn (value e) acc
                                                        then e:(streamFilterAcc accfn newAcc filterfn rest)
                                                        else   (streamFilterAcc accfn newAcc filterfn rest)

-- Stream map with accumulating parameter
streamScan:: (beta -> alpha -> beta) -> beta -> Stream alpha -> Stream beta
streamScan mapfn acc ((T id t  ):rest) =                 (streamScan mapfn acc    rest)
streamScan mapfn acc ((E id t v):rest) = (E id t newacc):(streamScan mapfn newacc rest) where newacc = mapfn acc v
streamScan mapfn acc ((V id   v):rest) = (V id   newacc):(streamScan mapfn newacc rest) where newacc = mapfn acc v
streamScan mapfn acc []                = (V 0    acc   ):[]

-- Map a Stream to a set of events
streamExpand :: Stream [alpha] -> Stream alpha
streamExpand s = concatMap eventExpand s

eventExpand :: Event [alpha] -> [Event alpha]
eventExpand (E id t v) = map (\nv->E id t nv) v
eventExpand (V id   v) = map (\nv->V id   nv) v
eventExpand (T id t  ) = [T id t]

streamSource :: Stream alpha -> Stream alpha
streamSource ss = ss

streamSink:: (Stream alpha -> beta) -> Stream alpha -> beta
streamSink ssink s = ssink s

--- Tests ------
t1 :: Int -> Int -> Stream alpha -> (Bool,Stream alpha,Stream alpha)
t1 tLen sLen s = splitAtValuedEvents tLen (take sLen s)

s1 :: Stream Int
s1 = [(E 0 (addUTCTime i (read "2013-01-01 00:00:00")) 999)|i<-[0..]]

s2 :: Stream Int
s2 = [T 0 (addUTCTime i (read "2013-01-01 00:00:00")) |i<-[0..]]

s3 :: Stream Int
s3 = streamMerge [s1,s2]

s4 :: Stream Int
s4 = [V i i|i<-[0..]]

s5 :: Stream Int
s5 = streamMerge [s2,s4]

s6 :: Stream Int
s6 = [V i i|i<-[100..]]

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
