module Striot.ParallelIoT where
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Data.List

--type Distributor alpha = Stream alpha -> [Stream alpha]
type Distributor alpha = Stream alpha -> [(Int,Event alpha)]

aStream:: Stream Int -- example stream
aStream = [E i (read "2013-01-01 00:00:00") i|i<-[0..]]

parallelise:: Distributor alpha -> (Stream alpha -> Stream beta) -> Stream alpha -> Stream beta
parallelise df f s = f $ map snd $ df s
--parallelise df f s = streamMerge $ map f $ map (\ls->map snd ls) $ groupBy (\x y-> (fst x==fst y)) $ sortBy fst $ df s

distributorR:: Int -> Distributor alpha
distributorR max s = distributorR' 0 max s

distributorR':: Int -> Int -> Distributor alpha
distributorR'   i      max   []    = []
distributorR'   i      max   (h:t) = (i,h):(distributorR' (mod (i+1) max) max t)

-- throws away timestamps
--distributeHashByKey:: Distributor (Int,beta)
--distributeHashByKey s = map (\(E t (k,v))-> (mod k max,E t (k,v))) $ filter dataEvent s

parStreamMap:: Distributor alpha -> EventMap alpha beta -> Stream alpha -> Stream beta
parStreamMap dist mf s = parallelise dist (streamMap mf) s -- map the value, not the order

parStreamFilter:: Distributor alpha -> EventFilter alpha -> Stream alpha -> Stream alpha
parStreamFilter dist ff s = parallelise dist (streamFilter ff) s

parStreamWindowAggregate:: Distributor [alpha] -> WindowMaker alpha -> WindowAggregator alpha gamma -> Stream alpha -> Stream gamma
parStreamWindowAggregate dist fwm fwa s = parallelise dist (streamMap fwa) (streamWindow fwm s)

parstreamJoinE:: Distributor ([alpha],[beta]) -> WindowMaker alpha -> WindowMaker beta -> 
                 JoinFilter alpha beta -> JoinMap alpha beta gamma -> Stream alpha -> Stream beta -> Stream gamma
parstreamJoinE dist fwm1 fwm2 fwj fwm s1 s2 = parallelise dist (joinWindowsE fwj fwm) (combineStreamWindows fwm1 fwm2 s1 s2)

parstreamJoinW:: Distributor ([alpha],[beta]) -> WindowMaker alpha -> WindowMaker beta -> 
                 ([alpha] -> [beta] -> gamma) -> Stream alpha -> Stream beta -> Stream gamma
parstreamJoinW dist fwm1 fwm2 fwm s1 s2 =  parallelise dist (joinWindowsW fwm)    (combineStreamWindows fwm1 fwm2 s1 s2)

-- not complete
--parStreamScan:: Int -> Distributor -> beta -> (alpha -> beta -> beta) -> (alpha -> beta -> beta) -> Stream alpha -> Stream beta
--parStreamScan max dist acc accfn mapfn ((E t h):rest) = let newAcc = accfn h acc in
--                                                             (E t (mapfn h acc)):(streamMapAcc newAcc accfn mapfn rest)
--parStreamScan max dist acc accfn mapfn (_      :rest) =                     (streamMapAcc acc    accfn mapfn rest)
--parStreamScan max dist acc accfn mapfn []             = []

parStreamExpand:: Distributor [alpha] -> Stream [alpha] -> Stream alpha
parStreamExpand dist s = parallelise dist streamExpand s

{-
-- order a stream that may be a little out of order (e.g. after parallelising an operation on its events)
streamOrder:: Stream (Int,alpha) -> Stream alpha
streamOrder s = order' 0 [] s

order':: Int -> [(Int,alpha)] -> Stream (Int,alpha) -> Stream alpha
order' i orderedList ((E t (j,v)):r) | j == i    = (E t v)::(order'' (i+1) orderedList                                                                   r)
                                     | otherwise =          (order'  i     (insertBy (\a b -> (fst $ value a) LT (fst $ value b)) (E t (j,v)) orderList) r)
order' i orderedList ((V   (j,v)):r) | j == i    = (V   v)::(order'' (i+1) orderedList                                                                   r)
                                     | otherwise =          (order'  i     (insertBy (\a b -> (fst $ value a) LT (fst $ value b)) (V   (j,v)) orderList) r)
order' i orderedList ((T t      ):r)             = (T   v)::(order'' (i+1) orderedList                                                                   r)

order'':: Int -> [(Int,alpha)] -> Stream (Int,alpha) -> Stream alpha
order''   i (h:t) s | value h == i = h::(order'' (i+1) t s)
order''   i (h:t) s | otherwise    = order' i (h:t) s
-}
-- tests
test5 max s = parallelise (distributorR max) (streamMap (\i->i)) s
test6 max s = parallelise (distributorR max) (streamFilter (\i->(mod i 2)==1)) s
test7 max s = parallelise (distributorR max) ((streamFilter (\i->(mod i 2)==1)).(streamMap (\i->i+1))) s
