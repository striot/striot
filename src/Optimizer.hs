module Striot.OptimizerIoT where
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.CompileIoT(StreamGraph(..),StreamOperation(..),StreamOperator(..),Partition,createPartitions)
import Data.List
import qualified Data.Map as Map


optimizer:: StreamGraph -> [Partition] -> [(Partition,[Int])] -> (StreamGraph -> [(Partition,[Int])] -> Double) -> [(Partition,StreamGraph)]
optimizer sg parts fixed costModel = let options                 = allPartitionings (map opid (operations sg)) parts fixed in
                                     let costs                   = map (costModel sg) options in
                                     let (idx,minC)              = foldl (\(indx,minCost) (i,c) -> if c<minCost then (i,c) else (indx,minCost)) (0,head costs) (zip [0..] costs) in
                                         createPartitions sg (options!!idx)                           

allPartitionings:: [Int] -> [Partition] -> [(Partition,[Int])] -> [[(Partition,[Int])]] -- list of operation ids, list of partitions, fixed operations, operation to partition map 
allPartitionings opIds parts fixedOps = map (Map.toList) $ map (\(fixed,variable)-> Map.unionWith (++) fixed variable) $ zip (repeat (Map.fromList fixedOps)) (genAll parts (opIds \\ (map fst fixedOps)))

genAll:: [Partition] -> [Int] -> [Map.Map Partition [Int]]
genAll  parts opIds =  map partitionListToMap (genAll' parts opIds  [[]])

genAll':: [Partition] -> [Int] -> [[(Partition,Int)]] -> [[(Partition,Int)]]
genAll' parts []       acc = acc
genAll' parts (o:rest) acc = genAll' parts rest (concatMap (\w-> [(p,o):w|p<-parts]) acc)

partitionListToMap::  [(Partition,Int)] -> Map.Map Partition [Int]
partitionListToMap pl = foldl (\m (p,o)-> Map.insertWith (++) p [o] m) Map.empty pl

-------- Tests
test1 = allPartitionings [1..2] [1..2] []
test2 = allPartitionings [1..2] [1..2] [(1,[1])]

