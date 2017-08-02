{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Striot.CompileIoT ( StreamGraph(..)
                         , StreamOperation(..)
                         , StreamOperator(..)
                         , Partition
                         , Id
                         , createPartitions
                         , createPartitionsAndEdges
                         , graphEdgesWithTypes
                         , printParams
                         , generateCode
                         , htf_thisModulesTests
                         ) where

import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Data.List
import qualified Data.Map as Map
import System.IO
import Test.Framework


type Id     = Int 
type Port   = Int
type PortId = (Id,Port)
type Param  = String  
data StreamOperator  = Map             |
                       Filter          |
                       Expand          |
                       Window          |
                       Merge           |
                       Join            |
                       Scan            |
                       FilterAcc       |
                       Source          |
                       Sink
                       deriving (Eq)

data StreamGraph = StreamGraph
   { gid        :: String
   , resultId   :: Id
   , ginputs    :: [(Id,String)] -- the String is the Type of the input
   , operations :: [StreamOperation]}
      deriving (Show, Eq)

data StreamOperation  = StreamOperation
   { opid       :: Int
   , opinputs   :: [Int]
   , operator   :: StreamOperator
   , parameters :: [String]
   , outputType :: String
   , imports    :: [String]}
     deriving (Show, Eq)

instance Show StreamOperator where
    show Map             = "streamMap"
    show Filter          = "streamFilter"
    show Window          = "streamWindow"
    show Merge           = "streamMerge"
    show Join            = "streamJoin"
    show Scan            = "streamScan"
    show FilterAcc       = "streamFilterAcc"
    show Expand          = "streamExpand"
    show Source          = "streamSource"
    show Sink            = "streamSink"
    
type OutputType  = String
type PartitionId = String
type GraphId     = String

showStreamGraph :: [String] -> StreamGraph -> String
showStreamGraph stdimports sg = (printImports $ stdimports ++ (nub (concatMap imports $ operations sg))) ++ "\n" ++
                                (gid sg) ++ "::" ++ (printInputTypes (map snd (ginputs sg))) ++ (if null (ginputs sg) then "" else " -> ") ++ (getType (operations sg) (resultId sg)) ++ "\n" ++
                                (gid sg) ++ " "  ++ (printArgs (map fst (ginputs sg))) ++ " = " ++ (intercalate padding (map showOperation (operations sg))) ++ padding ++ "n" ++ (show (resultId sg)) ++ "\n"
                                      where padding = spaces ((length $ gid sg) + (length $ ginputs sg) + (length (map show $ ginputs sg)) + 4)

printArgs:: [Id] -> String
printArgs ids = intercalate " " (map (\i->"n" ++ show i) ids)

printImports:: [String] -> String
printImports is = concatMap (\i->"import " ++ i ++"\n") is 

printInputTypes::[String] -> String
printInputTypes types = intercalate " -> " types

spaces:: Int -> String
spaces n = concat (take n (repeat " "))

getStreamOperation:: Id -> [StreamOperation] -> StreamOperation
getStreamOperation id sops =  head $ filter (\sop->(opid sop)==id) sops

getType:: [StreamOperation] -> Id -> String
getType sg i =  outputType $ getStreamOperation i sg

showOperation:: StreamOperation -> String
showOperation sop = "let n" ++ show (opid sop) ++ " = " ++ show (operator sop) ++ " " ++ printParams (parameters sop) ++ " " ++ printArgs (opinputs sop) ++ " in\n"

printParams:: [String] -> String
printParams ss = intercalate " " (map addBrackets ss)

addBrackets:: String -> String
addBrackets a = "(" ++ a ++ ")" 

-- Sgraph operations
data Graph alpha = Graph {nodes::[(Id,alpha)],edges::[(Id,PortId)]}
    deriving (Show)
    
type Sgraph = Graph (StreamOperator,[Param])

toGraph:: StreamGraph -> Sgraph 
toGraph sg = foldl (\g s->addStreamOperationToGraph g s) (Graph [] []) (operations sg)

removeNode:: Graph alpha -> Id -> Graph alpha
removeNode g id = Graph (filter (\(i,v)->i/=id) (nodes g)) (filter (\(s,(d,p))->s/=id && d/=id) (edges g))

removeNodeNotEdges:: Graph alpha -> Id -> Graph alpha
removeNodeNotEdges g i = Graph (filter (\(id,v)->id /= i) (nodes g)) (edges g)

removeNodeButNotInlinks:: Graph alpha -> Id -> Graph alpha
removeNodeButNotInlinks g id = Graph (filter (\(i,v)->i/=id) (nodes g)) (filter (\(s,d)->s/=id) (edges g))
                                 
updateSources:: [(Id,PortId)] -> Id -> Id -> [(Id,PortId)]
updateSources es n n' = map (\(s,d)-> if s == n then (n',d) else (s,d)) es

updateDestinations:: [(Id,PortId)] -> Id -> Id -> [(Id,PortId)]
updateDestinations es n n' = map (\(s,(d,p))-> if d == n then (s,(n',p)) else (s,(d,p))) es

destinations:: Graph alpha -> [Id] -> [PortId]
destinations g ns = map snd $ filter (\(s,d)    -> elem s ns) $ edges g

sources:: Graph alpha -> [Id] -> [Id]
sources      g ns = map fst $ filter (\(s,(d,p))-> elem d ns) $ edges g

addStreamOperationToGraph:: Sgraph -> StreamOperation -> Sgraph
addStreamOperationToGraph (Graph gn ge) sop = Graph ((opid sop,(operator sop,parameters sop)):gn) ((mkEdges (opinputs sop) (opid sop))++ge)

mkEdges:: [Id] -> Id -> [(Id,PortId)]
mkEdges inputs target = [(input,(target,port))|(input,port)<-zip inputs [1..]]

outLinks:: Graph alpha -> Graph alpha -> [PortId] -- Full graph -> a subset of the graph
outLinks g p = filter (\(d,p)->not (elem d nodesInPartition)) $ destinations g nodesInPartition
                  where nodesInPartition = nodeIds p
------

graphInLinks:: StreamGraph -> [Id] -- Links into a Graph
graphInLinks g = map fst $ ginputs g

graphOperationIds:: StreamGraph -> [Id]
graphOperationIds g = map opid (operations g)

graphOutLinks:: StreamGraph -> StreamGraph -> [Id] -- Links out of a Sub-Graph (p)
graphOutLinks g p = nub $ map (\(source,(dest,port)) -> dest)
                        $ filter (\(source,(dest,port)) ->      elem source (graphOperationIds p))  -- source in partition
                        $ filter (\(source,(dest,port)) -> not (elem dest   (graphOperationIds p))) -- target not in partition
                        $ graphEdges g
        
graphEdges:: StreamGraph -> [(Id,PortId)]
graphEdges g = concatMap mkGraphEdges $ map (\sop-> (opid sop,opinputs sop)) $ operations g

mkGraphEdges:: (Id,[Id]) -> [(Id,PortId)]
mkGraphEdges (dest,sources) = map (\(portId,source)->(source,(dest,portId))) $ zip [1..] sources

graphEdgesWithTypes::  StreamGraph -> [(Id,PortId,String)]
graphEdgesWithTypes g = concatMap mkGraphEdgesWithType $ map (\sop-> (opid sop,
                                                                      opinputs sop,
                                                                      map (\inp->outputType $ getStreamOperation inp (operations g)) (opinputs sop)))  (operations g)

mkGraphEdgesWithType:: (Id,[Id],[String]) -> [(Id,PortId,String)]
mkGraphEdgesWithType (dest,sources,oTypes) = map (\(portId,source,oType)->(source,(dest,portId),oType++":"++(show portId))) $ zip3 [1..] sources oTypes

inLinks:: Graph alpha -> Graph alpha -> [Id] -- Full graph -> a subset of the graph
inLinks  g p = sources g (nodeIds p) \\ (nodeIds p)

nodeIds:: Graph alpha -> [Id]
nodeIds g = map fst (nodes g)

nodeOutlinks:: Graph alpha -> Id -> [PortId]
nodeOutlinks g i = destinations g [i]

nodeInlinks:: Graph alpha -> Id -> [Id]
nodeInlinks g i = sources g [i]

zeroOps:: Graph alpha -> [Id]
zeroOps g = filter (\n->null (nodeOutlinks g n)) $ nodeIds g

zeroIps:: Graph alpha -> [Id]
zeroIps g = filter (\n->null (nodeInlinks g n)) $ nodeIds g

data PartitionType = SingularPartition | SourcePartition | LinkPartition | Link2Partition | SinkPartition | Sink2Partition
    deriving (Show)
{-    
graphPartitionCategory:: StreamGraph -> StreamGraph -> PartitionType -- full graph -> a partition
graphPartitionCategory s p   | null (graphInLinks  p)   && null (graphOutLinks s p) = SingularPartition
                             | null (graphInLinks  p)                               = SourcePartition
                             |                             null (graphOutLinks s p) = SinkPartition
                             | otherwise                                            = LinkPartition
-}                             

graphPartitionCategory:: StreamGraph -> StreamGraph -> PartitionType -- full graph -> a partition
graphPartitionCategory s p   = graphPartitionCategory' (length $ graphInLinks  p) (length $ graphOutLinks s p)
                             
graphPartitionCategory':: Int -> Int -> PartitionType -- #Inlinks -> #Outlinks -> PartitionType
graphPartitionCategory' 0 0 = SingularPartition
graphPartitionCategory' 0 _ = SourcePartition
graphPartitionCategory' 1 0 = SinkPartition
graphPartitionCategory' 2 0 = Sink2Partition
graphPartitionCategory' 1 _ = LinkPartition
graphPartitionCategory' 2 _ = Link2Partition

{-
partitionCategory:: Sgraph -> Sgraph -> PartitionType -- the full graph -> this partition
partitionCategory s p   | null (inLinks  s p) && null (outLinks s p) = SingularPartition
                        | null (inLinks  s p)                        = SourcePartition
                        | null (outLinks s p)                        = SinkPartition
                        | otherwise                                  = LinkPartition

partitionCatAndLinks:: Sgraph -> Sgraph -> (PartitionType,[Id],[PortId])
partitionCatAndLinks g p = (partitionCategory g p,inLinks  g p,outLinks g p)
-}
--------Replace Node----------
replaceNode:: Graph alpha -> Id -> Graph alpha -> Id -> Id ->  Graph alpha
replaceNode g nid r rStart rEnd = combineGraphs (removeNodeButNotInlinks g nid) (renumberGraph r g nid (nodeOutlinks g nid) rStart rEnd)  -- nid is node to be replaced in g by r (which starts at rStart and ends at rEnd) 

renumberGraph:: Graph alpha -> Graph alpha -> Id -> [PortId] -> Id -> Id -> Graph alpha
renumberGraph r g nid outs rStart rEnd = let freeIds   = [((maximum (nodeIds g))+1) ..] in
                                         let repMap    = (rStart,nid):(zip ((nodeIds r) \\ [rStart]) freeIds) in
                                             Graph (map (replaceNodeId repMap) (nodes r)) ((map (replaceEdgeId repMap) (edges r))++(zip (repeat (let Just newEnd = lookup rEnd repMap in newEnd)) outs))

replaceNodeId:: [(Id,Id)] -> (Id,alpha) -> (Id,alpha)
replaceNodeId aTob (i,v) = let Just a = lookup i aTob in (a,v)

replaceEdgeId:: [(Id,Id)] -> (Id,PortId) -> (Id,PortId)
replaceEdgeId aTob (s,(d,p)) = let Just s' = lookup s aTob in 
                               let Just d' = lookup d aTob in
                                  (s',(d',p))
                                           
combineGraphs:: Graph alpha -> Graph alpha -> Graph alpha
combineGraphs g1 g2 = Graph ((nodes g1) ++ (nodes g2)) ((edges g1) ++ (edges g2))

-----------
type Partition = Int
createPartitions:: StreamGraph -> [(Partition,[Id])] -> [(Partition,StreamGraph)]
createPartitions g partitionMap = map (\(pid,ids)->(pid,subsetGraphByIds g ids pid)) partitionMap

subsetGraphByIds:: StreamGraph -> [Id] -> Partition -> StreamGraph
subsetGraphByIds sg ids pid = StreamGraph ((gid sg) ++(show pid)) -- name
                                          (opid $ head $ filter (\sop -> not (elem (opid sop) allInputs)) allOpsInPartition) -- output node id
                                          (map (\ip-> (ip,outputType $ getStreamOperation ip $ operations sg)) $ allInputs \\ ids)   -- inputs to partition
                                          allOpsInPartition
                                                   where allOpsInPartition           = filter (\op -> elem (opid op) ids) $ operations sg
                                                         idsOfallOpsOutsidePartition = (map opid (operations sg)) \\ ids
                                                         allInputs                   = nub (concatMap (\op -> opinputs op) allOpsInPartition)

createPartitionEdges:: [(Partition,[Id])] -> [(Partition,StreamGraph)] -> [((Partition,Int),(Partition,Int))]
createPartitionEdges partitionMap partitions = let idToPart = idToPartition partitionMap in -- creates a map from opid to partition
                                                  concatMap (\(pid,pg)-> map (\(inputNumber,ipop)-> ((idToPart Map.! ipop,1),(pid,inputNumber))) $ zip [1..] (map fst (ginputs pg))) partitions
                                               
createPartitionsAndEdges:: StreamGraph -> [(Partition,[Id])] -> ([(Partition,StreamGraph)],[((Partition,Int),(Partition,Int))])
createPartitionsAndEdges sg partitionMap = let parts = createPartitions sg partitionMap in
                                               (parts,createPartitionEdges partitionMap parts)

idToPartition::[(Partition,[Int])] -> Map.Map Int Partition
idToPartition partitionMap = Map.unions $ map (\(pid,opids)->Map.fromList (zip opids (repeat pid))) partitionMap
                                              
-------------Examples----------------------                                                
s0 :: StreamGraph
s0 = StreamGraph "jmtdtest" 2 [] [
       StreamOperation 1 [ ] Source ["sourceGen"] "Stream Trip" ["Taxi.hs","SourceGenerator.hs"],
       StreamOperation 2 [1] Sink   ["print"]     ""            []
     ]

s1:: StreamGraph
s1  = StreamGraph "test" 7 [] [
       StreamOperation 1 [ ] Source    ["sourceGen"]                                                                           "Stream Trip"            ["Taxi.hs","SourceGenerator.hs"],
       StreamOperation 2 [1] Map       ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"]                  "Stream Journey"         ["Taxi.hs"],
       StreamOperation 3 [2] Filter    ["\\j-> inRangeQ1 (start j) && inRangeQ1 (end j)"]                                      "Stream Journey"         ["Taxi.hs"],
       StreamOperation 4 [3] Window    ["slidingTime 1800"]                                                                    "Stream [Journey]"       ["Taxi.hs"],
       StreamOperation 5 [4] Map       ["mostFrequent 10"]                                                                     "Stream [(Journey,Int)]" ["Taxi.hs"],
       StreamOperation 6 [5] FilterAcc ["value $ head s","\\h acc-> if (h==acc) then acc else h","\\h acc->(h/=acc)","tail s"] "Stream [(Journey,Int)]" ["Taxi.hs"],
       StreamOperation 7 [6] Sink      ["print"]                                                                               ""                                []]

p1:: StreamGraph
p1  = StreamGraph "partition1" 6 [(2,"Stream Journey")] [
       StreamOperation 3 [2] Filter    ["\\j-> inRangeQ1 (start j) && inRangeQ1 (end j)"]                                      "Stream Journey"         ["Taxi.hs"],
       StreamOperation 4 [3] Window    ["slidingTime 1800"]                                                                    "Stream [Journey]"       ["Taxi.hs"],
       StreamOperation 5 [4] Map       ["mostFrequent 10"]                                                                     "Stream [(Journey,Int)]" ["Taxi.hs"],
       StreamOperation 6 [5] FilterAcc ["value $ head s","\\h acc-> if (h==acc) then acc else h","\\h acc->(h/=acc)","tail s"] "Stream [(Journey,Int)]" ["Taxi.hs"]]

p2:: StreamGraph
p2  = StreamGraph "test" 2 [] [
       StreamOperation 1 [ ] Source         ["sourceGen"]                                                                            "Stream Trip"    ["Taxi.hs","SourceGenerator.hs"],
       StreamOperation 2 [1] Map            ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"]                   "Stream Journey" ["Taxi.hs"]]
       
p3:: StreamGraph
p3  = StreamGraph "test" 2 [(1,"Stream Trip")] [
       StreamOperation 2 [1] Map            ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"]                   "Stream Journey" ["Taxi.hs"]]
       
s2:: StreamGraph
s2  = StreamGraph "wikiJacksonEx" 3 [] [
       StreamOperation 0 [ ]   Source ["sourceGen"] "Stream Int" ["SourceGenerator.hs"],
       StreamOperation 1 [0]   Map    ["\\t-> t"]   "Stream Int" [],
       StreamOperation 2 [0]   Map    ["\\t-> t"]   "Stream Int" [],
       StreamOperation 3 [0,1] Map    ["\\t-> t"]   "Stream Int" []]

       
{-       
parMap:: StreamGraph
parMap = StreamGraph "parallelMap" 4 [] [
         StreamOperation 1 [ ]   Split ["roundRobin"]                                                         "Stream alpha" [],
         StreamOperation 2 [1]   Map   ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"] "Stream alpha" ["Taxi.hs"],
         StreamOperation 3 [1]   Map   ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"] "Stream alpha" ["Taxi.hs"], 
         StreamOperation 4 [2,3] Merge []                                                                     "Stream alpha" []]
       
ex1 = putStrLn $ showStreamGraph [] s1
ex2 = toGraph s1
s1g = toGraph s1
p1g = toGraph p1
p2g = toGraph p2
p3g = toGraph p3
ex3 = edges ex2
ex4 = partitionCategory s1g p1g
ex5 = partitionCategory s1g p2g
ex6 = partitionCategory s1g p3g
ex7 = partitionCategory s1g s1g
ex8 = inLinks s1g p3g
ex9 = outLinks s1g p3g
ex10 = partitionCatAndLinks s1g p1g
ex11 = partitionCatAndLinks s1g p2g
ex12 = partitionCatAndLinks s1g p3g
ex13 = partitionCatAndLinks s1g s1g
parMapg = toGraph parMap
ex14 = replaceNode s1g 2 parMapg 1 4

ex15 = do  
         handle <- openFile "output15.txt" WriteMode   
         hPutStr handle (show ex14)  
         hClose handle 

ex16 = do         
         handle <- openFile "output16.txt" WriteMode   
         hPutStr handle (show s1g)  
         hClose handle 

ex17 = do
         handle <- openFile "output17.txt" WriteMode   
         hPutStr handle (show parMapg)  
         hClose handle 
         
ex18 = do
         handle <- openFile "output18.txt" WriteMode   
         hPutStr handle (show (removeNodeButNotInlinks s1g 2))  
         hClose handle 

ex19 = do
         handle <- openFile "output19.txt" WriteMode   
         hPutStr handle (show (renumberGraph parMapg s1g 2 (nodeOutlinks s1g 2) 1 4))  
         hClose handle 

-- ex20 = allDeployments s1g [1..2] -}

-- some basic tests
test_partition_s0 = assertEqual (createPartitions s0 [(1,[1]),(2,[2])])
        [(1 :: Partition, StreamGraph { gid = "jmtdtest1"
                                      , resultId = 1
                                      , ginputs = []
                                      , operations = [ StreamOperation { opid = 1
                                                                       , opinputs = []
                                                                       , operator = Source
                                                                       , parameters = ["sourceGen"]
                                                                       , outputType = "Stream Trip"
                                                                       , imports = ["Taxi.hs","SourceGenerator.hs"]}]}),
         (2 :: Partition, StreamGraph { gid = "jmtdtest2"
                                      , resultId = 2
                                      , ginputs = [(1, "Stream Trip")]
                                      , operations = [ StreamOperation { opid = 2
                                                                       , opinputs = [1]
                                                                       , operator = Sink
                                                                       , parameters = ["print"]
                                                                       , outputType = ""
                                                                       , imports = [] }]})]
-- Test certain inputs generate a failure.
-- Expect an exception to be thrown (not yet implemented, so these tests fail)

test_empty_partitionmap = assertThrowsSome $ createPartitions s0 []

-- partition map refers to non-existent graph nodes
test_too_many_partitions = assertThrowsSome $ createPartitions s0 [(1,[1]),(2,[2]),(3,[3])]

-- partition map has multiple references to graph nodes
test_repeating_nodes = assertThrowsSome $ createPartitions s0 [(1,[1]),(2,[1])]


ex21 = createPartitions s1 [(1,[1,2]),(2,[3,4,5,6,7])]

--ex22 = map (partitionCatAndLinks s1g) (map snd ex21)

ex23 = createPartitions s1 [(1,[1,2]),(2,[3,4]),(3,[5,6,7])]

--ex24 = map (partitionCatAndLinks s1g) (map snd ex23)

ex25 = graphPartitionCategory s1 p1
ex26 = graphPartitionCategory s1 p2
ex27 = graphPartitionCategory s1 p3
ex28 = graphPartitionCategory s1 s1

ex30 = putStrLn $ showStreamGraph ["FunctionalIoTtypes","FunctionalProcessing"] s1
ex31 = putStrLn $ showStreamGraph ["FunctionalIoTtypes","FunctionalProcessing"] p1
ex32 = putStrLn $ showStreamGraph ["FunctionalIoTtypes","FunctionalProcessing"] p2
ex33 = putStrLn $ showStreamGraph ["FunctionalIoTtypes","FunctionalProcessing"] p3

------------example2
s3:: StreamGraph
s3  = StreamGraph "test s3" 2 [] [
       StreamOperation 1 [ ]   Source ["sourceFn"] "Stream Int" ["SourceGenerator"],
       StreamOperation 2 [1]   Map    ["\\t-> t"]  "Stream Int" []]

s31 = putStrLn $ showStreamGraph ["FunctionalIoTtypes","FunctionalProcessing"] $ snd $ (createPartitions s3 [(1,[1]),(2,[2])])!!1

generatePartitionCode:: StreamGraph -> [String] -> Partition -> StreamGraph -> String -- FullStreamGraph -> StandardImports -> Partition Stream Graph -> Standard Imports
generatePartitionCode fullsg stdImports pid partition = case graphPartitionCategory fullsg partition of
--                                                           SinkPartition     -> (showStreamGraph stdImports partition) ++ "\n" ++ "main :: IO () \n" ++ "main = nodeSink "     ++ gid partition ++ "\n"
                                                           SinkPartition     -> showSinkGraph stdImports fullsg pid partition "nodeSink"
                                                           Sink2Partition    -> showSinkGraph stdImports fullsg pid partition "nodeSink2"
--                                                           SourcePartition   -> (showStreamGraph stdImports partition) ++ "\n" ++ "main :: IO () \n" ++ "main = nodeSource "   ++ gid partition ++ "\n"
                                                           SourcePartition   -> showSourceGraph stdImports pid partition                                                     
                                                           LinkPartition     -> (showStreamGraph stdImports partition) ++ "\n" ++ "main :: IO () \n" ++ "main = nodeLink "     ++ gid partition ++ "\n"
                                                           Link2Partition    -> (showStreamGraph stdImports partition) ++ "\n" ++ "main :: IO () \n" ++ "main = nodeLink2 "    ++ gid partition ++ "\n"
                                                           SingularPartition -> (showStreamGraph stdImports partition) ++ "\n" ++ "main :: IO () \n" ++ "main = nodeSingular " ++ gid partition ++ "\n"

showSourceGraph:: [String] -> Partition -> StreamGraph -> String
showSourceGraph stdImports pid partition = let [srcOp] = filter (\op -> operator op == Source) (operations partition) in
                                           let srcOpId = opid srcOp in
                                           if (length (operations partition))>1
                                           then
                                              let newPart = snd $ head $ createPartitions partition [(pid,(map opid (operations partition)) \\ [srcOpId])] in
                                                 (showStreamGraph stdImports newPart) ++ "\n" ++ "main :: IO () \n" ++ "main = nodeSource " ++ printParams (parameters srcOp) ++ " " ++ gid newPart ++ "\n"
                                           else
                                              "main :: IO () \n" ++ "main = nodeSource " ++ printParams (parameters srcOp) ++ " " ++ "(streamFilter True)" ++ "\n"
                                                 
showSinkGraph:: [String] -> StreamGraph -> Partition -> StreamGraph -> String -> String
showSinkGraph stdImports fullsg pid partition nodeName = let [sinkOp] = filter (\op -> operator op == Sink) (operations partition) in
                                                         let sinkOpId = opid sinkOp in
                                                         if (length (operations partition))>1 -- just the sink node in this partition
                                                         then
                                                            let newPart = snd $ head $ createPartitions fullsg [(pid,(map opid (operations partition)) \\ [sinkOpId])] in 
                                                               (showStreamGraph stdImports newPart) ++ "\n" ++ "main :: IO () \n" ++ "main = " ++ nodeName ++ " " ++ gid newPart ++ " " ++ printParams (parameters sinkOp) ++ " " ++ "\n"
                                                         else
                                                            "main :: IO () \n" ++ "main = " ++ nodeName ++ " " ++ "(streamFilter True)" ++ " " ++ printParams (parameters sinkOp) ++ " " ++ "\n"
                                                         
generateCode:: StreamGraph -> [(Partition,[Id])] -> [String] -> [String]
generateCode sg ps stdImports = map (\(pid,part)->generatePartitionCode sg stdImports pid part) $ createPartitions sg ps

ex41 = generateCode s1 [(1,[1,2]),(2,[3,4]),(3,[5,6,7])] ["FunctionalIoTtypes","FunctionalProcessing","Nodes"]
ex42 = putStrLn $ ex41!!0
ex43 = putStrLn $ ex41!!1
ex44 = putStrLn $ ex41!!2
ex45 = createPartitions s1 [(1,[1,2]),(2,[3,4]),(3,[5,6,7])]

ex411 = generateCode s1 [(1,[1..5]),(2,[6,7])] ["FunctionalIoTtypes","FunctionalProcessing","Nodes"]
ex412 = putStrLn $ ex411!!0
ex413 = putStrLn $ ex411!!1
ex414 = createPartitions s1 [(1,[1..5]),(2,[6,7])]

ex416 = generateCode s1 [(1,[1]),(2,[2,3,4,5,6]),(3,[7])] ["FunctionalIoTtypes","FunctionalProcessing","Nodes"]


ex415p2:: StreamGraph
ex415p2 = StreamGraph "p2" 7 [(6,"String")] [StreamOperation 7 [6] Sink ["print"] "" [""]]
ex415 = graphPartitionCategory s1 ex415p2

{-
data PartitionGraph = PartitionGraph
   { pid         :: Partition
   , pCategory   :: PartitionType
   , pInputTypes :: [String]
   , streamGraph :: StreamGraph}
      deriving (Show)
-}

s4:: StreamGraph
s4 = StreamGraph "pipeline" 5 [] [
       StreamOperation 1 [ ] Source    ["src1"]                                                                                "Stream String"          ["SourceFileContainingsrc1.hs"],
       StreamOperation 2 [1] Map       ["\\st-> st++st"]                                                                       "Stream String"          [],
       StreamOperation 3 [2] Map       ["\\st-> reverse st"]                                                                   "Stream String"          [],
       StreamOperation 4 [3] Map       ["\\st-> \"Incoming Message at Server: \" ++ st"]                                       "Stream String"          [],
       StreamOperation 5 [4] Window    ["(chop 2)"]                                                                            "Stream [String]"        []]

s4parts = generateCode s4 [(1,[1,2]),(2,[3]),(3,[4,5])] ["FunctionalIoTtypes","FunctionalProcessing","Nodes"] 
exs41 = putStrLn $ s4parts!!0
exs42 = putStrLn $ s4parts!!1
exs43 = putStrLn $ s4parts!!2

s5:: StreamGraph
s5 = StreamGraph "joint-test" 4 [] [
       StreamOperation 1 [ ]   Source    ["src1"]                                                                                "Stream String"          ["SourceFileContainingsrc1.hs"],
       StreamOperation 2 [ ]   Source    ["src2"]                                                                                "Stream String"          ["SourceFileContainingsrc2.hs"],
       StreamOperation 3 [1,2] Join      []                                                                                      "Stream (String,String)" [],
       StreamOperation 4 [3]   Map       ["\\(l,r)-> \"Incoming Message at Server: \" ++ l++\":\"++r"]                           "Stream String"          []]


s5parts1 = generateCode s5 [(1,[1]),(2,[2]),(3,[3,4])] ["FunctionalIoTtypes","FunctionalProcessing","Nodes"] 
exs51 = putStrLn $ s5parts1!!0
exs52 = putStrLn $ s5parts1!!1
exs53 = putStrLn $ s5parts1!!2

s5parts2 = generateCode s5 [(1,[1]),(2,[2]),(3,[3]),(4,[4])] ["FunctionalIoTtypes","FunctionalProcessing","Nodes"] 
exs521 = putStrLn $ s5parts2!!0
exs522 = putStrLn $ s5parts2!!1
exs523 = putStrLn $ s5parts2!!2
exs524 = putStrLn $ s5parts2!!3

ex601 = createPartitionsAndEdges s1 [(1,[1,2]),(2,[3,4]),(3,[5,6,7])]

-----------------------------

replaceMergeNodes:: ([(Partition,StreamGraph)],[((Partition,Int),(Partition,Int))]) -> ([(Partition,StreamGraph)],[((Partition,Int),(Partition,Int))]) 
replaceMergeNodes (parts,links) = let newParts = map (\(p,sg)->let (isMerge,mopid) = mergeAtInput sg in 
                                                                   if isMerge
                                                                   then (p,replaceMerge mopid sg) 
                                                                   else (p,sg)) parts in 
                                   let inputMergePartIds = map fst $ filter (\(_,(isMergeInp,_))->isMergeInp) $ map (\(p,sg)-> (p,mergeAtInput sg)) parts in     -- needs to get partitionid from mergeatinput... not operation id                                                            
                                       (newParts,map (updateMergeLink inputMergePartIds) links)

mergeAtInput:: StreamGraph -> (Bool,Int)
mergeAtInput sg = let mergeOps    = filter (\op -> operator op == Merge) $ operations sg in  -- find all merge operators
                  let allopIds    = map opid $ operations sg in                              -- get the ids of all operators
                  let inputMerges = filter (\mop->(intersect allopIds (opinputs mop))==[]) mergeOps in -- find merge operators with no inputs in this graph
                      if   length inputMerges == 0 
                      then (False,0)                      -- no input merges
                      else (True,opid (head inputMerges)) -- return the id of the input merge operator                    
                                       
updateMergeLink:: [Int] -> ((Partition,Int),(Partition,Int)) -> ((Partition,Int),(Partition,Int))
updateMergeLink mergeIds ((partSrc,portSrc),(partDest,portDest)) | elem partDest mergeIds = ((partSrc,portSrc),(partDest,1))
                                                                 | otherwise              = ((partSrc,portSrc),(partDest,portDest))

replaceMerge:: Int -> StreamGraph -> StreamGraph
replaceMerge opId sg = StreamGraph (gid sg) (resultId sg) (ginputs sg) (map (\op->if   opid op == opId 
                                                                                  then StreamOperation opId (opinputs op) Filter ["True"] (outputType op) (imports op) -- use Filter (True) as identity function to replace merge
                                                                                  else op)
                                                                            (operations sg))

s6:: StreamGraph
s6 = StreamGraph "merge-test" 6 [] [
       StreamOperation 1 [ ]   Source    ["src1"]                                                                                "Stream String"          ["SourceFileContainingsrc1.hs"],
       StreamOperation 2 [ ]   Source    ["src2"]                                                                                "Stream String"          ["SourceFileContainingsrc2.hs"],
       StreamOperation 3 [2]   Filter    ["(\\t->length t>5)"]                                                                    "Stream String"          ["SourceFileContainingsrc2.hs"], 
       StreamOperation 4 [1,3] Merge     []                                                                                      "Stream String"          [],
       StreamOperation 5 [4]   Map       ["\\(l,r)-> \"Incoming Message at Server: \" ++ l++\":\"++r"]                           "Stream String"          [],
       StreamOperation 6 [5]   Sink      ["print"]                                                                               ""                       []]

exs6  = generateCode s6 [(1,[1]),(2,[2,3]),(3,[4..6])] ["FunctionalIoTtypes","FunctionalProcessing","Nodes"] 
exs61 = putStrLn $ exs6!!0
exs62 = putStrLn $ exs6!!1
exs63 = putStrLn $ exs6!!2

exs64 = createPartitionsAndEdges s6 [(1,[1]),(2,[2,3]),(3,[4..6])]
exs65 = replaceMergeNodes exs64
exs66 = map (updateMergeLink [3]) [((1,1),(3,1)),((2,1),(3,2))]
