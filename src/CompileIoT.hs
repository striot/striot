module CompileIoT where
import FunctionalIoTtypes
import FunctionalProcessing
import Data.List
import System.IO

type Id     = Int 
type Port   = Int
type PortId = (Id,Port)
type Param  = String  
data StreamOperator  = Map             |
                       Filter          |
                       Expand          |
                       Window          |
--                       WindowAggregate |
                       Merge           |
                       Join            |
--                       JoinE           |
--                       JoinW           |
                       Scan            |
                       FilterAcc       |
                       Source          |
                       Sink
                       deriving (Eq)
                       
data StreamGraph = StreamGraph
   { gid        :: String
   , resultId   :: Id
   , ginputs    :: [(Id,String)]
   , operations :: [StreamOperation]}
      deriving (Show)

data StreamOperation  = StreamOperation
   { opid       :: Int
   , opinputs   :: [Int]
   , operator   :: StreamOperator
   , parameters :: [String]
   , outputType :: String
   , imports    :: [String]}
     deriving (Show)     
 
instance Show StreamOperator where
    show Map             = "streamMap"
    show Filter          = "streamFilter"
    show Window          = "streamWindow"
--    show WindowAggregate = "streamWindowAggregate"
    show Merge           = "streamMerge"
    show Join            = "streamJoin"
--    show JoinE           = "streamJoinE"
--    show JoinW           = "streamJoinW"
    show Scan            = "streamScan"
    show FilterAcc       = "streamFilterAcc"
    show Expand          = "streamExpand"
    show Source          = "streamSource"
    show Sink            = "streamSink"
    
type OutputType  = String
type PartitionId = String
type GraphId     = String

showStreamGraph :: StreamGraph -> String
showStreamGraph sg = (printImports $ ["FunctionalIoTtypes","FunctionalProcessing"]++(nub (concatMap imports $ operations sg))) ++ "\n" ++
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
showOperation sop = "let n" ++ show (opid sop) ++ " = " ++ show (operator sop) ++ " " ++ (intercalate " " (map addBrackets (parameters sop))) ++ " " ++ printArgs (opinputs sop) ++ " in\n"

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

data PartitionType = SourcePartition | LinkPartition | SinkPartition | SingularPartition
    deriving (Show)
    
graphPartitionCategory:: StreamGraph -> StreamGraph -> PartitionType -- full graph -> a partition
graphPartitionCategory s p   | null (graphInLinks  p)   && null (graphOutLinks s p) = SingularPartition
                             | null (graphInLinks  p)                               = SourcePartition
                             | null (graphOutLinks s p)                             = SinkPartition
                             | otherwise                                            = LinkPartition

partitionCategory:: Sgraph -> Sgraph -> PartitionType -- the full graph -> this partition
partitionCategory s p   | null (inLinks  s p) && null (outLinks s p) = SingularPartition
                        | null (inLinks  s p)                        = SourcePartition
                        | null (outLinks s p)                        = SinkPartition
                        | otherwise                                  = LinkPartition

partitionCatAndLinks:: Sgraph -> Sgraph -> (PartitionType,[Id],[PortId])
partitionCatAndLinks g p = (partitionCategory g p,inLinks  g p,outLinks g p)

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
                                        
-------------Examples----------------------                                                
s1:: StreamGraph
s1  = StreamGraph "test" 6 [] [
       StreamOperation 1 [ ] Source    []                                                                                      "Stream Trip"            ["Taxi.hs"],
       StreamOperation 2 [1] Map       ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"]                  "Stream Journey"         ["Taxi.hs"],
       StreamOperation 3 [2] Filter    ["\\j-> inRangeQ1 (start j) && inRangeQ1 (end j)"]                                      "Stream Journey"         ["Taxi.hs"],
       StreamOperation 4 [3] Window    ["slidingTime 1800"]                                                                    "Stream [Journey]"       ["Taxi.hs"],
       StreamOperation 5 [4] Map       ["mostFrequent 10"]                                                                     "Stream [(Journey,Int)]" ["Taxi.hs"],
       StreamOperation 6 [5] FilterAcc ["value $ head s","\\h acc-> if (h==acc) then acc else h","\\h acc->(h/=acc)","tail s"] "Stream [(Journey,Int)]" ["Taxi.hs"]]

p1:: StreamGraph
p1  = StreamGraph "partition1" 6 [(2,"Stream Journey")] [
       StreamOperation 3 [2] Filter    ["\\j-> inRangeQ1 (start j) && inRangeQ1 (end j)"]                                      "Stream Journey"         ["Taxi.hs"],
       StreamOperation 4 [3] Window    ["slidingTime 1800"]                                                                    "Stream [Journey]"       ["Taxi.hs"],
       StreamOperation 5 [4] Map       ["mostFrequent 10"]                                                                     "Stream [(Journey,Int)]" ["Taxi.hs"],
       StreamOperation 6 [5] FilterAcc ["value $ head s","\\h acc-> if (h==acc) then acc else h","\\h acc->(h/=acc)","tail s"] "Stream [(Journey,Int)]" ["Taxi.hs"]]

p2:: StreamGraph
p2  = StreamGraph "test" 2 [] [
       StreamOperation 1 [ ] Source         []                                                                                       "Stream Trip" ["Taxi.hs"],
       StreamOperation 2 [1] Map            ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"]                   "Stream Journey" ["Taxi.hs"]]
       
p3:: StreamGraph
p3  = StreamGraph "test" 2 [(1,"Stream Trip")] [
       StreamOperation 2 [1] Map            ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"]                   "Stream Journey" ["Taxi.hs"]]
       
s2:: StreamGraph
s2  = StreamGraph "wikiJacksonEx" 3 [] [
       StreamOperation 0 [ ]   Source []          "Stream Int" [],
       StreamOperation 1 [0]   Map    ["\\t-> t"] "Stream Int" [],
       StreamOperation 2 [0]   Map    ["\\t-> t"] "Stream Int" [],
       StreamOperation 3 [0,1] Map    ["\\t-> t"] "Stream Int" []]

       
{-       
parMap:: StreamGraph
parMap = StreamGraph "parallelMap" 4 [] [
         StreamOperation 1 [ ]   Split ["roundRobin"]                                                         "Stream alpha" [],
         StreamOperation 2 [1]   Map   ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"] "Stream alpha" ["Taxi.hs"],
         StreamOperation 3 [1]   Map   ["\\t-> Journey{start=toCellQ1 (pickup t), end=toCellQ1 (dropoff t)}"] "Stream alpha" ["Taxi.hs"], 
         StreamOperation 4 [2,3] Merge []                                                                     "Stream alpha" []]
       
ex1 = putStrLn $ showStreamGraph s1
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

ex21 = createPartitions s1 [(1,[1,2]),(2,[3,4,5,6])]

--ex22 = map (partitionCatAndLinks s1g) (map snd ex21)

ex23 = createPartitions s1 [(1,[1,2]),(2,[3,4]),(3,[5,6])]

--ex24 = map (partitionCatAndLinks s1g) (map snd ex23)

ex25 = graphPartitionCategory s1 p1
ex26 = graphPartitionCategory s1 p2
ex27 = graphPartitionCategory s1 p3
ex28 = graphPartitionCategory s1 s1

ex30 = putStrLn $ showStreamGraph s1
ex31 = putStrLn $ showStreamGraph p1
ex32 = putStrLn $ showStreamGraph p2
ex33 = putStrLn $ showStreamGraph p3
