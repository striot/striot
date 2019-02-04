{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Striot.CompileIoT ( StreamGraph(..)
                         , StreamVertex(..)
                         , StreamOperator(..)
                         , createPartitions
                         , generateCode
                         , PartitionMap
                         , htf_thisModulesTests
                         ) where

import Data.List (intercalate)
import Algebra.Graph
import Test.Framework

data StreamOperator = Map
                    | Filter
                    | Expand
                    | Window
                    | Merge
                    | Join
                    | Scan
                    | FilterAcc
                    | Source
                    | Sink
                    deriving (Ord,Eq)

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

-- Id needed for uniquely identifying a vertex. (Is there a nicer way?)
data StreamVertex = StreamVertex
    { vertexId   :: Int
    , operator   :: StreamOperator
    , parameters :: [String] -- XXX strings of code. From CompileIoT. Variable length e.g.FilterAcc takes 3 (?)
    , intype     :: String
    , outtype    :: String
    } deriving (Eq,Show)

instance Ord StreamVertex where
    compare (StreamVertex x _ _ _ _) (StreamVertex y _ _ _ _) = compare x y

------------------------------------------------------------------------------
-- StreamGraph Partitioning

type PartitionMap = [[Int]]
-- outer-list index: partition ID
-- each inner-list is a list of Vertex IDs to include in that partition

-- createPartitions returns ([partition map], [inter-graph links])
-- where inter-graph links are the cut edges due to partitioning
createPartitions :: Graph StreamVertex -> PartitionMap -> ([Graph StreamVertex], Graph StreamVertex)
createPartitions _ [] = ([],empty)
createPartitions g (p:ps) = ((overlay vs es):tailParts, cutEdges `overlay` tailCuts) where
    vs        = vertices $ filter fv (vertexList g)
    es        = edges $ filter (\(v1,v2) -> (fv v1) && (fv v2)) (edgeList g)
    cutEdges  = edgesOut
    fv v      = (vertexId v) `elem` p
    edgesOut  = edges $ filter (\(v1,v2) -> (fv v1) && (not(fv v2))) (edgeList g)
    (tailParts, tailCuts) = createPartitions g ps

unPartition :: ([Graph StreamVertex], Graph StreamVertex) -> Graph StreamVertex
unPartition (a,b) = overlay b $ foldl overlay Empty a

type StreamGraph = Graph StreamVertex

------------------------------------------------------------------------------
-- Code generation from StreamGraph definitions

{-
    a well-formed streamgraph:
        always starts with a Source?
        always has just one Source?
        always ends with a Sink?
        always has just one Sink?
        is entirely connected?
        ...
    a well-formed partition spec:
        has ≥ 1 partition
        references node IDs that exist
        covers all node IDs?
        passes some kind of connectedness test?
-}

generateCode :: StreamGraph -> PartitionMap -> [String] -> [String]
generateCode sg pm imports = generateCode' (createPartitions sg pm) imports

generateCode' :: ([StreamGraph], StreamGraph) -> [String] -> [String]
generateCode' (sgs,cuts) imports = let
                  enumeratedParts = zip [1..] sgs
                  in map (generateCodeFromStreamGraph imports enumeratedParts cuts) enumeratedParts

data NodeType = NodeSource | NodeSink | NodeLink deriving (Show)

nodeType :: StreamGraph -> NodeType
nodeType sg = if operator (head (vertexList sg)) == Source
              then NodeSource
              else if (operator.head.reverse.vertexList) sg == Sink
                   then NodeSink
                   else NodeLink

-- vertexList outputs *sorted*. That corresponds to the Id value for
-- our StreamVertex type
generateCodeFromStreamGraph :: [String] -> [(Integer, StreamGraph)] -> StreamGraph -> (Integer,StreamGraph) -> String
generateCodeFromStreamGraph imports parts cuts (partId,sg) = intercalate "\n" $
    nodeId : -- convenience comment labelling the node/partition ID
    imports' ++
    (possibleSrcSinkFn sg) :
    sgTypeSignature :
    sgIntro :
    (map ((padding++).generateCodeFromVertex) (zip [(valence+1)..] intVerts)) ++
    [padding ++ "in " ++ lastIdentifier,"\n",
    "main :: IO ()",
    nodeFn sg] where
        nodeId = "-- node"++(show partId)
        padding = "    "
        sgTypeSignature = "streamGraphFn ::"++(concat $ take valence $ repeat $ " Stream "++(inType sg)++" ->")++" Stream "++(outType sg)
        sgIntro = "streamGraphFn "++sgArgs++" = let"
        sgArgs = unwords $ map (('n':).show) [1..valence]
        imports' = (map ("import "++) imports) ++ ["\n"]
        lastIdentifier = 'n':(show $ (length intVerts) + valence)
        intVerts= filter (\x-> not $ operator x `elem` [Source,Sink]) $ vertexList sg
        valence = partValence sg cuts
        nodeFn sg = case (nodeType sg) of
            NodeSource -> generateNodeSrc partId (connectNodeId sg parts cuts)
            NodeLink   -> generateNodeLink (partId + 1)
            NodeSink   -> generateNodeSink valence
        possibleSrcSinkFn sg = case (nodeType sg) of
            NodeSource -> generateSrcFn sg
            NodeLink   -> ""
            NodeSink   -> generateSinkFn sg

-- output type of a StreamGraph.
-- special-case if the terminal node is a Sink node: we want the
-- "pure" StreamGraph type that feeds into the sink function.
outType :: StreamGraph -> String
outType sg = let node = (head . reverse . vertexList) sg
        in if operator node == Sink
           then intype node
           else outtype node

-- input type of a StreamGraph
-- see outType for rationale
inType :: StreamGraph -> String
inType sg = let node = (head  . vertexList) sg
            in if operator node == Source
               then outtype node
               else intype node

t = path [ StreamVertex 0 Source ["return 0"]       "IO Int" "Int"
         , StreamVertex 1 Map    ["show","s"]       "Int" "String"
         , StreamVertex 2 Sink   ["mapM_ putStrLn"] "String" "IO ()"
         ]

test_outType = assertEqual "String" $
    (outType . head . fst) (createPartitions t [[0,1],[2]])

test_outType_sink = assertEqual "String" $
    (outType . head . fst) (createPartitions t [[0,1,2]])

test_inType = assertEqual "Int" $
    (inType . head . fst) (createPartitions t [[0,1],[2]])

-- determine the node(s?) to connect on to from this partition
-- XXX always 0 or 1? write quickcheck property...
connectNodeId :: StreamGraph -> [(Integer, StreamGraph)] -> StreamGraph -> [Integer]
connectNodeId sg parts cuts = let
    edges = edgeList cuts
    outs  = vertexList sg
    outEs = filter (\(f,t) -> f `elem` outs) edges
    destVs= map snd outEs
    destGs= concatMap (\v -> filter (\(n,sg) -> v `elem` (vertexList sg)) parts) destVs

    in map fst destGs

generateSrcFn :: StreamGraph -> String
generateSrcFn sg = "src1 = " ++
    (intercalate "\n" $ parameters $ head $ vertexList sg) ++ "\n"

generateSinkFn:: StreamGraph -> String
generateSinkFn sg = "sink1 :: Show a => Stream a -> IO ()\nsink1 = " ++
    (intercalate "\n" $ parameters $ head $ reverse $ vertexList sg) ++ "\n"

generateNodeLink :: Integer -> String
generateNodeLink n = "main = nodeLink streamGraphFn 9001 \"node"++(show n)++"\" 9001"

-- warts:
--  we accept a list of onward nodes but nodeSource only accepts one anyway
generateNodeSrc :: Integer -> [Integer] -> String
generateNodeSrc partId nodes = let
    node = head nodes
    host = "node" ++ (show node)
    port = 9001 + partId -1 -- XXX Unlikely to always be correct
    in "main = nodeSource src1 streamGraphFn \""++host++"\" "++(show port)

generateNodeSink :: Int -> String
generateNodeSink v = case v of
    1 -> "main = nodeSink streamGraphFn sink1 9001"
    2 -> "main = nodeSink2 streamGraphFn sink1 9001 9002"
    v -> error "generateNodeSink: unhandled valence " ++ (show v)

-- generateCodeFromVertex:  generates Haskell code to be included in a
-- let expression, corresponding to the supplied StreamVertex. The Int
-- argument represents the sequence order of the StreamVertex relative
-- to others, and is used to calculate the names of the input stream
-- argument(s).
-- As the StreamVertex parameters may need to reference the input streams,
-- the generated expression is wrapped in a lambda expression that names
-- them 's' for unary input streams and 's1, s2' for binary input streams
-- (Join and, for the time being, Merge)
-- XXX: streamMerge is broken. We have no way of knowing how many input
-- streams there are supposed to be, so we guess at 2.
generateCodeFromVertex :: (Int, StreamVertex) -> String
generateCodeFromVertex (opid, v)  = let
    op      = operator v
    lparams = if op `elem` [Join,Merge] then "s1 s2" else "s"
    params  = intercalate " " (parameters v)
    args    = concat $ if op `elem` [Join,Merge]
        then ["n", show (opid-2), " n", show (opid-1)]
        else ["n", show (opid-1)]
    in
        "n" ++ show opid ++ " = (\\" ++ lparams ++ " -> " ++ show op ++ " " ++ params ++ ") " ++ args

-- how many incoming edges to this partition?
-- + how many source nodes
partValence :: StreamGraph -> StreamGraph -> Int
partValence g cuts = let
    verts = vertexList g
    inEdges = filter (\e -> (snd e) `elem` verts) (edgeList cuts)
    sourceNodes = filter (\v -> Source == operator v) (vertexList g)
    in
        (length sourceNodes) + (length inEdges)

------------------------------------------------------------------------------
-- tests / test data

-- Source -> Sink
s0 = connect (Vertex (StreamVertex 0 (Source) [] "String" "String"))
    (Vertex (StreamVertex 1 (Sink) [] "String" "String"))

-- Source -> Filter -> Sink
s1 = path [ StreamVertex 0 (Source) [] "String" "String"
          , StreamVertex 1 Filter [] "String" "String"
          , StreamVertex 2 (Sink) [] "String" "String"
          ]

test_reform_s0 = assertEqual s0 (unPartition $ createPartitions s0 [[0],[1]])
test_reform_s1 = assertEqual s1 (unPartition $ createPartitions s1 [[0,1],[2]])
test_reform_s1_2 = assertEqual s1 (unPartition $ createPartitions s1 [[0],[1,2]])

------------------------------------------------------------------------------
-- quickcheck experiment

instance Arbitrary StreamOperator where
    arbitrary = elements [ Map , Filter , Expand , Window , Merge , Join , Scan
                         , FilterAcc , Source , Sink ]

instance Arbitrary StreamVertex where
    arbitrary = do
        vertexId <- arbitrary
        operator <- arbitrary
        let parameters = []
            intype = "String"
            outtype = "String"
            in
                return $ StreamVertex vertexId operator parameters intype outtype

streamgraph :: Gen StreamGraph
streamgraph = sized streamgraph'
streamgraph' 0 = return g where g = empty :: StreamGraph
streamgraph' n | n>0 = do
    v <- arbitrary
    t <- streamgraph' (n-1)
    return $ connect (vertex v) t
