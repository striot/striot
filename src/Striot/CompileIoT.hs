{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Striot.CompileIoT ( createPartitions
                         , generateCode
                         , GenerateOpts(..)
                         , defaultOpts
                         , PartitionMap
                         , writePart
                         , genDockerfile
                         , partitionGraph
                         , simpleStream

                         , htf_thisModulesTests
                         ) where

import Data.List (intercalate)
import Algebra.Graph
import Test.Framework
import System.FilePath ((</>))
import System.Directory (createDirectoryIfMissing)
import Data.Function ((&))

import Striot.StreamGraph
import Striot.LogicalOptimiser

------------------------------------------------------------------------------
-- StreamGraph Partitioning

-- |The user's desired partitioning of the input Graph.
-- Each element in the outer-most list corresponds to a distinct partition.
-- The inner-lists are the IDs of Operators to include in that partition.
type PartitionMap = [[Int]]

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

-- | Builds a function to remove Merges from a StreamGraph, if they are
-- connected to from operators in another (local) graph. We remove Merges
-- from the beginning of Partitions and use the TCP/IP machinery to merge
-- multiple incoming streams instead.
mkStripMerge :: StreamGraph -> StreamGraph -> (StreamGraph -> StreamGraph)
mkStripMerge local global = let
    -- find Merges in global Graph connected to from local Graph
    merges = map snd
           $ filter (\(f,t)-> f `elem` vertexList local && operator t == Merge)
           $ edgeList global

    remove m = let nextOp = snd . head . filter ((==m).fst) . edgeList $ global
        in removeEdge nextOp nextOp . replaceVertex m nextOp

    in \g -> foldl (&) g (map remove merges)

global = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Merge [] "Int" "Int"
    , StreamVertex 2 Map ["show","s"] "Int" "String"
    , StreamVertex 3 Sink ["mapM_ print"] "String" "String"
    ]
local = Vertex $ StreamVertex 0 Source [] "Int" "Int"

stripMergePost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 2 Map ["show","s"] "Int" "String"
    , StreamVertex 3 Sink ["mapM_ print"] "String" "String"
    ]

test_stripMerge1 = assertEqual stripMergePost $
    mkStripMerge local global $ global

test_stripMerge2 = assertEqual stripMergePost $
    mkStripMerge local stripMergePost $ stripMergePost

unPartition :: ([Graph StreamVertex], Graph StreamVertex) -> Graph StreamVertex
unPartition (a,b) = overlay b $ foldl overlay Empty a

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

data GenerateOpts = GenerateOpts
    { imports   :: [String]     -- list of import statements to add to generated files
    , packages  :: [String]     -- list of Cabal packages to install within containers
    , preSource :: Maybe String -- code to run prior to starting nodeSource
    , rewrite   :: Bool         -- should each partition be logically optimised?
    }

defaultOpts = GenerateOpts
    { imports   = ["Striot.FunctionalIoTtypes", "Striot.FunctionalProcessing", "Striot.Nodes", "Control.Concurrent"]
    , packages  = []
    , preSource = Nothing
    , rewrite   = True
    }

-- |Partitions the supplied `StreamGraph` according to the supplied `PartitionMap`
-- and options specified within the supplied `GenerateOpts` and returns a list of
-- the sub-graphs converted into source code and encoded as `String`s.
generateCode :: StreamGraph -> PartitionMap -> GenerateOpts -> [String]
generateCode sg pm opts = generateCode' (createPartitions sg pm) opts

generateCode' :: ([StreamGraph], StreamGraph) -> GenerateOpts -> [String]
generateCode' (sgs,cuts) opts = let
                  sgs' = if   rewrite opts
                         then map optimise sgs
                         else sgs
                  enumeratedParts = zip [1..] sgs'
                  in map (generateCodeFromStreamGraph opts enumeratedParts cuts) enumeratedParts

data NodeType = NodeSource | NodeSink | NodeLink deriving (Show)

nodeType :: StreamGraph -> NodeType
nodeType sg = if operator (head (vertexList sg)) == Source
              then NodeSource
              else if (operator.head.reverse.vertexList) sg == Sink
                   then NodeSink
                   else NodeLink

-- vertexList outputs *sorted*. That corresponds to the Id value for
-- our StreamVertex type
generateCodeFromStreamGraph :: GenerateOpts -> [(Integer, StreamGraph)] -> StreamGraph -> (Integer,StreamGraph) -> String
generateCodeFromStreamGraph opts parts cuts (partId,sg) = intercalate "\n" $
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
        imports' = (map ("import "++) (imports opts)) ++ ["\n"]
        lastIdentifier = 'n':(show $ (length intVerts) + valence)
        intVerts= filter (\x-> not $ operator x `elem` [Source,Sink]) $ vertexList sg
        valence = partValence sg cuts
        nodeFn sg = case (nodeType sg) of
            NodeSource -> generateNodeSrc partId (connectNodeId sg parts cuts) opts
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
generateNodeLink n = "main = nodeLink (defaultLink \"9001\" \"node"++(show n)++"\" \"9001\") streamGraphFn"

-- warts:
--  we accept a list of onward nodes but nodeSource only accepts one anyway
generateNodeSrc :: Integer -> [Integer] -> GenerateOpts -> String
generateNodeSrc partId nodes opts = let
    node = head nodes
    host = "node" ++ (show node)
    port = 9001 + partId -1 -- XXX Unlikely to always be correct
    pref = case preSource opts of
       Nothing -> ""
       Just f  -> f

    in "main = do\n\
\       "++pref++"\n\
\       nodeSource (defaultSource \""++host++"\" \""++(show port)++"\") src1 streamGraphFn"

generateNodeSink :: Int -> String
generateNodeSink v = case v of
    1 -> "main = nodeSink (defaultSink \"9001\") streamGraphFn sink1"
    2 -> "main = nodeSink2 streamGraphFn sink1 \"9001\" \"9002\""
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
        "n" ++ show opid ++ " = (\\" ++ lparams ++ " -> " ++ printOp op ++ " " ++ params ++ ") " ++ args

printOp :: StreamOperator -> String
printOp = (++) "stream" . show

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

genDockerfile listen opts = 
    let pkgs = packages opts in concat
    [ "FROM striot/striot-base:latest\n"
    , "WORKDIR /opt/node\n"
    , "COPY . /opt/node\n"
    , if pkgs /= [] then "RUN cabal install " ++ (intercalate " " pkgs) else ""
    , "\n"
    , "RUN ghc node.hs\n"
    , if listen then "EXPOSE 9001\n" else ""
    , "CMD /opt/node/node\n"
    ]

-- XXX rename
writePart :: GenerateOpts -> (Int, String) -> IO ()
writePart opts (x,y) = let
    dockerfile = genDockerfile True opts
    bn = "node" ++ (show x)
    fn = bn </> "node.hs"
    in do
        createDirectoryIfMissing True bn
        writeFile (bn </> "Dockerfile") dockerfile
        writeFile fn y

-- |Partitions the supplied `StreamGraph` according to the supplied `PartitionMap`;
-- invokes `generateCode` for each derived sub-graph; writes out the resulting
-- source code to individual source code files, one per node.
partitionGraph :: StreamGraph -> PartitionMap -> GenerateOpts -> IO ()
partitionGraph graph partitions opts =
    mapM_ (writePart opts) $ zip [1..] $ generateCode graph partitions opts

-- |Convenience function for specifying a simple path-style of stream processing
-- program, with no merge or join operations. The list of tuples are converted
-- into a series of connected Stream Vertices in a Graph. The tuple arguments are
-- the relevant `StreamOperator` for the node; the parameters and the *output*
-- type. The other parameters to `StreamVertex` are inferred from the neighbouring
-- tuples. Unique and ascending `vertexId` values are assigned.
simpleStream :: [(StreamOperator, [String], String)] -> Graph StreamVertex
simpleStream tupes = path lst

    where
        intypes = "IO ()" : (map (\(_,_,ty) -> ty) (init tupes))
        tupes3 = zip3 [1..] intypes tupes
        lst = map (\ (i,intype,(op,params,outtype)) ->
            StreamVertex i op params intype outtype) tupes3
