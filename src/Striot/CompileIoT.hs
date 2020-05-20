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
                         , optimiseWriteOutAll

                         , optimise
                         , generateCodeFromStreamGraph
                         , nodeFn
                         , nodeType
                         , generateNodeSrc
                         , connectNodeId

                         , htf_thisModulesTests
                         ) where

import Data.List (intercalate, nub)
import Algebra.Graph
import Algebra.Graph.ToGraph (reachable)
import Test.Framework
import System.FilePath ((</>))
import System.Directory (createDirectoryIfMissing)
import Data.Function ((&))
import Data.List.Match (compareLength)

import Striot.StreamGraph
import Striot.LogicalOptimiser

------------------------------------------------------------------------------
-- StreamGraph Partitioning

-- |eventually, this might include properties about the partition (Catalogue).
-- For now we just need a way of enumerating them.
type Partition = Int

-- |The user's desired partitioning of the input Graph.
-- Each element in the outer-most list corresponds to a distinct partition.
-- The inner-lists are the IDs of Operators to include in that partition.
type PartitionMap = [[Int]]

-- createPartitions returns ([partition map], [inter-graph links])
-- where inter-graph links are the cut edges due to partitioning
createPartitions :: StreamGraph -> PartitionMap -> PartitionedGraph
createPartitions _ [] = ([],empty)
createPartitions g (p:ps) = (thisGraph:tailParts, edgesOut `overlay` tailCuts) where
    fv v       = (vertexId v) `elem` p
    vs         = vertices $ filter fv (vertexList g)
    es         = edges $ filter (\(v1,v2) -> (fv v1) && (fv v2)) (edgeList g)
    thisGraph  = overlay vs es
    stripMerge = mkStripMerge thisGraph g
    edgesOut   = edges $ filter (\(v1,v2) -> (fv v1) && (not(fv v2))) (edgeList (stripMerge g))
    (tailParts, tailCuts) = createPartitions (stripMerge g) ps

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

unPartition :: PartitionedGraph -> Graph StreamVertex
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
        has >= 1 partition
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
generateCode sg pm opts = let
    (sgs,cuts)      = createPartitions sg pm
    sgs'            = if rewrite opts then map optimise sgs else sgs
    enumeratedParts = zip [1..] sgs'
    in map (generateCodeFromStreamGraph opts enumeratedParts cuts) enumeratedParts

data NodeType = NodeSource | NodeSink | NodeLink deriving (Show)

nodeType :: StreamGraph -> NodeType
nodeType sg = if operator (head (vertexList sg)) == Source
              then NodeSource
              else if (operator.head.reverse.vertexList) sg == Sink
                   then NodeSink
                   else NodeLink

-- vertexList outputs *sorted* (By Ord a =>). That corresponds to the Id value for
-- our StreamVertex type
-- TODO consider Difference Lists here
generateCodeFromStreamGraph :: GenerateOpts -> [(Integer, StreamGraph)] -> StreamGraph -> (Integer,StreamGraph) -> String
generateCodeFromStreamGraph opts parts cuts (partId,sg) = intercalate "\n" $
    nodeId : -- convenience comment labelling the node/partition ID
    imports' ++
    possibleSrcFn parts sg :
    possibleSinkFn parts sg :
    sgTypeSignature :
    sgIntro :
    sgBody ++
    [padding ++ "in " ++ lastIdentifier,"\n",
    "main :: IO ()",
    nodeFn parts sg partId cuts opts] where

        nodeId = "-- node"++(show partId)
        padding = "    "
        pad = map (padding++)

        sgTypeSignature = if startsWithJoin sg
            then "streamGraphFn ::"
                    ++" Stream "++inType sg++" ->"
                    ++" Stream "++inType sg++" ->"
                    ++" Stream "++outType sg
            else "streamGraphFn ::"++" Stream "++inType sg++" ->"++" Stream "++outType sg

        sgIntro = "streamGraphFn "++sgArgs++" = let"
        sgArgs = if startsWithJoin sg
            then "n1 n2"
            else "n1"
        sgBody = pad $ case zip [(valence+1)..] intVerts of
            [] -> ["n2 = n1"]
            ns -> map generateCodeFromVertex ns
        imports' = (map ("import "++) (imports opts)) ++ ["\n"]
        lastIdentifier = 'n':(show $ length intVerts
            + if startsWithJoin sg then 2 else 1)
        intVerts= filter (not . singleton) $ vertexList sg
        valence = partValence sg cuts

nodeFn parts sg partId cuts opts =
    if length parts == 1
    then "main = nodeSimple src1 streamGraphFn sink1"
    else case (nodeType sg) of
        NodeSource -> generateNodeSrc partId (connectNodeId sg parts cuts) opts parts
        NodeLink   -> generateNodeLink (partId + 1)
        NodeSink   -> generateNodeSink sg

possibleSrcFn parts sg =
    if   length parts == 1
    then generateSrcFn sg
    else case (nodeType sg) of
             NodeSource -> generateSrcFn sg
             _          -> ""

possibleSinkFn parts sg =
    if   length parts == 1
    then generateSinkFn sg
    else case (nodeType sg) of
             NodeSink -> generateSinkFn sg
             _        -> ""

possibleSrcSinkFn sg = case (nodeType sg) of
    NodeSource -> generateSrcFn sg
    NodeLink   -> ""
    NodeSink   -> generateSinkFn sg

-- output type of a StreamGraph.
-- special-case if the terminal node is a Sink node: we want the
-- "pure" StreamGraph type that feeds into the sink function.
outType :: StreamGraph -> String
outType sg = let node = (last . vertexList) sg
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
-- TODO: this breaks if the outer-edge node has been optimised away…
-- the graph of cut edges has the pre-optimisation StreamVertex in it.
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
generateNodeSrc :: Integer -> [Integer] -> GenerateOpts -> [(Integer, StreamGraph)] -> String
generateNodeSrc partId nodes opts parts = let
    node = head nodes
    host = "node" ++ (show node)

    port = case lookup node parts of
        Just sg -> if startsWithJoin sg
                   then 9001 + partId -1 -- XXX Unlikely to always be correct
                   else 9001
        Nothing -> 9001

    pref = case preSource opts of
       Nothing -> ""
       Just f  -> f

    in "main = do\n\
\       "++pref++"\n\
\       nodeSource (defaultSource \""++host++"\" \""++(show port)++"\") src1 streamGraphFn"

-- | does this StreamGraph start with a Join operator?
startsWithJoin :: StreamGraph -> Bool
startsWithJoin sg = let
    joinOps   = filter ((==Join).operator) . vertexList $ sg
    hasInputs = map snd . edgeList $ sg
    in length joinOps > 0 && (not . or . map (`elem` hasInputs) $ joinOps)

test_startsWithJoin_1 = assertBool . startsWithJoin . path $
    [StreamVertex 1 Join [] "" "", StreamVertex 0 Merge [] "" ""]

test_startsWithJoin_2 = assertBool . not . startsWithJoin . path $
    [StreamVertex 0 Merge [] "" "", StreamVertex 1 Join [] "" ""]

test_startsWithJoin_3 = assertBool . not . startsWithJoin $ empty

generateNodeSink :: StreamGraph -> String
generateNodeSink sg =
    if startsWithJoin sg
    then "main = nodeSink2 streamGraphFn sink1 \"9001\" \"9002\""
    else "main = nodeSink (defaultSink \"9001\") streamGraphFn sink1"

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

main = htfMain htf_thisModulesTests

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

------------------------------------------------------------------------------

-- | Derive a partition map.
-- Eventually, derive all possible partition maps.
partitionings :: StreamGraph -> [Partition] -> PartitionMap
partitionings sg parts = let
    vIds = map vertexId . vertexList $ sg
    in case compareLength vIds parts of
        EQ -> map (:[]) vIds

        GT -> let
            diff         = length vIds - length parts
            (first,rest) = splitAt (diff + 1) vIds
            in [first] ++ map (:[]) rest

        LT -> error "cannot partition a graph over more partitions than there are nodes"

partTestGraph = path
    [ StreamVertex 0 Source []        "Int" "Int"
    , StreamVertex 1 Map ["show","s"] "Int" "String"
    , StreamVertex 2 Filter ["<3"]    "Int" "Int"
    , StreamVertex 3 Window []        "String" "[String]"
    , StreamVertex 4 Sink []          "String" "String"
    ]

test_partitionings_1 = assertEqual [[x]|x <- [0..4]] $
    partitionings partTestGraph [0..4]

test_partitionings_2 = assertEqual 3 $ length $
    partitionings partTestGraph [0..2]

test_partitionings_3 = assertEqual 3 $ length $ head $
    partitionings partTestGraph [0..2]

-- | placeholder
allPartitionings :: StreamGraph -> [Partition] -> [PartitionMap]
allPartitionings sg pt = let
    count = length pt
    in filter ((==count) . length) (allPartitions sg)

------------------------------------------------------------------------------

-- | write out all rewritten versions of the input StreamGraph, along with some
-- of the necessary supporting code.
-- TODO: rename this to something more intuitive
optimiseWriteOutAll :: FilePath -> [Partition] -> StreamGraph -> IO ()
optimiseWriteOutAll fn parts =
    writeFile fn
        . template
        . intercalate "\n    , "
        . map show
        . deriveStreamGraphOptions parts

-- | Apply the Logical Optimiser to the supplied StreamGraph and then return a
-- list of all possible pairings of StreamGraphs and Partition Maps
-- TODO tests for this pure bit
deriveStreamGraphOptions :: [Partition] -> StreamGraph -> [(StreamGraph, PartitionMap)]
deriveStreamGraphOptions parts sg =
    [ (x,y) | x <- optimise' sg, y <- allPartitionings x parts ]

optimise' :: StreamGraph -> [StreamGraph]
optimise' = nub . map simplify . applyRules 5

-- first ensure that the Logical Optimiser produces at least one rewritten graph
-- for this test input
test_ensureOptimised' = assertBool $ length (optimise' partTestGraph) > 1

-- we must have at least as many options after considering partition mapping as
-- we have StreamGraphs
test_deriveStreamGraphOptions = assertBool $
    length (optimise' partTestGraph) <= length (deriveStreamGraphOptions [0..4] partTestGraph)

template g = intercalate "\n"
    [ "import Striot.StreamGraph"
    , "import Algebra.Graph"
    , "\n"
    , "graphs = "
    , "    [ "++g
    , "    ]\n"
    ]

------------------------------------------------------------------------------

allPartitions :: StreamGraph -> [[[Int]]]
allPartitions = (map.map.map) vertexId . foldgl fun [] . transpose
    where
        fun []      n = [[[n]]]
        fun choices n = concatMap (extendPartitioning n) choices

-- | A left-fold over Graphs. Unlike 'foldg', the traversal order follows
-- edges from a root node.
--
-- XXX could we rework this in terms of subGraph, instead of subGraphs?
-- Caveat: when the incoming graph is e.g. overlay x y, getRoot returns
-- one of x or y, and subGraphs returns [], so we lose the other branch
foldgl :: Eq a => Ord a =>
               (b -> a -> b) -> b -> Graph a -> b
foldgl f z g =
    if isEmpty g then z
    else let x  = getRoot g     -- :: a
             xs = subGraphs x g -- [Graph a]
         in foldl (\b g -> foldgl f b g) (f z x) xs

test_foldgl1 = assertEqual "ABC" $
    foldgl (\b a -> b++[a]) "" (path "ABC")
test_foldgl2 = assertEqual "ABCD" $
    foldgl (\b a -> b++[a]) "" (overlay (path "ABC") (path "BD"))

getRoots :: Eq a => Ord a =>
            Graph a -> [a]
getRoots g = let
    edges = edgeList g
    dests = map snd edges
    roots = filter (not.(`elem`dests)) (vertexList g)
    in roots

getRoot :: Eq a => Ord a =>
           Graph a -> a
getRoot = head . getRoots

subGraphs :: Ord a =>
             a -> Graph a -> [Graph a]
subGraphs n g = map (\k -> subGraph k g) (childrenOf n g)

test_subGraphs1 = assertEqual (subGraphs 2 t2) [Vertex v | v <- [3,4]]
test_subGraphs2 = assertEqual (subGraphs 3 t2) []
test_subGraphs3 = assertEqual (subGraphs 1 t1) [path [2,3]]
test_subGraphs4 = assertEqual (subGraphs 1 t2) [removeVertex 1 t2]

v0 = StreamVertex 0 Source [] "" ""
v1 = StreamVertex 1 Map [] "" ""
v2 = StreamVertex 2 Sink [] "" ""
v3 = StreamVertex 3 Source [] "" ""
v4 = StreamVertex 4 Merge [] "" ""
v5 = StreamVertex 5 Map [] "" ""
g3 = overlay (path [v0, v1, v4, v2]) (path [v3, v5, v4])
g4 = transpose g3

test_subGraphs5 = let
    g = head $ subGraphs (getRoot g4) g4
    in assertBool $ v0 `elem` (vertexList g)

subGraph :: Eq a => Ord a =>
            a -> Graph a -> Graph a
subGraph n g = induce (`elem` reachable n g) g

t1 = path [1,2,3]
test_subGraph1 = assertEqual (subGraph 1 t1) $ t1
test_subGraph2 = assertEqual (subGraph 2 t1) $ path [2,3]
test_subGraph3 = assertEqual (subGraph 3 t1) $ Vertex 3

t2 = t1 + path [2,4]
test_subGraph4 = assertEqual (subGraph 4 t2) $ Vertex 4
test_subGraph5 = assertEqual (subGraph 2 t2) $ path [2,4] + path [2,3]

childrenOf :: Ord a =>
              a -> Graph a -> [a]
childrenOf n = map snd . filter ((==n).fst) . edgeList

-- | Given an existing partitioning and a new operator to consider: We can
-- start a new partition; in some circumstances we can also append the operator
-- to the last partition.
extendPartitioning :: StreamVertex -> [[StreamVertex]] -> [[[StreamVertex]]]
extendPartitioning n choice = let
  lastNode = last . last $ choice
  in if 1 < (length (filter singleton (n:(last choice))))
     || operator lastNode `elem` [Merge,Source]
     then [ choice ++ [[n]] ]
     else [ choice ++ [[n]]
          , init choice ++ [last choice ++ [n]]
          ]

singleton v = operator v `elem` [Source,Sink]

g' = path [ v0 , v1 , v2 ]
test_g' = assertEqual [ [[2],[1],[0]]
                      , [[2],[1,0]]
                      , [[2,1],[0]]]
    $ allPartitions g'

g2 = overlay (path [v0, v4, v2]) (path [v3, v4])

test_g2 = assertEqual [ [[2],[4],[0],[3]]
                      , [[2,4],[0],[3]]]
    $ allPartitions g2

test_g3 = assertEqual
    [ [[2],[4],[1],[0],[5],[3]]
    , [[2],[4],[1],[0],[5,3]]
    , [[2],[4],[1,0],[5],[3]]
    , [[2],[4],[1,0],[5,3]]
    , [[2,4],[1],[0],[5],[3]]
    , [[2,4],[1],[0],[5,3]]
    , [[2,4],[1,0],[5],[3]]
    , [[2,4],[1,0],[5,3]]
    ] $ allPartitions g3
