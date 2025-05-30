{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}

                         -- entrypoints
module Striot.CompileIoT ( createPartitions -- pure
                         , partitionGraph   -- impure

                         -- types
                         , GenerateOpts(..)
                         , defaultOpts
                         , Partition
                         , PartitionMap
                         , Plan(..)

                         -- not used outside CompileIoT
                         , generateCode
                         , writePart
                         , genDockerfile
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
import Data.Maybe (catMaybes)
import Data.List (nub,sort)
import Data.List.Match (compareLength)
import Language.Haskell.TH

import Striot.StreamGraph
import Striot.LogicalOptimiser
import Striot.Partition

------------------------------------------------------------------------------
-- StreamGraph Partitioning

-- | A 'Partition', a.k.a. *Node*, in a deployment.
-- Eventually we may define properties about Partitions in these types. For
-- now they are considered to be homogeneous. The chosen type just needs to
-- be enumerable.
type Partition = Int

-- |The user's desired partitioning of the input Graph.
-- Each element in the outer-most list corresponds to a distinct partition.
-- The inner-lists are the IDs of Operators to include in that partition.
type PartitionMap = [[Int]]

-- | A Plan is a pairing of a 'StreamGraph' with a 'PartitionMap' that could
-- be used for its partitioning and deployment.
data Plan = Plan { planStreamGraph  :: StreamGraph
                 , planPartitionMap :: PartitionMap }
                 deriving (Eq)

-- |`createPartitions` returns ([partitions], [inter-graph links])
-- where inter-graph links are the cut edges due to partitioning
createPartitions :: StreamGraph -> PartitionMap -> PartitionedGraph
createPartitions _ [] = ([],empty)
createPartitions g (p:ps) = (thisGraph:tailParts, edgesOut `overlay` tailCuts) where
    fv v       = (vertexId v) `elem` p
    thisGraph  = induce fv g
    edgesOut   = edges $ filter (\(v1,v2) -> (fv v1) && (not(fv v2))) (edgeList g)
    (tailParts, tailCuts) = createPartitions g ps

unPartition :: PartitionedGraph -> Graph StreamVertex
unPartition (a,b) = overlays (b:a)

------------------------------------------------------------------------------
-- Code generation from StreamGraph definitions

{-
    a well-formed streamgraph:
        always starts with a Source?
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

-- | Options for source code generation are captured in instances of the
-- 'GenerateOpts' data-type.
data GenerateOpts = GenerateOpts
  { imports           :: [String]      -- ^ list of import statements to add to generated files
  , packages          :: [String]      -- ^ list of Cabal packages to install within containers
  , preSource         :: Maybe String  -- ^ code to run prior to starting 'nodeSource'
  , rules             :: [LabelledRewriteRule] -- ^ A list of rewrite rules for the logical optimiser
  , maxNodeUtil       :: Double        -- ^ The per-Partition utilisation limit
  , maxBandwidth      :: Double        -- ^ Bandwidth limit between the first two deployment Nodes
  }

-- | Sensible default values for 'GenerateOpts'. Users who wish to customise
-- options in 'GenerateOpts' are encouraged to derive from 'defaultOpts'.
defaultOpts = GenerateOpts
  { imports     = [ "Striot.FunctionalIoTtypes"
                  , "Striot.FunctionalProcessing"
                  , "Striot.Nodes"
                  , "Control.Concurrent"
                  , "Control.Category ((>>>))" -- (generated by rewrites)
                  ]
  , packages    = []
  , preSource   = Nothing
  , rules       = defaultRewriteRules
  , maxNodeUtil = 3.0
  , maxBandwidth= 200
  }

-- |Partitions the supplied `StreamGraph` according to the supplied `PartitionMap`
-- and options specified within the supplied `GenerateOpts` and returns a list of
-- the sub-graphs converted into source code and encoded as `String`s.
generateCode :: GenerateOpts -> StreamGraph -> PartitionMap -> [String]
generateCode opts sg pm = let
    (sgs,cuts)      = createPartitions sg (sort (map sort pm))
    enumeratedParts = zip [1..] sgs
    in map (generateCodeFromStreamGraph opts enumeratedParts cuts) enumeratedParts

-- TODO: the sorting of the `PartitionMap` is a work-around for
-- <https://github.com/striot/striot/issues/124>
--
-- TODO: there is no test coverage for generateCode

data NodeType = NodeSource | NodeSink | NodeLink deriving (Show)

nodeType :: StreamGraph -> NodeType
nodeType sg = if isSource $ operator (head (vertexList sg))
              then NodeSource
              else if (operator.head.reverse.vertexList) sg == Sink
                   then NodeSink
                   else NodeLink

------------------------------------------------------------------------------

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

        sgTypeSignature = "streamGraphFn ::" ++ case startsWith sg of
            Join ->   " Stream "++inType sg++" ->"
                    ++" Stream "++inType sg++" ->"
                    ++" Stream "++outType sg
            -- the merge takes place in TCP/IP prior to the streamGraphFn.
            -- note the re-use of outType (a) instead of inType ([a])
            Merge -> " Stream "++outType sg++" ->"++" Stream "++outType sg
            _     -> " Stream "++inType  sg++" ->"++" Stream "++outType sg

        sgIntro = "streamGraphFn "++sgArgs++" = let"
        sgArgs = if startsWithJoin sg
            then "n1 n2"
            else "n1"
        sgBody = pad $ case intVerts of
            [] -> ["n2 = n1"]
            ns -> if startsWithJoin sg
                  then map generateCodeFromVertex (zip [3..] ns)
                  else map generateCodeFromVertex (zip [2..] ns)

        imports' = (map ("import "++) (imports opts)) ++ ["\n"]
        lastIdentifier = 'n':(show $ length intVerts
            + if startsWithJoin sg then 2 else 1)
        intVerts= filter (not . singleton) $ vertexList sg

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
            in if isSource $ operator node
               then outtype node
               else intype node

t = path [ StreamVertex 0 (Source 1) [[| return 0 |]]       "IO Int" "Int" 1
         , StreamVertex 1 Map    [[| show |]]       "Int" "String" 2
         , StreamVertex 2 Sink   [[| mapM_ putStrLn |]] "String" "IO ()" 3
         ]

test_outType = assertEqual "String" $
    (outType . head . fst) (createPartitions t [[0,1],[2]])

test_outType_sink = assertEqual "String" $
    (outType . head . fst) (createPartitions t [[0,1,2]])

test_inType = assertEqual "Int" $
    (inType . head . fst) (createPartitions t [[0,1],[2]])

-- determine the node(s?) to connect on to from this partition
-- XXX always 0 or 1? write quickcheck property...
-- TODO: this breaks if the outer-edge node has been optimised away...
-- the graph of cut edges has the pre-optimisation StreamVertex in it.
connectNodeId :: StreamGraph -> [(Integer, StreamGraph)] -> StreamGraph -> [Integer]
connectNodeId sg parts cuts = let
    edges = edgeList cuts
    outs  = vertexList sg
    outEs = filter (\(f,t) -> f `elem` outs) edges
    destVs= map snd outEs
    destGs= concatMap (\v -> filter (\(n,sg) -> v `elem` (vertexList sg)) parts) destVs

    in case map fst destGs of
        [] -> error "connectNodeId returned an empty list, last vertex optimised away?"
        x  -> x

generateSrcFn :: StreamGraph -> String
generateSrcFn sg = "src1 = " ++
    (intercalate "\n" . map showParam . parameters . head . vertexList $ sg) ++ "\n"

generateSinkFn:: StreamGraph -> String
generateSinkFn sg = "sink1 :: Show a => Stream a -> IO ()\nsink1 = " ++
    (intercalate "\n" . map showParam . parameters . head . reverse . vertexList $ sg) ++ "\n"

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
       Just f  -> f ++ "\n  "

    in "main = do\n  " ++ pref ++ (showParam [|
        nodeSource (defaultSource $(litE (StringL host))
                                  $(litE (StringL (show port))))
                   src1 streamGraphFn
    |])

-- | does this StreamGraph start with a Join operator?
startsWithJoin :: StreamGraph -> Bool
startsWithJoin = (Join==) . startsWith

startsWith :: StreamGraph -> StreamOperator
startsWith sg = let
    inEdges = map snd (edgeList sg)
    in (operator . head . filter (not . (flip elem) inEdges) . vertexList) sg

test_startsWithJoin_1 = assertBool . startsWithJoin . path $
    [StreamVertex 1 Join [] "" "" 1, StreamVertex 0 Merge [] "" "" 2]

test_startsWithJoin_2 = assertBool . not . startsWithJoin . path $
    [StreamVertex 0 Merge [] "" "" 3, StreamVertex 1 Join [] "" "" 4]

generateNodeSink :: StreamGraph -> String
generateNodeSink sg = "main = " ++ (showParam $
    if startsWithJoin sg
    then [| nodeSink2 streamGraphFn sink1 "9001" "9002" |]
    else [| nodeSink (defaultSink "9001") streamGraphFn sink1 |])

-- generateCodeFromVertex:  generates Haskell code to be included in a
-- let expression, corresponding to the supplied StreamVertex. The Int
-- argument represents the sequence order of the StreamVertex relative
-- to others, and is used to calculate the names of the input stream
-- argument(s).
-- As the StreamVertex parameters may need to reference the input streams,
-- the generated expression is wrapped in a lambda expression with a
-- constant stream name 's'.
generateCodeFromVertex :: (Int, StreamVertex) -> String
generateCodeFromVertex (opid, v) = let
    op  = operator v
    n   = 'n' : show opid
    n_1 = 'n' : show (opid - 1)
    in case op of
        -- merges are handled by the runtime via TCP/IP. Replace with a no-op
        Merge -> n++" = "++n_1

        -- Joins have two inputs and no parameters
        Join  -> let n_2 = 'n' : show (opid - 2)
                 in  concat [n, " = streamJoin ", n_2, " ", n_1]

        _ -> let params  = intercalate " " $ map (paren.showParam) (parameters v)
             in  concat [n, " = (\\s -> ", printOp op, " ", params, " s) ", n_1]


paren :: String -> String
paren s = "("++s++")"

printOp :: StreamOperator -> String
printOp (Filter _) = "streamFilter"
printOp (FilterAcc _) = "streamFilterAcc"
printOp op = "stream" ++ (show op)

------------------------------------------------------------------------------
-- tests / test data

main = htfMain htf_thisModulesTests

-- Source -> Sink
s0 = connect (Vertex (StreamVertex 0 ((Source 1)) [] "String" "String" 1))
             (Vertex (StreamVertex 1 (Sink) [] "String" "String" 2))

-- Source -> Filter -> Sink
s1 = path [ StreamVertex 0 ((Source 1)) [] "String" "String" 3
          , StreamVertex 1 (Filter 0.5) [] "String" "String" 4
          , StreamVertex 2 (Sink) [] "String" "String" 5
          ]

test_reform_s0 = assertEqual s0 (unPartition $ createPartitions s0 [[0],[1]])
test_reform_s1 = assertEqual s1 (unPartition $ createPartitions s1 [[0,1],[2]])
test_reform_s1_2 = assertEqual s1 (unPartition $ createPartitions s1 [[0],[1,2]])

genDockerfile listen opts = 
    let pkgs = packages opts in concat
    [ "FROM ghcr.io/striot/striot:main\n"
    , "WORKDIR /opt/node\n"
    , "COPY . /opt/node\n"
    , if pkgs /= [] then "RUN cabal install " ++ (intercalate " " pkgs) else ""
    , "\n"
    , "RUN ghc node.hs\n"
    , if listen then "EXPOSE 9001\n" else ""
    , "CMD /opt/node/node\n"
    ]

mergeEx = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Merge [] "[Int]" "Int" 2
    , StreamVertex 2 Map [[| show |]] "Int" "String" 3
    , StreamVertex 3 Sink [[| mapM_ print |]] "String" "String" 4
    ]

------------------------------------------------------------------------------

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
-- 
-- *TODO*: move GenerateOpts to first parameter?
partitionGraph :: StreamGraph -> PartitionMap -> GenerateOpts -> IO ()
partitionGraph graph partitions opts = do
    mapM_ (writePart opts) $ zip [1..] $ generateCode opts graph partitions
