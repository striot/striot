{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}

module Striot.VizGraph ( streamGraphToDot
                       , displayGraph
                       , displayGraphKitty
                       , displayGraphDebug
                       , displayPartitionedGraph
                       , partitionedGraphToDot
                       , subGraphToPartition

                       , htf_thisModulesTests) where

import Striot.StreamGraph
import Striot.CompileIoT
import Algebra.Graph
import Algebra.Graph.Export.Dot
import Data.String
import Test.Framework
import Data.List (intercalate)
import Data.List.Split
import Language.Haskell.TH

import System.Process
import System.IO (openTempFile, hPutStr, hGetContents, hClose)

streamGraphToDot :: StreamGraph -> String
streamGraphToDot = export myStyle

show' :: StreamVertex -> String
show' v = intercalate " " $ ((printOp . operator) v) : ((map (\s->"("++s++")")) . map showParam . parameters) v

printOp :: StreamOperator -> String
printOp (Filter _)        = "streamFilter"
printOp (FilterAcc _)     = "streamFilterAcc"
printOp (Source _)        = "streamSource"
printOp x                 = "stream" ++ (show x)

myStyle :: Style StreamVertex String
myStyle = Style
    { graphName               = mempty
    , preamble                = mempty
    , graphAttributes         = ["bgcolor":="white"]
    , defaultVertexAttributes = ["shape" := "box","fillcolor":="white","style":="filled"]
    , defaultEdgeAttributes   = ["weight":="10","color":="black","fontcolor":="black"]
    , vertexName              = show . vertexId
    , vertexAttributes        = (\v -> ["label":=(escape . show') v])
    , edgeAttributes          = (\_ o -> ["label":=intype o])
    }

-- escape a string, suitable for inclusion in a .dot file
escape [] = []
escape (x:xs) = case x of
    '"'  -> '\\':'"' : escape xs
    '\\' -> '\\':'\\': escape xs
    _    -> x        : escape xs

test_escape_1 = assertEqual "no escaping"        $ escape ("no escaping")
test_escape_2 = assertEqual "escaped \\\" quote" $ escape ("escaped \" quote")
test_escape_3 = assertEqual "escaped \\\\ backslash" $ escape ("escaped \\ backslash")

-- test data

source x = [| do
    let x' = $(litE (StringL x))
    threadDelay (1000*1000)
    putStrLn "sending '"++x'++"'"
    return x'
    |]

v1 = StreamVertex 1 (Source 1) [source "foo"]    "String" "String" 0
v2 = StreamVertex 2 Map    [[| id |]]        "String" "String" 1
v3 = StreamVertex 3 (Source 1) [source "bar"]    "String" "String" 2
v4 = StreamVertex 4 Map    [[| id |]]        "String" "String" 3
v5 = StreamVertex 5 Merge  []                "[String]" "String" 4
v6 = StreamVertex 6 Sink   [[| mapM_ print|]] "String" "IO ()" 5
mergeEx :: StreamGraph
mergeEx = overlay (path [v3, v4, v5]) (path [v1, v2, v5, v6])

v7 = StreamVertex  1 (Source 1) [[| sourceOfRandomTweets |]] "String" "String" 0
v8 = StreamVertex  2 Map    [[| filter (('#'==).head) . words |]] "String" "[String]" 1
v9 = StreamVertex  5 Expand [] "[String]" "String" 2
v10 = StreamVertex 6 Sink   [[|mapM_ print|]] "String" "IO ()" 3
expandEx :: StreamGraph
expandEx = path [v7, v8, v9, v10]

-- | display a graph using GraphViz and "display" from ImageMagick
displayGraph :: StreamGraph -> IO ()
displayGraph = displayGraph' streamGraphToDot

displayGraph' toDot g = do
    (Just hin,Just hout,_, _) <- createProcess (proc "dot" ["-Tpng"])
      { std_out = CreatePipe, std_in = CreatePipe }
    _ <- createProcess (proc "display" []){ std_in = UseHandle hout }

    hPutStr hin (toDot g)
    hClose hin

-- | display a graph inline in the Kitty terminal emulator
displayGraphKitty :: StreamGraph -> IO ()
displayGraphKitty g = do
    (Just hin, Just hout, _, _)   <- createProcess (proc "dot" ["-Tpng"])
      { std_out = CreatePipe, std_in = CreatePipe }
    (_, Just hout2, _, _) <- createProcess (proc "base64" ["-w0"])
      { std_in = UseHandle hout , std_out = CreatePipe }

    hPutStr hin (streamGraphToDot g)
    hClose hin
    foo <- hGetContents hout2
    let bar = chunksOf 4096 foo
    mapM_ (\c -> putStr $ "\ESC_Gf=100,a=T,m=1;" ++ c ++ "\ESC\\") (init bar)
    putStrLn $ "\ESC_Gf=100,a=T,m=0;" ++ (last bar) ++ "\ESC\\"

-- | display a debug graph using GraphViz and ImageMagick
displayGraphDebug = displayGraph' (export debugStyle :: StreamGraph -> String)
debugStyle        = myStyle { vertexAttributes = (\v -> ["label":=(escape . show) v]) }

-- | display a `PartitionedGraph` using GraphViz and "display" from ImageMagick
displayPartitionedGraph :: PartitionedGraph -> IO ()
displayPartitionedGraph = displayGraph' partitionedGraphToDot

-- | Convert `PartitionedGraph` into a GraphViz representation,
-- with each sub-graph separately delineated,
-- encoded in a `String`.
partitionedGraphToDot :: PartitionedGraph -> String
partitionedGraphToDot pgs@(ps,cuts) = let
    graph = overlays (cuts:ps)
    pre   = map (uncurry subGraphToPartition) (zip ps [0..])
    style = myStyle
      { preamble = pre -- without forcing shape=box, the nodes end up ellipses
      , vertexAttributes = (\v -> [ "label":=(escape . show') v
                                  , "shape":="box"]) }
    in export style graph

-- | generate a GraphViz subgraph definition (encoded into a `String`)c
-- corresponding to a StreamGraph and an Int representing a label.
--
-- We place the Partition label at the bottom and offset it with some
-- whitespace to reduce the likelyhood of the label being overdrawn by
-- edges or edge labels.
subGraphToPartition :: StreamGraph -> Int -> String
subGraphToPartition sg i = let
    n = show i
    ids = intercalate "," $ map (show.vertexId) (vertexList sg)
    in "  subgraph cluster"++n++" {\n\
    \    color=\"#888888\"\n\
    \    style=\"rounded,dashed\"\n\
    \    labelloc=b\n\
    \    label=\"                    Node "++n++"\"\n\
    \    "++ids++"\n\
    \  }\n"

-- test data
pgs   = createPartitions mergeEx [[1,2],[3,4],[5,6]]
pgs'  = createPartitions expandEx [[1,2],[5],[6]]
