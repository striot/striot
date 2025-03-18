{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}

module Striot.VizGraph ( streamGraphToDot
                       , displayGraph
                       , displayGraph'
                       , displayGraphKitty
                       , displayGraphDebug
                       , displayPartitionedGraph
                       , jacksonGraphToDot
                       , partitionedGraphToDot
                       , subGraphToPartition
                       , writeGraph

                       , bandwidthStyle
                       , baseGraphStyle
                       , jacksonStyle
                       , enumGraphStyle

                       , displayPlan

                       , htf_thisModulesTests) where

import Striot.StreamGraph
import Striot.Bandwidth
import Striot.CompileIoT
import Striot.Jackson
import Algebra.Graph
import Algebra.Graph.Export.Dot
import Data.String
import Test.Framework
import Data.List (intercalate)
import Data.List.Split
import Language.Haskell.TH

import System.Process
import System.IO (openTempFile, hPutStr, hGetContents, hClose)

------------------------------------------------------------------------------
-- main functions

streamGraphToDot :: StreamGraph -> String
streamGraphToDot = export baseGraphStyle

-- | display a graph using GraphViz and "display" from ImageMagick
displayGraph :: StreamGraph -> IO ()
displayGraph = displayGraph' streamGraphToDot

-- | display a graph by applying a provided converter to the supplied
-- StreamGraph
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
debugStyle        = baseGraphStyle { vertexAttributes = (\v -> ["label":=(doubleQuotes . escape . show) v]) }
    where doubleQuotes v = "\"" ++ v ++ "\""

-- | display a `PartitionedGraph` using GraphViz and "display" from ImageMagick
displayPartitionedGraph :: PartitionedGraph -> IO ()
displayPartitionedGraph = displayGraph' partitionedGraphToDot

-- | Convert a `StreamGraph` into a GraphViz representation, including
-- parameters derived from queueing theory/Jackson
jacksonGraphToDot :: StreamGraph -> String
jacksonGraphToDot graph = export (jacksonStyle graph) graph

-- | Convert `PartitionedGraph` into a GraphViz representation,
-- with each sub-graph separately delineated,
-- encoded in a `String`.
partitionedGraphToDot :: PartitionedGraph -> String
partitionedGraphToDot pgs@(ps,cuts) = let
    graph = overlays (cuts:ps)
    pre   = map (uncurry subGraphToPartition) (zip ps [1..])
    style = (bandwidthStyle graph) { preamble = pre }
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
    \    labeljust=r\n\
    \    label=\"Node "++n++"\"\n\
    \    "++ids++"\n\
    \  }\n"

-- | Render a graph to a PNG using GraphViz and write it out to the supplied
-- path.
writeGraph toDot g path = do
    (Just hin, _, _, _) <- createProcess (proc "dot" ["-Tpng", "-o", path])
      { std_in = CreatePipe }
    hPutStr hin (toDot g)
    hClose hin

------------------------------------------------------------------------------
-- graph display styles

-- | the base Style information for StreamGraphs of all types.
baseGraphStyle :: Style StreamVertex String
baseGraphStyle = Style
    { graphName               = mempty
    , preamble                = mempty
    , graphAttributes         = ["bgcolor":="white","ratio":="compress"]
    , defaultVertexAttributes = ["shape" := "box","fillcolor":="white","style":="filled"]
    , defaultEdgeAttributes   = ["weight":="10","color":="black","fontcolor":="black","fontsize":="18"]
    , vertexName              = show . vertexId
    , vertexAttributes        = (\v -> [ "label":=(("<"++) . (++">") . show') v
                                       , "fontsize":="18"
                                       , "shape":="box"])
    -- without forcing shape=box, the nodes end up ellipses in PartitionedGraphs
    , edgeAttributes          = (\_ o -> ["label":=("\" " ++ intype o ++ "\"")])
    , attributeQuoting        = NoQuotes
    }

-- | A specialised Style to annotate edges with calculated bandwidth
-- XXX merge with jacksonStyle?
bandwidthStyle :: StreamGraph -> Style StreamVertex String
bandwidthStyle sg = enumGraphStyle
    {
        -- XXX: bandwidth units?
        edgeAttributes = \v o -> "xlabel" := concat
            [ "<",
              case whatBandwidthWeighted sg (vertexId v) of
                  Nothing -> "??"
                  Just b  -> show b,
              " <sup>bytes</sup>/<sub>sec</sub>>"
            ] : (edgeAttributes baseGraphStyle) v o
    }

-- | A specialised Style for StreamGraphs annotated with Jackson parameters.
-- Presently requires a StreamGraph argument.
jacksonStyle graph = baseGraphStyle
    { edgeAttributes   = (\i _ -> [ "label" := concat [ "<λ = <SUP>"
                                                      , show (outputRate graph (vertexId i))
                                                      , "</SUP>/<SUB>s</SUB>>"
                                                      ]])

    , vertexAttributes = (\v -> [ "label"     :=(("<"++) . (++">") . show') v
                                , "xlabel"    := ("<" ++ srvRate v
                                              ++  "<br />"
                                              ++  utilS v ++">")
                                , "fontsize":="18"
                                , "fillcolor" := if   overUt v
                                                 then "\"#ffcccc\""
                                                 else "\"#ffffff\"" ])
    } where
    jackson = map (\oi -> (opId oi, oi)) (calcAllSg graph) -- (Int, [OperatorInfo])

    srvRate v = case serviceRate v of
        0 -> "" -- effectively undefined/not useful
        t -> "μ = " ++ wrap (show t)

    wrap s = "<SUP>"++s++"</SUP>/<SUB>s</SUB>"

    utilS  v = case lookup (vertexId v) jackson of
        Nothing -> ""
        Just oi -> case util oi of
            0.0 -> "" -- hide utilisation=0
            n   -> "ρ = " ++ show n

    overUt v = case lookup (vertexId v) jackson of
        Nothing -> False
        Just oi -> util oi > 1

-- | A StreamGraph Style where each Operator is annotated with its vertexId.
enumGraphStyle = baseGraphStyle
 {
    vertexAttributes = \v ->
        (vertexAttributes baseGraphStyle) v
        ++ ["xlabel" := show (vertexId v)]
 }
------------------------------------------------------------------------------
-- utility functions

show' :: StreamVertex -> String
show' v = intercalate "<br />\n" $ ((printOp . operator) v)
                          : (map (paren . cleanParam . showParam) . parameters) v

printOp :: StreamOperator -> String
printOp (Filter _)        = "streamFilter"
printOp (FilterAcc _)     = "streamFilterAcc"
printOp (Source _)        = "streamSource"
printOp x                 = "stream" ++ (show x)

paren :: String -> String
paren s = '(' : (s ++ ")")

prop_paren_prefix s = head (paren s) == '('
prop_paren_suffix s = last (paren s) == ')'

-- escape HTML brackets
cleanParam :: String -> String
cleanParam [] = []
cleanParam (x:s) | x == '<'  = "&lt;"  ++ cleanParam s
                 | x == '>'  = "&gt;"  ++ cleanParam s
                 | x == '&'  = "&amp;" ++ cleanParam s
                 | x == '\n' = "<br />\n" ++ cleanParam s
                 | otherwise = x : cleanParam s

test_cleanParam_1 = assertEqual "no escaping"          $ cleanParam "no escaping"
test_cleanParam_2 = assertEqual "opening &lt; chevron" $ cleanParam "opening < chevron"
test_cleanParam_3 = assertEqual "closing &gt; chevron" $ cleanParam "closing > chevron"
test_cleanParam_4 = assertEqual "ampersand &amp; amp"  $ cleanParam "ampersand & amp"

-- escape a string, suitable for inclusion inside a double-quoted string in a .dot file
escape [] = []
escape (x:xs) = case x of
    '"'  -> '\\':'"' : escape xs
    '\\' -> '\\':'\\': escape xs
    _    -> x        : escape xs

test_escape_1 = assertEqual "no escaping"            $ escape ("no escaping")
test_escape_2 = assertEqual "escaped \\\" quote"     $ escape ("escaped \" quote")
test_escape_3 = assertEqual "escaped \\\\ backslash" $ escape ("escaped \\ backslash")

------------------------------------------------------------------------------
-- test data
pgs   = createPartitions mergeEx [[1,2],[3,4],[5,6]]
pgs'  = createPartitions expandEx [[1,2],[5],[6]]

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

------------------------------------------------------------------------------
-- plans

displayPlan :: Plan -> IO ()
displayPlan (Plan g p) =
  displayPartitionedGraph (createPartitions g p)
