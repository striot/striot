{-# OPTIONS_GHC -F -pgmF htfpp #-}

module VizGraph(streamGraphToDot, htf_thisModulesTests) where

import Striot.CompileIoT
import Algebra.Graph
import Algebra.Graph.Export.Dot
import Data.String
import Test.Framework

streamGraphToDot :: StreamGraph -> String
streamGraphToDot = export myStyle

myStyle :: Style StreamVertex String
myStyle = Style
    { graphName               = mempty
    , preamble                = mempty
    , graphAttributes         = ["bgcolor":="white"]
    , defaultVertexAttributes = ["shape" := "box","fillcolor":="white","style":="filled"]
    , defaultEdgeAttributes   = ["weight":="10","color":="black","fontcolor":="black"]
    , vertexName              = show . vertexId
    , vertexAttributes        = (\v -> ["label":=(escape . show) v])
    , edgeAttributes          = (\_ o -> ["label":=intype o])
    }

-- escape a string, suitable for inclusion in a .dot file
escape [] = []
escape (x:xs) = if x == '"' then '\\':'"':(escape xs) else x:(escape xs)

-- test data
--source x = "do\n    threadDelay (1000*1000)\n    putStrLn \"sending '"++x++"'\"\n    return \""++x++"\""
source x = "do threadDelay (1000*1000); putStrLn \"sending '"++x++"'\"; return \""++x++"\""
v1 = StreamVertex 1 Source [source "foo"]  "String"
v2 = StreamVertex 2 Map    ["Prelude.id"]  "String"
v3 = StreamVertex 3 Source [source "bar"]  "String"
v4 = StreamVertex 4 Map    ["Prelude.id"]  "String"
v5 = StreamVertex 5 Merge  ["[n1,n2]"]     "String"
v6 = StreamVertex 6 Sink   ["mapM_ print"] "String"
mergeEx :: StreamGraph
mergeEx = overlay (path [v3, v4, v5]) (path [v1, v2, v5, v6])

v7 = StreamVertex 1 Source ["<source of random tweets>"] "String"
v8 = StreamVertex 2 Map    ["filter (('#'==).head) . words"] "[String]"
v9 = StreamVertex 5 Expand [""]                 "[String]"
v10 = StreamVertex 6 Sink   ["mapM_ print"] "String"
expandEx :: StreamGraph
expandEx = path [v7, v8, v9, v10]
