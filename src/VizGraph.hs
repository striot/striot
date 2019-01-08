{-# OPTIONS_GHC -F -pgmF htfpp #-}

module VizGraph(streamGraphToDot, htf_thisModulesTests) where

import Striot.CompileIoT
import Algebra.Graph
import Algebra.Graph.Export.Dot
import Data.String
import Test.Framework
import Data.List (intercalate)

streamGraphToDot :: StreamGraph -> String
streamGraphToDot = export myStyle

show' :: StreamVertex -> String
show' v = intercalate " " $ ((show . operator) v) : ((map (\s->"("++s++")")) . parameters) v

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
escape (x:xs) = if x == '"' then '\\':'"':(escape xs) else x:(escape xs)

-- test data
--source x = "do\n    threadDelay (1000*1000)\n    putStrLn \"sending '"++x++"'\"\n    return \""++x++"\""
source x = "do threadDelay (1000*1000); putStrLn \"sending '"++x++"'\"; return \""++x++"\""
v1 = StreamVertex 1 Source [source "foo"]  "String" "String"
v2 = StreamVertex 2 Map    ["Prelude.id"]  "String" "String"
v3 = StreamVertex 3 Source [source "bar"]  "String" "String"
v4 = StreamVertex 4 Map    ["Prelude.id"]  "String" "String"
v5 = StreamVertex 5 Merge  ["[n1,n2]"]     "[String]" "String"
v6 = StreamVertex 6 Sink   ["mapM_ print"] "String" "IO ()"
mergeEx :: StreamGraph
mergeEx = overlay (path [v3, v4, v5]) (path [v1, v2, v5, v6])

v7 = StreamVertex 1 Source ["<source of random tweets>"] "String" "String"
v8 = StreamVertex 2 Map    ["filter (('#'==).head) . words"] "String" "[String]"
v9 = StreamVertex 5 Expand [""]                 "[String]" "String"
v10 = StreamVertex 6 Sink   ["mapM_ print"] "String" "IO ()"
expandEx :: StreamGraph
expandEx = path [v7, v8, v9, v10]
