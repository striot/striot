{-
    demonstration of generating examples/pipeline via CompileIoT
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph

opts = defaultOpts { rewrite = False }

graph = path
    [ StreamVertex 1 Source [[| do
            threadDelay (1000*1000)
            return "Hello from Client!"
        |]] "String" "String"
    , StreamVertex 2 Map    [[| \st->st++st |]] "String" "String"
    , StreamVertex 3 Map    [[| reverse |]]     "String" "String"
    , StreamVertex 4 Map    [[| \st->"Incoming Message at Server: " ++ st |]] "String" "String"
    , StreamVertex 5 Window [[| chop 2 |]]      "String" "[String]"
    , StreamVertex 6 Sink   [[| mapM_ print |]] "[String]" "IO ()"
    ]

main = partitionGraph graph [[1,2],[3],[4,5,6]] opts
