{-
    demonstration of generating examples/pipeline via CompileIoT
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph

opts = defaultOpts { rewrite = False }

graph = path
    [ StreamVertex 1 (Source 1) [[| do
            threadDelay (1000*1000)
            return "Hello from Client!"
        |]] "String" "String" 0
    , StreamVertex 2 Map    [[| \st->st++st |]] "String" "String" 1
    , StreamVertex 3 Map    [[| reverse |]]     "String" "String" 1
    , StreamVertex 4 Map    [[| \st->"Incoming Message at Server: " ++ st |]] "String" "String" 1
    , StreamVertex 5 Window [[| chop 2 |]]      "String" "[String]" 1
    , StreamVertex 6 Sink   [[| mapM_ print |]] "[String]" "IO ()" 0
    ]

main = partitionGraph graph [[1,2],[3],[4,5,6]] opts
