{-
    demonstration of generating examples/merge via CompileIoT
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph

source x y = "do\n\
\    threadDelay ("++y++")\n\
\    putStrLn \"sending '"++x++"'\"\n\
\    return \""++x++"\""

v1 = StreamVertex 1 Source [source "foo" "1000*1000"] "String" "String"
v2 = StreamVertex 2 Source [source "bar"  "500*1000"] "String" "String"
v3 = StreamVertex 3 Merge  ["[s1,s2]"]                "String" "String"
-- XXX: ^ we lie about the input type here, because the generated function has split-out arguments
v4 = StreamVertex 4 Sink   ["mapM_ print"] "String" "IO ()"

graph = overlay (path [v1, v3]) $ path [v2, v3, v4]

parts = [[1],[2],[3,4]]

main = partitionGraph graph parts defaultOpts
