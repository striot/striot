import Striot.CompileIoT
import Algebra.Graph

source x = "do\n\
\    threadDelay (1000*1000)\n\
\    putStrLn \"sending '"++x++"'\"\n\
\    return \""++x++"\""

v1 = StreamVertex 1 Source [source "foo"]  "String" "String"
v2 = StreamVertex 2 Map    ["id", "s"]     "String" "String"
v3 = StreamVertex 3 Source [source "bar"]  "String" "String"
v4 = StreamVertex 4 Map    ["id", "s"]     "String" "String"
v5 = StreamVertex 5 Join   ["s1", "s2"]    "String" "(String, String)"
v6 = StreamVertex 6 Sink   ["mapM_ print"] "(String, String)" "IO ()"

graph = overlay (path [v3, v4, v5]) $ path [v1, v2, v5, v6]

parts = [[1,2],[3,4],[5,6]]

main = partitionGraph graph parts defaultOpts
