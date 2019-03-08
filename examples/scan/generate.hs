import Striot.CompileIoT
import Algebra.Graph

source x = "do\n\
\    threadDelay (1000*1000)\n\
\    putStrLn \"sending '"++x++"'\"\n\
\    return \""++x++"\""

v1 = StreamVertex 1 Source [source "foo"] "String" "String"
v2 = StreamVertex 2 Map    ["id", "s"] "String" "String"

v5 = StreamVertex 5 Scan   ["(\\old _ -> old + 1)", "0", "s"] "String" "Int"
v6 = StreamVertex 6 Sink   ["mapM_ print"] "Int" "IO ()"

graph = path [v1, v2, v5, v6]

parts = [[1,2],[5,6]]

main = partitionGraph graph parts defaultOpts
