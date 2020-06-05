import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph
import Language.Haskell.TH

source x = [| do
    let x' = $(litE (StringL x))
    threadDelay (1000*1000)
    putStrLn $ "sending "++ x'
    return x'
    |]

v1 = StreamVertex 1 Source [source "foo"]    "String" "String"
v2 = StreamVertex 2 Map    [[| id |]]        "String" "String"
v3 = StreamVertex 3 Source [source "bar"]    "String" "String"
v4 = StreamVertex 4 Map    [[| id |]]        "String" "String"
v5 = StreamVertex 5 Join   []                "String" "(String, String)"
v6 = StreamVertex 6 Sink   [[|mapM_ print|]] "(String, String)" "IO ()"

graph = overlay (path [v3, v4, v5]) $ path [v1, v2, v5, v6]

parts = [[1,2],[3,4],[5,6]]

main = partitionGraph graph parts defaultOpts
