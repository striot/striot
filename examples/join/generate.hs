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

v1 = StreamVertex 1 (Source 1) [source "foo"]    "String" "String" 0
v2 = StreamVertex 2 Map    [[| id |]]        "String" "String" 1
v3 = StreamVertex 3 (Source 1) [source "bar"]    "String" "String" 0
v4 = StreamVertex 4 Map    [[| id |]]        "String" "String" 1
v5 = StreamVertex 5 Join   []                "String" "(String, String)" 1
v6 = StreamVertex 6 Sink   [[|mapM_ print|]] "(String, String)" "IO ()" 0

graph = overlay (path [v3, v4, v5]) $ path [v1, v2, v5, v6]

parts = [[1,2],[3,4],[5,6]]

main = partitionGraph graph parts defaultOpts
