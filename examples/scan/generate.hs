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

v1 = StreamVertex 1 Source [source "foo"] "String" "String" 0
v2 = StreamVertex 2 Map    [[| id |]] "String" "String" 1

v5 = StreamVertex 5 Scan   [[| \old _ -> old + 1 |], [|0|]] "String" "Int" 1
v6 = StreamVertex 6 Sink   [[| mapM_ print |]] "Int" "IO ()" 0

graph = path [v1, v2, v5, v6]

parts = [[1,2],[5,6]]

main = partitionGraph graph parts defaultOpts
