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

v1 = StreamVertex 1 Source [source "foo"] "String" "String"
v2 = StreamVertex 2 Map    [[| id |]] "String" "String"

v5 = StreamVertex 5 Scan   [[| \old _ -> old + 1 |], [|0|]] "String" "Int"
v6 = StreamVertex 6 Sink   [[| mapM_ print |]] "Int" "IO ()"

graph = path [v1, v2, v5, v6]

parts = [[1,2],[5,6]]

main = partitionGraph graph parts defaultOpts
