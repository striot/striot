{-
    demonstration of generating examples/merge via CompileIoT
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph
import Language.Haskell.TH

source x y = [| do
    let z = $(litE (StringL x))
    threadDelay $(litE (IntegerL y))
    putStrLn $ "sending "++ z
    return z
    |]

v1 = StreamVertex 1 (Source 1) [source "foo" (1000*1000)] "String" "String" 0
v2 = StreamVertex 2 (Source 1) [source "bar"  (500*1000)] "String" "String" 0
v3 = StreamVertex 3 (Source 1) [source "baz"  (200*1000)] "String" "String" 0

v4 = StreamVertex 4 Merge [] "String" "String" 1
-- XXX: ^ we lie about the input type here, because the generated function has split-out arguments
v5 = StreamVertex 5 Sink [[| mapM_ print |]] "String" "IO ()" 0

graph = (overlays (map vertex [v1,v2,v3]) `connect` (vertex v4)) `overlay` path [v4,v5]

parts = [[1],[2],[3],[4,5]]

main = partitionGraph graph parts defaultOpts
