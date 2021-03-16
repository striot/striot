{-# LANGUAGE TemplateHaskell #-}
{-
    demonstration of streamMerge starvation (GitHub #125)
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Striot.VizGraph
import Algebra.Graph

v1 = StreamVertex 1 (Source 1) [[| do
                                putStrLn "sending 3"
                                threadDelay 1000000
                                return 3
                                 |]] "Int" "Int" 0
v2 = StreamVertex 2 (Filter 0) [[| (>3)     |]] "Int" "Int" 0 -- rejects all input
                                               
v3 = StreamVertex 3 (Source 1) [[| do
                                putStrLn "sending 5"
                                threadDelay 1000000
                                return 5
                                 |]] "Int" "Int" 0
v4 = StreamVertex 4 (Filter 0) [[| (>3)     |]] "Int" "Int" 0 -- accepts all input

v5 = StreamVertex 5 Merge [] "Int" "Int" 0
v6 = StreamVertex 6 Sink [[| print |]] "Int" "IO ()" 0


graph = path [v3,v4,v5] `overlay` path [v1,v2,v5,v6]

parts = [[1,2],[3,4],[5,6]]

main = partitionGraph graph parts defaultOpts
