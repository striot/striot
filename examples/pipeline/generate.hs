{-# LANGUAGE TemplateHaskell #-}
{-
    demonstration of generating examples/pipeline via CompileIoT
 -}

import Striot.CompileIoT
import Striot.CompileIoT.Compose
import Striot.StreamGraph
import Algebra.Graph

source = [| threadDelay 1000000 >> return "Hello from Client!" |]

graph = path
 [ StreamVertex 1 (Source 1) [source] "IO ()"  "String" 0
 , StreamVertex 2 Map    [[| \st->st++st |]] "String" "String" 1
 , StreamVertex 3 Map    [[| reverse |]] "String" "String" 1
 , StreamVertex 4 Map    [[| ("Incoming: "++) |]] "String" "String" 1
 , StreamVertex 5 Window [[| chop 2 |]] "String" "[String]" 1
 , StreamVertex 6 Sink   [[| mapM_ print |]] "[String]" "IO ()" 0
 ]

pmap = [[1,2],[3],[4,5,6]]
main = do
    partitionGraph graph pmap defaultOpts
    writeFile "compose.yml" (generateDockerCompose
                             (createPartitions graph pmap))
