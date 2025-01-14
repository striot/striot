{-# LANGUAGE TemplateHaskell #-}

import Striot.CompileIoT
import Striot.CompileIoT.Compose

import WearableExample

parts = [[1,2,3],[4,5,6,7],[8]]

opts = defaultOpts { imports = imports defaultOpts ++
                        [ "System.Random" -- getStdGen
                        , "Data.Maybe" -- fromJust
                        , "WearableExample"
                        ]
                   , packages = []
                   }

main = do
    partitionGraph graph parts opts
    let partitionedGraph = createPartitions graph parts
    writeFile "compose.yml" (generateDockerCompose partitionedGraph)
