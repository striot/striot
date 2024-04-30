{-# LANGUAGE TemplateHaskell #-}

import Algebra.Graph -- path

import Striot.CompileIoT
import Striot.CompileIoT.Compose
import Striot.StreamGraph

import WearableExample

parts = [[1,2,3],[4,5,6,7],[8]]

opts = defaultOpts { imports = imports defaultOpts ++
                        [ "System.Random" -- getStdGen
                        , "Data.Maybe" -- fromJust
                        , "WearableExample"
                        ]
                   , Striot.CompileIoT.preSource = Just "preSource"
                   , packages = []
                   }

-- example graph which reports the arrival rate in Hz
calcArrivalRate = path
  [ StreamVertex 1 (Source 25) [[| sampleInput |]]   "IO ()"          "PebbleMode60"   25
  , StreamVertex 2 Window      [[| chopTime 1000 |]] "PebbleMode60"   "[PebbleMode60]" 25
  , StreamVertex 3 Map         [[| length |]]        "[PebbleMode60]" "Int"            25
  , StreamVertex 4 Sink        [[| mapM_ print |]]   "Int"            "IO ()"          25
  ]

useCsvData = path
  [ StreamVertex 1 (Source 25) [[| source |]]        "IO ()"                      "[(Timestamp,PebbleMode60)]" 25
  , StreamVertex 2 Expand      []                    "[(Timestamp,PebbleMode60)]" "(Timestamp,PebbleMode60)"   25
  , StreamVertex 3 Window      [[| pebbleTimes |]]   "(Timestamp,PebbleMode60)"   "[(Timestamp,PebbleMode60)]" 25
  , StreamVertex 4 Expand      []                    "[(Timestamp,PebbleMode60)]" "(Timestamp,PebbleMode60)"   25
  , StreamVertex 5 Map         [[| snd |]]           "(Timestamp,PebbleMode60)"   "PebbleMode60"               25
  , StreamVertex 6 Sink        [[| mapM_ print |]]   "PebbleMode60"               "IO ()"                      25
  ]


main = do
    partitionGraph useCsvData [[1,2,3,4],[5,6]] opts
    let partitionedGraph = createPartitions graph parts
    writeFile "compose.yml" (generateDockerCompose partitionedGraph)
