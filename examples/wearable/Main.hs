{-# LANGUAGE TemplateHaskell #-}

import Algebra.Graph -- path

import Striot.CompileIoT
import Striot.CompileIoT.Compose
import Striot.StreamGraph

import WearableExample

opts = defaultOpts { imports = imports defaultOpts ++
                        [ "System.Random" -- getStdGen
                        , "Data.Maybe" -- fromJust
                        , "WearableExample"
                        ]
                   , Striot.CompileIoT.preSource = Just "preSource"
                   , packages = []
                   }

wearable = let thr = 100 :: Int in path
  [ StreamVertex 1 (Source avgArrivalRate) [[| session1Input |]]
     "IO ()" "(Timestamp,PebbleMode60)" 25
  , StreamVertex 2 Window [[| pebbleTimes |]]
     "(Timestamp,PebbleMode60)" "[(Timestamp,PebbleMode60)]" 25
  , StreamVertex 3 Expand []
     "[(Timestamp,PebbleMode60)]" "(Timestamp,PebbleMode60)" 25
  , StreamVertex 4 Map [[| snd |]]
     "(Timestamp,PebbleMode60)" "PebbleMode60" 25

  , StreamVertex 5 (Filter vibeFrequency) [[| ((==0) . snd) |]]
    "PebbleMode60"  "PebbleMode60"  25

  -- edEvent
  , StreamVertex 6 Map [[| \((x,y,z),_) -> (x*x,y*y,z*z) |]]
    "PebbleMode60"  "(Int,Int,Int)" 25
  , StreamVertex 7 Map [[| \(x,y,z) -> intSqrt (x+y+z) |]]
    "(Int,Int,Int)" "Int" 25

  -- stepEvent
  , StreamVertex 8 (FilterAcc 0.5) [[| (\_ n-> n) |], [| 0 |], [| (\new last ->(last>thr) && (new<=thr)) |]]
    "Int" "Int" 25

  -- stepCount
  , StreamVertex 9 Window [[| chopTime 120 |]]
    "a" "[a]" 25
  , StreamVertex 10 Map [[| length |]]
    "[Int]" "Int" 25

  , StreamVertex 11 Sink [[| mapM_ print |]]
    "Int" "IO ()" 25
  ]
wearableParts = [[1,2,3,4,5,6,7],[8,9,10,11]]

main = do
    let g = wearable
    let p = wearableParts
    partitionGraph g p opts -- writes out Node*/*
    let partitionedGraph = createPartitions g p
    writeFile "compose.yml" (generateDockerCompose partitionedGraph)
