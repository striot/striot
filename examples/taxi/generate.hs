{-# LANGUAGE TemplateHaskell #-}
{-
    generate.hs for Taxi Q1
-}

import Striot.CompileIoT
import Striot.CompileIoT.Compose
import Striot.StreamGraph
import Striot.VizGraph
import Algebra.Graph.Export.Dot (export)

import Algebra.Graph
import Data.Time -- UTCTime(..)
import Data.Maybe (fromJust)
import Data.List.Split (splitOn)

opts = defaultOpts { imports = imports defaultOpts ++
                        [ "Taxi"
                        , "Data.Maybe"
                        , "Data.List.Split"
                        , "Data.Time" -- UTCTime(..)..
                        ]
                    , packages = []
                    , preSource = Just "preSource"
                    }
source = [| getLine >>= return . stringsToTrip . splitOn "," |]

topk' = [| \w -> (let lj = last w in (pickupTime lj, dropoffTime lj), topk 10 w) |]

filterDupes = [ [| \_ h -> Just h |]
              , [| Nothing |]
              , [| \h wacc -> case wacc of Nothing -> True; Just acc -> snd h /= snd acc |]
              ]

sink = [| mapM_ (print.show.fromJust.value) |]

taxiQ1 :: StreamGraph
taxiQ1 = simpleStream
    [ ((Source 1.2), [source],                         "Trip", 0)
    , (Window,    [[| tripTimes |]],                "[Trip]", 0.0001)
    , (Expand,    [],                               "Trip", 0.0001)
    , (Map,       [[| tripToJourney |]],            "Journey", 0.0001)
    , ((Filter 0.95),    [[| \j -> inRangeQ1 (start j) |]],"Journey", 0.00005)
    , ((Filter 0.95),    [[| \j -> inRangeQ1 (end j) |]],  "Journey", 0.00005)
    , (Window,    [[| slidingTime 1800000 |]],      "[Journey]", 0.0001)
    , (Map,       [topk'],                          "((UTCTime,UTCTime),[(Journey,Int)])", 0.01)
    , ((FilterAcc 0.1), filterDupes,                "((UTCTime,UTCTime),[(Journey,Int)])", 0.0001)
    , (Sink,      [sink],                           "((UTCTime,UTCTime),[(Journey,Int)])", 0.0001)
    ]

parts = [[1..7],[8],[9..10]]

main = do
    partitionGraph taxiQ1 parts opts
    writeFile "compose.yml" (generateDockerCompose (createPartitions taxiQ1 parts))
    let pg = createPartitions taxiQ1 parts
    writeGraph (export enumGraphStyle) taxiQ1 "taxiQ1.png"
    writeGraph partitionedGraphToDot pg "partitionedTaxiQ1.png"
