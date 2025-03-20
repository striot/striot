{-# LANGUAGE TemplateHaskell #-}
-- generate.hs for Taxi Q1

import Algebra.Graph
import Algebra.Graph.Export.Dot (export)
import Data.List.Split (splitOn)
import Data.Maybe (fromJust)
import Data.Time -- UTCTime(..)
import Striot.CompileIoT
import Striot.CompileIoT.Compose
import Striot.StreamGraph
import Striot.VizGraph

opts = defaultOpts { imports = imports defaultOpts ++
                        [ "Taxi"
                        , "Data.Maybe"
                        , "Data.List.Split"
                        , "Data.Time" ] -- UTCTime(..)..
                   , preSource = Just "preSource" }

source = [| getLine >>= return . stringsToTrip . splitOn "," |]

topk' = [| \w -> (let lj = last w in (pickupTime lj, dropoffTime lj), topk 10 w) |]

filterDupes = [ [| \_ h -> Just h |]
              , [| Nothing |]
              , [| \h wacc -> case wacc of Nothing -> True; Just acc -> snd h /= snd acc |]
              ]

sink = [| mapM_ (print.show.fromJust.value) |]

taxiQ1 :: StreamGraph
taxiQ1 = simpleStream
    [ ((Source 1.2),    [source],                    "Trip",      0)
    , (Window,          [[| tripTimes |]],           "[Trip]",    1)
    , (Expand,          [],                          "Trip",      1)
    , (Map,             [[| tripToJourney |]],       "Journey",   1)
    , ((Filter 0.95),   [[| inRangeQ1 .start |]],    "Journey",   20000)
    , ((Filter 0.95),   [[| inRangeQ1 .end   |]],    "Journey",   20000)
    , (Window,          [[| slidingTime 1800000 |]], "[Journey]", 10000)
    , (Map,             [topk'],                     "((UTCTime,UTCTime),[(Journey,Int)])", 100)
    , ((FilterAcc 0.1), filterDupes,                 "((UTCTime,UTCTime),[(Journey,Int)])", 10000)
    , (Sink,            [sink],                      "((UTCTime,UTCTime),[(Journey,Int)])", 10000)
    ]

parts = [[1..7],[8],[9..10]]

main = do
    partitionGraph taxiQ1 parts opts
    writeFile "compose.yml" (generateDockerCompose (createPartitions taxiQ1 parts))
    let pg = createPartitions taxiQ1 parts
    writeFile "taxiQ1.dot" $ export enumGraphStyle taxiQ1
    writeGraph (export enumGraphStyle) taxiQ1 "taxiQ1.png"
    writeGraph partitionedGraphToDot pg "partitionedTaxiQ1.png"
