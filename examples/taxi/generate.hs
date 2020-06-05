{-
    generate.hs for Taxi Q1
-}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph
import Data.Time -- UTCTime(..)
import Data.Maybe (fromJust)
import Data.List.Split (splitOn)

opts = GenerateOpts { imports = imports defaultOpts ++
                        [ "Taxi"
                        , "Data.Time" -- UTCTime(..)..
                        ]
                    , packages = []
                    , preSource = Just "preSource"
                    , rewrite = True
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
    [ (Source,    [source],                         "Trip")
    , (Window,    [[| tripTimes |]],                "[Trip]")
    , (Expand,    [],                               "Trip")
    , (Map,       [[| tripToJourney |]],            "Journey")
    , (Filter,    [[| \j -> inRangeQ1 (start j) |]],"Journey")
    , (Filter,    [[| \j -> inRangeQ1 (end j) |]],  "Journey")
    , (Window,    [[| slidingTime 1800000 |]],      "[Journey]")
    , (Map,       [topk'],                          "((UTCTime,UTCTime),[(Journey,Int)])")
    , (FilterAcc, filterDupes,                      "((UTCTime,UTCTime),[(Journey,Int)])")
    , (Sink,      [sink],                           "((UTCTime,UTCTime),[(Journey,Int)])")
    ]

parts = [[1..7],[8],[9..10]]

main = partitionGraph taxiQ1 parts opts
