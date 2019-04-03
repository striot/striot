{-
    generate.hs for Taxi Q1
-}

import Striot.CompileIoT
import Algebra.Graph

import VizGraph

opts = GenerateOpts { imports = [ "Striot.FunctionalIoTtypes"
                                , "Striot.FunctionalProcessing"
                                , "Striot.Nodes"
                                , "Taxi"
                                , "Data.Time" -- UTCTime(..)..
                                , "Data.Maybe" -- fromJust
                                , "Data.List.Split" -- splitOn
                                , "Control.Concurrent"
                                ] -- threadDelay
                    , packages = []
                    , preSource = Just "preSource"
                    }
source = "do\n\
\   line <- getLine;\n\
\   return $ stringsToTrip $ splitOn \",\" line"

topk = "(\\w -> (let lj = last w in (pickupTime lj, dropoffTime lj), topk 10 w))"
filterDupes = ["(\\acc h -> if snd h == snd acc then acc else h)"
                               , "(fromJust (value (head s)))"
                               , "(\\h acc -> snd h /= snd acc)"
                               , "(tail s)"  ]
sink = "mapM_ (print.show.fromJust.value)"

taxiQ1 :: StreamGraph
taxiQ1 = simpleStream
    [ (Source,    [source],                             "Trip")
    , (Window,    ["tripTimes","s"],                    "[Trip]")
    , (Expand,    ["s"],                                "Trip")
    , (Map,       ["tripToJourney", "s"],               "Journey")
    , (Filter,    ["(\\j -> inRangeQ1 (start j))", "s"],"Journey")
    , (Filter,    ["(\\j -> inRangeQ1 (end j))", "s"],  "Journey")
    , (Window,    ["(slidingTime 1800000)", "s"],       "[Journey]")
    , (Map,       [topk, "s"],                          "((UTCTime,UTCTime),[(Journey,Int)])")
    , (FilterAcc, filterDupes,                          "((UTCTime,UTCTime),[(Journey,Int)])")
    , (Sink,      [sink],                               "((UTCTime,UTCTime),[(Journey,Int)])")
    ]

parts = [[1..7],[8],[9..10]]

main = partitionGraph taxiQ1 parts opts
