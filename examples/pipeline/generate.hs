{-
    demonstration of generating examples/pipeline via CompileIoT
 -}

import Striot.CompileIoT
import Algebra.Graph
import System.FilePath --(</>)

imports = ["Striot.FunctionalIoTtypes", "Striot.FunctionalProcessing", "Striot.Nodes", "Control.Concurrent"]

pipeEx :: StreamGraph
pipeEx = path
    [ StreamVertex 1 Source ["do\n    threadDelay (1000*1000)\n    return \"Hello from Client!\""] "String" "String"
    , StreamVertex 2 Map    ["(\\st->st++st)", "s"]                                                "String" "String"
    , StreamVertex 3 Map    ["reverse", "s"]                                                       "String" "String"
    , StreamVertex 4 Map    ["(\\st->\"Incoming Message at Server: \" ++ st)", "s"]                "String" "String"
    , StreamVertex 5 Window ["(chop 2)", "s"]                                                      "String" "[String]"
    , StreamVertex 6 Sink   ["mapM_ print"]                                                        "[String]" "IO ()"
    ]

partEx = generateCode pipeEx [[1,2],[3],[4,5,6]] imports

writePart :: (Char, String) -> IO ()
writePart (x,y) = let
    bn = "node" ++ (x:[])
    fn = bn </> bn ++ ".hs"
    in
        writeFile fn y

main = mapM_ writePart (zip ['1'..] partEx)
