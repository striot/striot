{-
    demonstration of generating examples/merge via CompileIoT
 -}

import Striot.CompileIoT
import Algebra.Graph
import System.FilePath --(</>)

imports = ["Striot.FunctionalIoTtypes", "Striot.FunctionalProcessing", "Striot.Nodes", "Control.Concurrent", "System.Random"]

source = "do\n\
\    i <- getStdRandom (randomR (1,10)) :: IO Int\n\
\    let s = show i in do\n\
\        threadDelay 1000000\n\
\        putStrLn $ \"client sending \" ++ s\n\
\        return s"

v1 = StreamVertex 1 Source    [source]                                                 "String"
v2 = StreamVertex 2 Map       ["Prelude.id"]                                           "String"
v3 = StreamVertex 3 FilterAcc ["(\\_ s -> s)", "\"0\"", "(/=)"]                        "String"
v4 = StreamVertex 4 Window    ["chop 1"]                                               "[String]"
v5 = StreamVertex 5 Sink      ["mapM_ $ putStrLn . (\"receiving \"++) . show . value"] "[String]"

mergeEx :: StreamGraph
mergeEx = path [v1, v2, v3, v4, v5]

parts = [[1,2],[3,4,5]]
partEx = generateCode mergeEx parts imports

writePart :: (Char, String) -> IO ()
writePart (x,y) = let
    bn = "node" ++ (x:[])
    fn = bn </> bn ++ ".hs"
    in
        writeFile fn y

main = mapM_ writePart (zip ['1'..] partEx)
