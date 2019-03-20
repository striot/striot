{-
    demonstration of generating examples/merge via CompileIoT
 -}

import Striot.CompileIoT
import Algebra.Graph

opts = defaultOpts { imports  = imports defaultOpts ++ ["System.Random"]
                   , packages = ["random"]
                   }

source = "do\n\
\    i <- getStdRandom (randomR (1,10)) :: IO Int\n\
\    let s = show i in do\n\
\        threadDelay 1000000\n\
\        putStrLn $ \"client sending \" ++ s\n\
\        return s"

v1 = StreamVertex 1 Source    [source]                                                 "String" "String"
v2 = StreamVertex 2 Map       ["id", "s"]                                              "String" "String"
v3 = StreamVertex 3 FilterAcc ["(\\_ e -> e)", "\"0\"", "(/=)", "s"]                   "String" "String"
v4 = StreamVertex 4 Window    ["(chop 1)", "s"]                                        "String" "[String]"
v5 = StreamVertex 5 Sink      ["mapM_ $ putStrLn . (\"receiving \"++) . show . value"] "[String]" "IO ()"

graph = path [v1, v2, v3, v4, v5]

parts = [[1,2],[3,4,5]]

main = partitionGraph graph parts opts
