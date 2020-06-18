{-
    demonstration of generating examples/merge via CompileIoT
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph

opts = defaultOpts { imports  = imports defaultOpts ++ ["System.Random"]
                   , rewrite = False
                   }

source = [| do
    i <- getStdRandom (randomR (1,10)) :: IO Int
    let s = show i in do
        threadDelay 1000000
        putStrLn $ "client sending " ++ s
        return s
    |]

v1 = StreamVertex 1 Source    [source]                                 "String" "String"
v2 = StreamVertex 2 Map       [[| id |]]                               "String" "String"
v3 = StreamVertex 3 FilterAcc [[| \_ e -> e |], [| "0" |], [| (/=) |]] "String" "String"
v4 = StreamVertex 4 Window    [[| chop 1 |]]                           "String" "[String]"
v5 = StreamVertex 5 Sink      [[| mapM_ $ putStrLn . ("receiving "++) . show . value |]]
                                                                       "[String]" "IO ()"

graph = path [v1, v2, v3, v4, v5]

parts = [[1,2],[3,4,5]]

main = partitionGraph graph parts opts
