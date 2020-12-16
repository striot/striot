{-
    demonstration of generating examples/merge via CompileIoT
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph
import Language.Haskell.TH
import Control.Concurrent
import System.Random

opts = defaultOpts { imports = imports defaultOpts ++ [ "System.Random" ] }

source = [| do
    i <- getStdRandom (randomR (1,10)) :: IO Int
    threadDelay 1000000
    putStrLn $ "client sending " ++ (show i)
    return i
    |]

ssi =
 [ ((Source 1) , [source], "Int", 0)
 , ((Filter 0.5), [[| (>5) |]], "Int", 1)
 , ((Filter 0.5), [[| (<8) |]], "Int", 1)
 , (Map    , [[| id   |]], "Int", 1) -- work around bug #88
 , (Window , [[| chop 1 |]], "[Int]", 1)
 , (Sink   , [[| mapM_ $ putStrLn . ("receiving "++) . show . value |]], "[String]", 0)
 ]

graph = simpleStream ssi

parts = [[1,2,3,4],[5,6]]

main = partitionGraph graph parts opts
