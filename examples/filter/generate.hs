{-
    demonstration of generating examples/merge via CompileIoT
 -}

import Striot.CompileIoT
import Striot.StreamGraph
import Algebra.Graph
import Language.Haskell.TH
import Control.Concurrent
import System.Random

opts = defaultOpts { rewrite = True }

source = [| do
    i <- getStdRandom (randomR (1,10)) :: IO Int
    threadDelay 1000000
    putStrLn $ "client sending " ++ (show i)
    return i
    |]

ssi =
 [ (Source , [source], "Int")
 , (Filter , [[| (>5) |]], "Int")
 , (Filter , [[| (<8) |]], "Int")
 , (Map    , [[| id   |]], "Int") -- work around bug #88
 , (Window , [[| chop 1 |]], "[Int]")
 , (Sink   , [[| mapM_ $ putStrLn . ("receiving "++) . show . value |]], "[String]")
 ]

graph = simpleStream ssi

parts = [[1,2,3,4],[5,6]]

main = do
    partitionGraph graph parts opts
