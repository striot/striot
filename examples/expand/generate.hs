import Striot.CompileIoT
import Striot.StreamGraph

import Algebra.Graph
import Control.Monad (replicateM)
import System.Random
import Data.List.Split (splitOn)
import Language.Haskell.TH

-- 40 words picked randomly from the dictionary, 20 of which are prefixed
-- with # to simulate hashtags
randomWords = words "Angelica #Seine #sharpened sleeve consonance diabolically\
\ #bedlam #sharpener sentimentalizing amperage #quilt Ahmed #quadriceps Mia\
\ #burglaries constricted julienne #wavier #gnash #blowguns wiping somebodies\
\ nematode metaphorical Chablis #taproom disrespects #oddly ideograph rotunda\
\ #verdigrised #blazoned #murmuring #clover #saguaro #sideswipe faulted brought\
\ #Selkirk #Kshatriya"

source = [| do
    indices <- replicateM 10 (getStdRandom (randomR (0,39)) :: IO Int)
    let words = $(listE (map (litE . StringL) randomWords))
    let s = foldr1 (\w s -> w ++ " " ++ s)
          $ map (words !!)
          $ indices
    threadDelay 1000000
    putStrLn $ "sending " ++ (show s)
    return s
    |]

v1 = StreamVertex 1 Source [source]                              "String" "String"
v2 = StreamVertex 2 Map    [[| filter (('#'==).head) . splitOn " "|]] "String" "[String]"

v5 = StreamVertex 5 Expand []                                    "[String]" "String"
v6 = StreamVertex 6 Sink   [[| mapM_ print |]]                   "String" "String"

graph = path [v1, v2, v5, v6]

parts = [[1,2],[5,6]]

opts = defaultOpts { imports = imports defaultOpts ++
    [ "Control.Monad"
    , "System.Random"
    , "Data.List.Split"
    ]
}

main = partitionGraph graph parts opts
