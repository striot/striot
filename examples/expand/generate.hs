import Striot.CompileIoT
import Striot.StreamGraph

import Algebra.Graph
import Control.Monad (replicateM)
import System.Random
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
    let randomWords' = $(listE (map (litE . StringL) randomWords))
    let s = unwords
          $ map (randomWords' !!)
          $ indices
    threadDelay 1000000
    putStrLn $ "sending " ++ (show s)
    return s
    |]

v1 = StreamVertex 1 (Source 1) [source]                              "String" "String" 0
v2 = StreamVertex 2 Map    [[| filter (('#'==).head) . words |]] "String" "[String]" 1

v5 = StreamVertex 5 Expand []                                    "[String]" "String" 1
v6 = StreamVertex 6 Sink   [[| mapM_ print |]]                   "String" "String" 0

graph = path [v1, v2, v5, v6]

parts = [[1,2],[5,6]]

opts = defaultOpts { imports = imports defaultOpts ++
    [ "Control.Monad"
    , "System.Random"
    ]
}

main = partitionGraph graph parts opts
