import Striot.CompileIoT
import Algebra.Graph
import System.FilePath --(</>)

imports = [ "Striot.FunctionalIoTtypes"
          , "Striot.FunctionalProcessing"
          , "Striot.Nodes"
          , "Control.Concurrent"
          , "System.Random (getStdRandom, randomR)"
          , "Control.Monad (replicateM)"]

-- 40 words picked randomly from the dictionary, 20 of which are prefixed
-- with # to simulate hashtags
randomWords = words "Angelica #Seine #sharpened sleeve consonance diabolically\
\ #bedlam #sharpener sentimentalizing amperage #quilt Ahmed #quadriceps Mia\
\ #burglaries constricted julienne #wavier #gnash #blowguns wiping somebodies\
\ nematode metaphorical Chablis #taproom disrespects #oddly ideograph rotunda\
\ #verdigrised #blazoned #murmuring #clover #saguaro #sideswipe faulted brought\
\ #Selkirk #Kshatriya"

source = "do\n\
\    indices <- replicateM 10 (getStdRandom (randomR (0,39)) :: IO Int)\n\
\    let s = unwords $ map (randomWords !!) indices in do\n\
\        threadDelay 1000000\n\
\        putStrLn $ \"sending \" ++ (show s)\n\
\        return s where\n\
\            randomWords = " ++ (show randomWords)


v1 = StreamVertex 1 Source [source] "String"
v2 = StreamVertex 2 Map    ["filter (('#'==).head) . words"] "[String]"

v5 = StreamVertex 5 Expand [""]                 "[String]"
v6 = StreamVertex 6 Sink   ["mapM_ print"] "String"

mergeEx :: StreamGraph
mergeEx = path [v1, v2, v5, v6]

parts = [[1,2],[5,6]]
partEx = generateCode mergeEx parts imports

writePart :: (Char, String) -> IO ()
writePart (x,y) = let
    bn = "node" ++ (x:[])
    fn = bn </> bn ++ ".hs"
    in
        writeFile fn y

main = mapM_ writePart (zip ['1'..] partEx)
