import Striot.CompileIoT
import Algebra.Graph
import System.FilePath --(</>)

imports = ["Striot.FunctionalIoTtypes", "Striot.FunctionalProcessing", "Striot.Nodes", "Control.Concurrent"]

source x = "do\n\
\    threadDelay (1000*1000)\n\
\    putStrLn \"sending '"++x++"'\"\n\
\    return \""++x++"\""

v1 = StreamVertex 1 Source [source "foo"]              "String"
v2 = StreamVertex 2 Map    ["Prelude.id"]              "String"
v3 = StreamVertex 3 Source [source "bar"]              "String"
v4 = StreamVertex 4 Map    ["Prelude.id"]              "String"
v5 = StreamVertex 5 Join   ["[n1,n2]"]                 "String"
v6 = StreamVertex 6 Sink   ["mapM_ print"] "(String, String)"

joinEx :: StreamGraph
joinEx = overlay (path [v3, v4, v5]) $ path [v1, v2, v5, v6]

parts = [[1,2],[3,4],[5,6]]
partEx = generateCode joinEx parts imports

writePart :: (Char, String) -> IO ()
writePart (x,y) = let
    bn = "node" ++ (x:[])
    fn = bn </> bn ++ ".hs"
    in
        writeFile fn y

main = mapM_ writePart (zip ['1'..] partEx)
