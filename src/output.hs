import Striot.CompileIoT

stdImports = ["FunctionalIoTtypes","FunctionalProcessing","Nodes"]

s4:: StreamGraph
s4 = StreamGraph "pipeline" 6 [] [
       StreamOperation 1 [ ] Source    ["src1"]                                                                                "Stream String"          ["SourceFileContainingsrc1.hs"],
       StreamOperation 2 [1] Map       ["\\st-> st++st"]                                                                       "Stream String"          [],
       StreamOperation 3 [2] Map       ["\\st-> reverse st"]                                                                   "Stream String"          [],
       StreamOperation 4 [3] Map       ["\\st-> \"Incoming Message at Server: \" ++ st"]                                       "Stream String"          [],
       StreamOperation 5 [4] Window    ["(chop 2)"]                                                                            "Stream [String]"        [],
       StreamOperation 6 [5] Sink      ["print"]                                                                               ""                       []]

s4parts = generateCode s4 [(1,[1,2]),(2,[3]),(3,[4,5])] stdImports

main = do
    mapM_ (\(x,y) -> writeFile ((show x) ++ ".hs") y) (zip [1..] s4parts)
