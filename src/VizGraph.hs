module VizGraph where
import Data.List
import Data.GraphViz
import CompileIoT

-- https://hackage.haskell.org/package/graphviz-2999.18.1.2/docs/Data-GraphViz-Attributes-Complete.html#t:Attributes
graph :: DotGraph Int
graph = graphElemsToDot graphParams gnodes gedges

graphParams :: GraphvizParams Int String Bool () String
graphParams = defaultParams

gnodes :: [(Int, String)]
gnodes = map (\x -> (x,"")) [1..4]
gedges:: [(Int, Int, Bool)]
gedges = [ (1, 3, True)
         , (1, 4, True)
         , (2, 3, True)
         , (2, 4, True)
         , (3, 4, True)]

-- Execute main to output the graph:
main = addExtension (runGraphviz graph) Png "graph"

-------------

streamGraphToDotGraph:: StreamGraph -> (GraphvizParams Int String Bool () String) -> DotGraph Int
streamGraphToDotGraph sg params = graphElemsToDot params
                                                  (map (\op -> (opid op,show $ operator op)) (operations sg)) -- nodes
                                                  (map (\(sourceNode,(destNode,destPort)) -> (sourceNode,destNode,True)) (CompileIoT.graphEdges sg))

main2 = addExtension (runGraphviz (streamGraphToDotGraph s1 defaultParams)) Png "graphS1"                                                 
                                                  
main3 = streamGraphToDotGraph s1 defaultParams
