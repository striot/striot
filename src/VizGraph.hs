module VizGraph(drawPartitionedStreamGraph,drawStreamGraph) where
import Data.List
import Striot.CompileIoT as CompileIoT
import Data.GraphViz
import Data.Text.Lazy (Text, pack, unpack)
import Data.Graph.Inductive (Gr, mkGraph)
import qualified Data.Map as Map
import Data.GraphViz.Printing (toDot, renderDot)
import Data.GraphViz.Attributes.Complete



-- https://hackage.haskell.org/package/graphviz-2999.18.1.2/docs/Data-GraphViz-Attributes-Complete.html#t:Attributes
-- https://hackage.haskell.org/package/graphviz-2999.18.1.2/docs/Data-GraphViz.html
-- http://haroldcarr.com/posts/2014-02-28-using-graphviz-via-haskell.html

graph :: DotGraph Int
graph = graphElemsToDot graphParams gnodes gedges

graphParams :: GraphvizParams Int String Bool () String
graphParams = defaultParams

gnodes :: [(Int, String)]
gnodes = map (\x -> (x,"Node")) [1..4]
gedges:: [(Int, Int, Bool)]
gedges = [ (1, 3, True)
         , (1, 4, True)
         , (2, 3, True)
         , (2, 4, True)
         , (3, 4, True)]

-- Execute main to output the graph:
main = addExtension (runGraphviz graph) Png "graph"

-------------

streamGraphToDotGraph:: StreamGraph -> (GraphvizParams Int String String () String) -> Data.GraphViz.DotGraph Int
streamGraphToDotGraph sg params = graphElemsToDot params
                                                  (map (\op -> (opid op,(show $ operator op) ++ " " ++ CompileIoT.printParams (parameters op))) (operations sg)) -- nodes -- could remove printParams if operation only is needed
                                                  (map (\(sourceNode,(destNode,destPort),oType) -> (sourceNode,destNode,oType)) (CompileIoT.graphEdgesWithTypes sg)) -- edges

---------
-- https://hackage.haskell.org/package/graphviz-2999.18.0.0/docs/Data-GraphViz.html#t:GraphvizParams
myParams :: GraphvizParams Int String String () String
myParams = nonClusteredParams {
--4. Let the graphing engine know that we want the edges to be directed arrows
-- as follows:
              isDirected = True
--5. Set our own global attributes for a graph, node, and edge appearance as follows:
            , globalAttributes = [myGraphAttrs, myNodeAttrs, myEdgeAttrs]
--6. Format nodes in our own way as follows:
            , fmtNode = myFN
--7. Format edges in our own way as follows:
            , fmtEdge = myFE
            }
--8. Define the customizations as shown in the following code snippet:
           where myGraphAttrs = GraphAttrs [ -- RankDir FromLeft
                                            BgColor [toWColor White] ]
                 myNodeAttrs =  NodeAttrs [ Shape BoxShape
                                          , FillColor [toWColor White]
                                          , Style [SItem Filled []] ]
                 myEdgeAttrs =  EdgeAttrs [ Weight (Int 10)
                                          , Color [toWColor Black]
                                          , FontColor (toColor Black) ]
                 myFN (n,l)   = [(Label . StrLabel) (Data.Text.Lazy.pack (drop 6 l))] -- pack converts the strings used in streamGraph to Text used by GraphvizParams 
                                                                                      -- drop 6 removes the "Stream " prefix
                 myFE (f,t,l) = [(Label . StrLabel) (Data.Text.Lazy.pack (' ':(drop 7 l)))] -- drop 7 removes the "Stream" prefix from the operators
                 
-- http://goto.ucsd.edu/~rjhala/llvm-haskell/doc/html/llvm-analysis/src/LLVM-Analysis-CFG.html gives a helpful example
clusteredParams ::  GraphvizParams Int String String Int String
clusteredParams = defaultParams {
--4. Let the graphing engine know that we want the edges to be directed arrows
-- as follows:
              isDirected = True
--5. Set our own global attributes for a graph, node, and edge appearance as follows:
            , globalAttributes = [myGraphAttrs, myNodeAttrs, myEdgeAttrs]
--6. Format nodes in our own way as follows:
            , fmtNode    = myFN
--7. Format edges in our own way as follows:
            , fmtEdge    = myFE
-- Format clusters:
            , clusterBy  = clustBy
            , clusterID  = Num . Int
            , fmtCluster = myFC
            }
--8. Define the customizations as shown in the following code snippet:
           where myGraphAttrs  = GraphAttrs [ -- RankDir FromLeft
                                            BgColor [toWColor White] ]
                 myNodeAttrs   =  NodeAttrs [ Shape BoxShape
                                          , FillColor [toWColor White]
                                          , Style [SItem Filled []] ]
                 myEdgeAttrs   =  EdgeAttrs [ Weight (Int 10)
                                          , Color [toWColor Black]
                                          , FontColor (toColor Black) ]
                 myFN (n,l)    = [(Label . StrLabel) (Data.Text.Lazy.pack (drop 6 l))] -- pack converts the strings used in streamGraph to Text used by GraphvizParams 
                                                                                      -- drop 6 removes the "Stream " prefix
                 myFE (f,t,l)  = [(Label . StrLabel) (Data.Text.Lazy.pack (' ':(drop 7 l)))] -- drop 7 removes the "Stream" prefix from the operators    
                 clustBy (n,l) = C (n `mod` 2) $ N (n,l)
                 myFC m        = [GraphAttrs [toLabel $ "n == " ++ show m ++ " (mod 2)"]]


clStreamGraphToDotGraph:: StreamGraph -> (GraphvizParams Int String String Int String) -> Data.GraphViz.DotGraph Int
clStreamGraphToDotGraph sg params = graphElemsToDot params
                                                  (map (\op -> (opid op,(show $ operator op) ++ " " ++ CompileIoT.printParams (parameters op))) (operations sg)) -- nodes -- could remove printParams if operation only is needed
                                                  (map (\(sourceNode,(destNode,destPort),oType) -> (sourceNode,destNode,oType)) (CompileIoT.graphEdgesWithTypes sg)) -- edges

clusteredParams2 :: Map.Map Id Partition -> GraphvizParams Int String String Int String
clusteredParams2 idToPart = defaultParams {
              isDirected = True
            , globalAttributes = [myGraphAttrs, myNodeAttrs, myEdgeAttrs]
            , fmtNode    = myFN
            , fmtEdge    = myFE
            , clusterBy  = clustBy
            , clusterID  = Num . Int
            , fmtCluster = myFC
            }
           where myGraphAttrs  = GraphAttrs [ -- RankDir FromLeft
                                            BgColor [toWColor White] ]
                 myNodeAttrs   =  NodeAttrs [ Shape BoxShape
                                          , FillColor [toWColor White]
                                          , Style [SItem Filled []] ]
                 myEdgeAttrs   =  EdgeAttrs [ Weight (Int 10)
                                          , Color [toWColor Black]
                                          , FontColor (toColor Black) ]
                 myFN (n,l)    = [(Label . StrLabel) (Data.Text.Lazy.pack (drop 6 l))] -- pack converts the strings used in streamGraph to Text used by GraphvizParams 
                                                                                       -- drop 6 removes the "Stream " prefix
                 myFE (f,t,l)  = [(Label . StrLabel) (Data.Text.Lazy.pack (' ':(drop 7 l)))] -- drop 7 removes the "Stream" prefix from the operators    
                 clustBy (n,l) = C (idToPart Map.! n) $ N (n,l)
                 myFC p        = [GraphAttrs [toLabel $ "Partition " ++ show p]]

drawPartitionedStreamGraph::  StreamGraph -> [(Partition,[Id])] -> String -> IO FilePath
drawPartitionedStreamGraph sg parts filename = addExtension (runGraphviz (clStreamGraphToDotGraph sg (clusteredParams2 (mkPartMap parts)))) Png filename

mkPartMap:: [(Partition,[Id])] -> Map.Map Id Partition
mkPartMap ps = foldl (\m (i,p) -> Map.insert i p m) Map.empty (concatMap (\(p,ids)->[(i,p)|i<-ids]) ps)

drawStreamGraph:: StreamGraph -> String -> IO FilePath
drawStreamGraph sg filename = addExtension (runGraphviz (streamGraphToDotGraph sg myParams)) Png filename 

