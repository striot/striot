import Network
import System.IO
import Data.List
import FunctionalProcessing
import FunctionalIoTtypes
import Nodes

main :: IO ()
main = nodeSink2 streamGraph

streamGraph :: Stream String -> Stream String -> Stream [String]
streamGraph s1 s2 = streamWindow (chop 2) $ streamMap (\(v1,v2)-> "(" ++ v1 ++ "," ++ v2 ++ ")") $ streamJoin s1 s2
