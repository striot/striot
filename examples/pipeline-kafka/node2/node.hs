-- node2
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Nodes
import Control.Concurrent


streamGraphFn :: Stream String -> Stream String
streamGraphFn n1 = let
    n2 = (\s -> streamMap reverse s) n1
    in n2


main :: IO ()
main = nodeLinkKafka streamGraphFn "node2" "kafka" "9092" "node3" "9001"