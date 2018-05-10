-- node3
import Network
import Control.Concurrent
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Nodes


sink1 :: Show a => [a] -> IO ()
sink1 = mapM_ (putStrLn . show)

streamGraphFn :: Stream String -> Stream String -> Stream [String]
streamGraphFn s1 s2 = let
       n1 = streamMerge [s1,s2]
       n2 = streamWindow ((chop 2)) n1
       in n2

main :: IO ()
main = nodeSink2 streamGraphFn sink1 "9001" "9002"
