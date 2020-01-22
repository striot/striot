-- node3
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Nodes
import Control.Concurrent


sink1 :: Show a => Stream a -> IO ()
sink1 = mapM_ print


streamGraphFn :: Stream String -> Stream [String]
streamGraphFn n1 = let
    n2 = (\s -> streamMap (\st->"Incoming Message at Server: " ++ st) s) n1
    n3 = (\s -> streamWindow (chop 2) s) n2
    in n3


main :: IO ()
main = nodeSink streamGraphFn sink1 "9001"