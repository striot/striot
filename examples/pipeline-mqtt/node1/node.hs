-- node1
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Nodes
import Control.Concurrent


src1 = do
    threadDelay (1000*1000)
    return "Hello from Client!"

streamGraphFn :: Stream String -> Stream String
streamGraphFn n1 = let
    n2 = (\s -> streamMap (\st->st++st) s) n1
    in n2


main :: IO ()
main = do
       nodeSourceMqtt src1 streamGraphFn "node1" "mqtt" "1883"