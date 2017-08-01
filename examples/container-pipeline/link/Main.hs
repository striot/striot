import Network
--import Control.Concurrent
import System.IO
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes

listenPort  = 61616  :: PortNumber
connectPort = listenPort
connectHost = "sink" :: HostName

main :: IO ()
main = nodeLink streamGraph1 listenPort connectHost connectPort

streamGraph1 :: Stream String -> Stream String
streamGraph1 s = streamMap (\st-> reverse st) s
