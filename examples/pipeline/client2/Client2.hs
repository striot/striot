--import Control.Concurrent
import System.IO
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes
import Network.Socket (HostName, ServiceName)

listenPort =  "9001" :: ServiceName
connectPort = "9001" :: ServiceName
connectHost = "haskellserver" :: HostName

main :: IO ()
main = nodeLink streamGraph1 listenPort connectHost connectPort

streamGraph1 :: Stream String -> Stream String
streamGraph1 s = streamMap (\st-> reverse st) s
