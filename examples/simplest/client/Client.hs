--import Network
import Control.Concurrent
import System.IO
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes
import Network.Socket (HostName, ServiceName)

-- processes source before sending it to another node
main :: IO ()
main = do
         threadDelay (1 * 1000 * 1000)
         nodeSource src streamGraph ("haskellserver"::HostName) ("9001"::ServiceName)

streamGraph :: Stream String -> Stream String
streamGraph s = streamMap id s where
    id = Prelude.id

src :: IO String
src = clockStreamNamed "Hello from Client!" 1000

clockStreamNamed :: String -> Int -> IO String -- returns the (next) payload to be added into an event and sent to a server
clockStreamNamed message period = do -- period is in ms
                                    threadDelay (period*1000)
                                    return message
