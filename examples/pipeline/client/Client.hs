import Control.Concurrent
import System.IO
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes
import Network

import Data.Time (getCurrentTime)

portNum  = 9002::PortNumber
hostName = "haskellclient2"::HostName

main :: IO ()
main = do
         threadDelay (1 * 1000 * 1000)
         nodeSource src1 streamGraphid hostName portNum -- processes source before sending it to another node

streamGraph2 :: Stream String -> Stream String
streamGraph2 s = streamMap (\st-> st++st) s

src1:: IO String
src1 = clockStreamNamed "Hello from Client!" 200

clockStreamNamed:: String -> Int -> IO String -- returns the (next) payload to be added into an event and sent to a server
clockStreamNamed message period = do -- period is in ms
                                    threadDelay (period*1000)
                                    now <- getCurrentTime
                                    return $ show now

streamGraphid :: Stream String -> Stream String
streamGraphid = Prelude.id
