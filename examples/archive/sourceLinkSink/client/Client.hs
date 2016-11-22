import Network
import Control.Concurrent
import System.IO
import FunctionalProcessing
import FunctionalIoTtypes
import HandleConnections

main :: IO ()
main = do
         threadDelay (1 * 1000 * 1000)
         sendSource src1

src1:: IO String
src1 = clockStreamNamed "Hello from clockStreamNamed" 1000

clockStreamNamed:: String -> Int -> IO String -- returns the (next) payload to be added into an event and sent to a server
clockStreamNamed message period = do -- period is in ms                                   
                                    threadDelay (period*1000)
                                    return message                                 
