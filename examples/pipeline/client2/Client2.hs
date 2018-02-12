--import Control.Concurrent
import System.IO
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes
import Network

listenPort =  9002 :: PortNumber
connectPort = 9001 :: PortNumber
connectHost = "haskellserver" :: HostName

main :: IO ()
main = nodeLink streamGraphid listenPort connectHost connectPort

streamGraph1 :: Stream String -> Stream String
streamGraph1 s = streamMap (\st-> reverse st) s

streamGraphWin :: Stream String -> Stream [String]
streamGraphWin = streamWindow (slidingTime windowLength)

streamGraphid :: Stream String -> Stream String
streamGraphid = Prelude.id

windowLength :: Int
windowLength = 1000
