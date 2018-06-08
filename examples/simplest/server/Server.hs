import Network.Socket (ServiceName)
import System.IO
import Data.List
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes

main :: IO ()
main = nodeSink streamGraph1 printStream ("9001"::ServiceName)

streamGraph1 :: Stream String -> Stream [String]
streamGraph1 s = streamWindow (chop 1) s

printStream:: Show alpha => Stream alpha -> IO ()
printStream []  = return ()
printStream (h:t) = do
                      putStrLn $ show h
                      printStream t
