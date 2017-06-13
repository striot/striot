import Network
import System.IO
import Data.List
import FunctionalProcessing
import FunctionalIoTtypes
import Nodes

main :: IO ()
main = nodeSink streamGraph1 printStream (9001::PortNumber)

streamGraph1 :: Stream String -> Stream [String]
streamGraph1 s = streamWindow (chop 1) s

printStream:: Show alpha => Stream alpha -> IO ()
printStream []  = return ()
printStream (h:t) = do
                      putStrLn $ show h
                      printStream t
