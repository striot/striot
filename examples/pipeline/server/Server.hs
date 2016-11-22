import Network
import System.IO
import Data.List
import FunctionalProcessing
import FunctionalIoTtypes
import Nodes

main :: IO ()
main = nodeSink streamGraph1 printStream

streamGraph1 :: Stream String -> Stream [String]
streamGraph1 s = streamWindow (chop 2) $ streamMap (\st-> "Incoming Message at Server: " ++ st) s

printStream:: Show alpha => Stream alpha -> IO ()
printStream (h:t) = do
                      putStrLn $ show h
                      printStream t
