import System.IO
import Data.List
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes
import Network.Socket (ServiceName)

listenPort = "9001" :: ServiceName

main :: IO ()
main = nodeSink streamGraph1 printStream listenPort

streamGraph1 :: Stream String -> Stream [String]
streamGraph1 s = streamWindow (chop 2) $ streamMap (\st-> "Incoming Message at Server: " ++ st) s

printStream:: Show alpha => Stream alpha -> IO ()
printStream = mapM_ (putStrLn.show)
