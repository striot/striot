import Network.Socket (ServiceName)
import System.IO
import Data.List
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes

main :: IO ()
main = nodeSink streamGraph1 printStream ("9001"::ServiceName)

streamGraph1 :: Stream String -> Stream Int
streamGraph1 s = streamScan (\old _ -> old + 1) 0 s

printStream:: Show alpha => Stream alpha -> IO ()
printStream = mapM_ (putStrLn.show)
