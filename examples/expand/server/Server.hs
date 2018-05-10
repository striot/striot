import Network.Socket (ServiceName)
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes

main :: IO ()
main = nodeSink streamGraph printStream ("9001"::ServiceName)

streamGraph :: Stream [String] -> Stream String
streamGraph = streamExpand

printStream:: Show alpha => Stream alpha -> IO ()
printStream = mapM_ (\s -> putStrLn $ "receiving " ++ (show (value s)))
