-- node2
import Network
import Control.Concurrent
import Striot.FunctionalIoTtypes hiding (id)
import Striot.FunctionalProcessing
import Striot.Nodes


src1 :: IO String
src1 = do
    threadDelay (1000*1000)
    putStrLn "Client2 sending 'bar'"
    return "bar"

main :: IO ()
main = do
    putStrLn "starting Client2...."
    nodeSource src1 id "node3" "9002"
