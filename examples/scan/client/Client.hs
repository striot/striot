-- node1
import Network
import Control.Concurrent
import Striot.FunctionalIoTtypes hiding (id)
import Striot.FunctionalProcessing
import Striot.Nodes


src1 :: IO String
src1 = do
    threadDelay (1000*1000)
    putStrLn "Client1 sending 'foo'"
    return "foo"

main :: IO ()
main = do
    putStrLn "starting Client1...."
    nodeSource src1 id "haskellserver" "9001"
