--import Network
import Control.Concurrent
import System.IO
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes
import Network.Socket (HostName, ServiceName)
import System.Random

main :: IO ()
main = do
         threadDelay 1000000
         nodeSource src Prelude.id ("haskellserver"::HostName) ("9001"::ServiceName)

src :: IO String
src = do
    i <- getStdRandom (randomR (1,10)) :: IO Int
    let s = show i in do
        threadDelay 1000000
        putStrLn $ "client sending " ++ s
        return s
