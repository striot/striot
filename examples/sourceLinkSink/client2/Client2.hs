import Network
import Control.Concurrent
import System.IO
import FunctionalProcessing
import FunctionalIoTtypes
import HandleConnections
import Data.Time (UTCTime,NominalDiffTime,getCurrentTime)

main :: IO ()
main = withSocketsDo $ do
        -- Sleep 5 seconds to ensure the server is up
         threadDelay (1 * 1000 * 1000)
         handle <- connectTo "haskellserver" (PortNumber 9001)
         now    <- getCurrentTime
--         let now = "2013-01-01 00:00:00"
         let payload = "Hello from Client2!"
--         hPutStr handle ("E," ++ show now ++ "," ++ payload)
         hPutStr handle (show (E now payload))
--         hPutStr handle ("V," ++ payload)
         hClose handle
         main --  Recurse forever
