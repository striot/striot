import Network
import Control.Concurrent
import System.IO
import FunctionalProcessing
import FunctionalIoTtypes
import HandleConnections
import Data.Time (UTCTime,NominalDiffTime,getCurrentTime)

main :: IO ()
main = withSocketsDo $ do
        -- Sleep to ensure the server is up
         threadDelay (1 * 1000 * 1000)                          -- boilerplate (parameterised)
         handle <- connectTo "haskellserver" (PortNumber 9001)  -- boilerplate (parameterised)
         now    <- getCurrentTime
         let payload = "Hello From Client 1"
         let msg = show (E now payload)
         hPutStr handle msg                                     -- boilerplate
         hClose handle                                          -- boilerplate
         main --  Recurse forever

--streamSource:: String -> Int -> IO ()
--streamSource server port =  do
--                              handle <- connectTo server (PortNumber port)
--                              now    <- getCurrentTime
--                              let payload = "Hello From Client 1"
--                              hPutStr handle (show (E now payload))
--                              hClose handle
--                              streamSource
