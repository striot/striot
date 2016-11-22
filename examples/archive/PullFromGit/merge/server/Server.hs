-- tcp-server.hs

import Network
import System.IO
import Data.List
import FunctionalProcessing
import FunctionalIoTtypes
import HandleConnections

main :: IO ()
main = withSocketsDo $ do
         sock <- listenOn $ PortNumber 9001
         putStrLn "Starting server ..."
         handleConnectionsSink sock (\stream->streamWindow (chop 2) $ streamFilter (\s-> length s >10) $ streamMap (\s-> "Is mapped to:" ++ s) $ stream)
