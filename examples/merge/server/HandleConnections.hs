module HandleConnections where
import Network
import System.IO
import Control.Concurrent
import Data.List
import FunctionalIoTtypes
import Data.Time (UTCTime,NominalDiffTime)
import System.IO.Unsafe

handleConnectionsSink :: Read alpha => Show beta => Socket -> (Stream alpha -> Stream beta) -> IO ()
handleConnectionsSink sock streamOps = do
                                         stream <- readListFromSocket sock          -- read stream of Strings from socket
                                         let eventStream = map (\e->read e) stream
                                         let result = streamOps eventStream         -- process stream
                                         printStream result -- to print stream

readListFromSocket :: Socket -> IO [String]
readListFromSocket sock = do {l <- go sock; return l}
  where
    go sock   = do (handle, host, port) <- accept sock
                   eventMsg             <- hGetLine handle                  
                   r                    <- System.IO.Unsafe.unsafeInterleaveIO (go sock)
                   return (eventMsg:r)

printStream:: Show alpha => Stream alpha -> IO ()
printStream (h:t) = do
                      putStrLn $ show h
                      printStream t

--handleConnectionsLink :: Read alpha => Show beta => Socket -> (Stream alpha -> Stream beta) -> String -> Int -> IO ()
--handleConnectionsLink sock streamOps server port = do
--                                                     stream <- readListFromSocket sock          -- read stream of Strings from socket
--                                                     let eventStream = map (\e->read e) stream
--                                                     let result = streamOps eventStream         -- process stream
--                                                     sendStream result server port              -- send stream to next partition

--sendStream:: Stream alpha -> String -> Int -> IO ()
--sendStream (h:t) server port = withSocketsDo $ do
--                                 -- Sleep to ensure the server is up
--                                 threadDelay (1 * 1000 * 1000)   -- how to get rid of this: need to move to start-up              
--                                 handle <- connectTo server (PortNumber port)  
--                                hPutStr handle (show h)                                     
--                                 hClose handle                                 
--                                 sendStream t server port --  Recurse forever

------------ may not work! --------

--handleConnectionsN :: Read alpha => Show beta => [Socket] -> (Stream alpha -> Stream beta) -> IO () -- what is the signature of a partition that reads in multiple streams?
--handleConnectionsN socks streamOps = do
--                            streams <- map readListFromSocket socks
--                            let eventStreams = map (map (\e->read e)) streams
--                            let result = streamOps eventStreams -- could pick them out by index?
--                            printStream result