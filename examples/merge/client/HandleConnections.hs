module HandleConnections where
import Network
import System.IO
import Data.List
import FunctionalIoTtypes
import Data.Time (UTCTime,NominalDiffTime)
import System.IO.Unsafe

handleConnections :: Read alpha => Show beta => Socket -> (Stream alpha -> Stream beta) -> IO ()
handleConnections sock streamOps = do
                                   stream <- readListFromSocket sock          -- read stream of Strings from socket
                                   let eventStream = map (\e->read e) stream
                                   let result = streamOps eventStream         -- process stream
                                   printStream result                                                                                                                      -- print stream

readListFromSocket :: Socket -> IO [String]
readListFromSocket sock = do {l <- go sock; return l}
  where
    go sock   = do (handle, host, port) <- accept sock
                   eventMsg             <- hGetLine handle                  
                   r                    <- System.IO.Unsafe.unsafeInterleaveIO (go sock)
                   return (eventMsg:r)

printStream :: Show alpha => Stream alpha -> IO ()
printStream (h:t) = do
                      putStrLn $ show h
                      printStream t
