module HandleConnections where
import Network
import System.IO
import Control.Concurrent
import Data.List
import FunctionalIoTtypes
--import Data.Time (UTCTime,NominalDiffTime)
import System.IO.Unsafe
import Data.Time (UTCTime,NominalDiffTime,getCurrentTime)

portNumInput1:: PortNumber
portNumInput1 = 9001::PortNumber -- needs fixing

portNumOutput:: PortNumber
portNumOutput = 9001::PortNumber -- needs fixing

hostNameOutput = "haskellserver"::HostName

handleConnectionsSink:: Read alpha => Show beta => (Stream alpha -> Stream beta) -> IO ()
handleConnectionsSink streamGraph = withSocketsDo $ do
                                                sock <- listenOn $ PortNumber portNumInput1
                                                putStrLn "Starting server ..."
                                                handleConnectionsSink' sock streamGraph

handleConnectionsSink' :: Read alpha => Show beta => Socket -> (Stream alpha -> Stream beta) -> IO ()
handleConnectionsSink' sock streamOps = do
                                          stream <- readListFromSocket sock          -- read stream of Strings from socket
                                          let eventStream = map (\e->read e) stream
                                          let result = streamOps eventStream         -- process stream
                                          printStream result                         -- to print stream
                                       
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

handleConnectionsLink:: Read alpha => Show beta => (Stream alpha -> Stream beta) -> IO ()
handleConnectionsLink streamGraph = withSocketsDo $ do
                                                sockIn <- listenOn $ PortNumber portNumInput1
                                                putStrLn "Starting link ..."
                                                handleConnectionsLink' sockIn streamGraph

handleConnectionsLink' :: Read alpha => Show beta => Socket -> (Stream alpha -> Stream beta) -> IO ()
handleConnectionsLink' sock streamOps = do
                                          stream <- readListFromSocket sock          -- read stream of Strings from socket
                                          let eventStream = map (\e->read e) stream
                                          let result = streamOps eventStream         -- process stream
                                          sendStream result                          -- to print stream

sendStream:: Show alpha => Stream alpha -> IO ()
sendStream (h:t) = withSocketsDo $ do
                      handle <- connectTo hostNameOutput (PortNumber portNumOutput)
                      hPutStr handle (show h)                                     
                      hClose handle                                      
                      sendStream t

sendSource:: Show alpha => IO alpha -> IO ()
sendSource pay       = withSocketsDo $ do
                            handle <- connectTo hostNameOutput (PortNumber portNumOutput)
                            now    <- getCurrentTime
                            payload <- pay
                            let msg = show (E now payload)
                            hPutStr handle msg                                    
                            hClose handle                                      
                            sendSource pay

