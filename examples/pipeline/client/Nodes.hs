module Nodes(nodeSink,nodeSink2,nodeLink,nodeLink2,nodeSource) where
import Network
import System.IO
import Control.Concurrent
import Data.List
import FunctionalIoTtypes
import System.IO.Unsafe
import Data.Time (getCurrentTime)
import PortConfiguration

nodeSink:: Read alpha => Show beta => (Stream alpha -> Stream beta) -> IO ()
nodeSink streamGraph = withSocketsDo $ do
                                         sock <- listenOn $ PortNumber portNumInput1
                                         putStrLn "Starting server ..."
                                         nodeSink' sock streamGraph

nodeSink' :: Read alpha => Show beta => Socket -> (Stream alpha -> Stream beta) -> IO ()
nodeSink' sock streamOps = do
                             stream <- readListFromSocket sock          -- read stream of Strings from socket
                             let eventStream = map read stream
                             let result = streamOps eventStream         -- process stream
                             printStream result                         -- to print stream

nodeSink2:: Read alpha => Read beta => Show gamma => (Stream alpha -> Stream beta -> Stream gamma) -> IO () -- A Link with 2 inputs
nodeSink2 streamGraph = withSocketsDo $ do
                                          sock1 <- listenOn $ PortNumber portNumInput1
                                          sock2 <- listenOn $ PortNumber portNumInput2
                                          putStrLn "Starting server ..."
                                          nodeSink2' sock1 sock2 streamGraph

nodeSink2' :: Read alpha => Read beta => Show gamma => Socket -> Socket -> (Stream alpha -> Stream beta -> Stream gamma) -> IO ()
nodeSink2' sock1 sock2 streamOps = do
                                     stream1 <- readListFromSocket sock1          -- read stream of Strings from socket
                                     stream2 <- readListFromSocket sock2          -- read stream of Strings from socket
                                     let eventStream1 = map read stream1
                                     let eventStream2 = map read stream2
                                     let result = streamOps eventStream1 eventStream2     -- process stream
                                     printStream result                                    -- to send stream to another node

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

nodeLink:: Read alpha => Show beta => (Stream alpha -> Stream beta) -> IO ()
nodeLink streamGraph = withSocketsDo $ do
                                         sockIn <- listenOn $ PortNumber portNumInput1
                                         putStrLn "Starting link ..."
                                         nodeLink' sockIn streamGraph

nodeLink' :: Read alpha => Show beta => Socket -> (Stream alpha -> Stream beta) -> IO ()
nodeLink' sock streamOps = do
                             stream <- readListFromSocket sock          -- read stream of Strings from socket
                             let eventStream = map read stream
                             let result = streamOps eventStream         -- process stream
                             sendStream result                          -- to send stream to another node

nodeLink2:: Read alpha => Read beta => Show gamma => (Stream alpha -> Stream beta -> Stream gamma) -> IO () -- A Link with 2 inputs
nodeLink2 streamGraph = withSocketsDo $ do
                                          sock1 <- listenOn $ PortNumber portNumInput1
                                          sock2 <- listenOn $ PortNumber portNumInput2
                                          putStrLn "Starting server ..."
                                          nodeLink2' sock1 sock2 streamGraph

nodeLink2' :: Read alpha => Read beta => Show gamma => Socket -> Socket -> (Stream alpha -> Stream beta -> Stream gamma) -> IO ()
nodeLink2' sock1 sock2 streamOps = do
                                     stream1 <- readListFromSocket sock1          -- read stream of Strings from socket
                                     stream2 <- readListFromSocket sock2          -- read stream of Strings from socket
                                     let eventStream1 = map read stream1
                                     let eventStream2 = map read stream2
                                     let result = streamOps eventStream1 eventStream2     -- process stream
                                     sendStream result                                    -- to send stream to another node


sendStream:: Show alpha => Stream alpha -> IO ()
sendStream (h:t) = withSocketsDo $ do
                      handle <- connectTo hostNameOutput (PortNumber portNumOutput)
                      hPutStr handle (show h)
                      hClose handle
                      sendStream t

{-
sendSource:: Show alpha => IO alpha -> IO ()
sendSource pay       = withSocketsDo $ do
                            handle <- connectTo hostNameOutput (PortNumber portNumOutput)
                            now    <- getCurrentTime
                            payload <- pay
                            let msg = show (E now payload)
                            hPutStr handle msg
                            hClose handle
                            sendSource pay
-}

nodeSource :: Show beta => IO alpha -> (Stream alpha -> Stream beta) -> IO ()
nodeSource pay streamGraph = do
                               stream <- readListFromSource pay
                               let result = streamGraph stream
                               sendStream result  --- or printStream if it's a completely self contained streamGraph

readListFromSource :: IO alpha -> IO (Stream alpha)
readListFromSource pay = do {l <- go pay 0; return l}
  where
    go pay i  = do
                   now <- getCurrentTime
                   payload <- pay
                   let msg = E i now payload
                   r <- System.IO.Unsafe.unsafeInterleaveIO (go pay (i+1)) -- at some point this will overflow
                   return (msg:r)
