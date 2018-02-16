module Striot.Nodes ( nodeSink
                    , nodeSink2
                    , nodeLink
                    , nodeLink2
                    , nodeSource
                    ) where

import           Control.Concurrent
import           Control.Monad             (when)
import           Data.List
import           Data.Time                 (getCurrentTime)
import           Network                   (PortID (PortNumber), connectTo,
                                            listenOn)
import           Network.Socket
import           Striot.FunctionalIoTtypes
import           System.IO
import           System.IO.Unsafe

--- SINK FUNCTIONS ---

nodeSink :: (Read alpha, Show beta) => (Stream alpha -> Stream beta) -> (Stream beta -> IO ()) -> PortNumber -> IO ()
nodeSink streamGraph iofn portNumInput1 = withSocketsDo $ do
    sock <- listenOn $ PortNumber portNumInput1
    putStrLn "Starting server ..."
    hFlush stdout
    nodeSink' sock streamGraph iofn


nodeSink' :: (Read alpha, Show beta) => Socket -> (Stream alpha -> Stream beta) -> (Stream beta -> IO ()) -> IO ()
nodeSink' sock streamOps iofn = do
    (handle, stream) <- readEventStreamFromSocket sock -- read stream of Events from socket
    let result = streamOps stream         -- process stream
    iofn result
    hClose handle
    -- print "Closed input handle"
    nodeSink' sock streamOps iofn


-- A Link with 2 inputs
nodeSink2 :: (Read alpha, Read beta, Show gamma) => (Stream alpha -> Stream beta -> Stream gamma) -> (Stream gamma -> IO ()) -> PortNumber -> PortNumber -> IO ()
nodeSink2 streamGraph iofn portNumInput1 portNumInput2= withSocketsDo $ do
    sock1 <- listenOn $ PortNumber portNumInput1
    sock2 <- listenOn $ PortNumber portNumInput2
    putStrLn "Starting server ..."
    hFlush stdout
    nodeSink2' sock1 sock2 streamGraph iofn


nodeSink2' :: (Read alpha, Read beta, Show gamma) => Socket -> Socket -> (Stream alpha -> Stream beta -> Stream gamma) -> (Stream gamma -> IO ()) -> IO ()
nodeSink2' sock1 sock2 streamOps iofn = do
    (handle1, stream1) <- readEventStreamFromSocket sock1 -- read stream of Events from socket
    (handle2, stream2) <- readEventStreamFromSocket sock2 -- read stream of Events from socket
    let result = streamOps stream1 stream2     -- process stream
    iofn result
    hClose handle1
    hClose handle2
    -- print "Close input handles"
    nodeSink2' sock1 sock2 streamOps iofn


--- LINK FUNCTIONS ---

nodeLink :: (Read alpha, Show beta) => (Stream alpha -> Stream beta) -> PortNumber -> HostName -> PortNumber -> IO ()
nodeLink streamGraph portNumInput1 hostNameOutput portNumOutput = withSocketsDo $ do
    sockIn <- listenOn $ PortNumber portNumInput1
    putStrLn "Starting link ..."
    hFlush stdout
    nodeLink' sockIn streamGraph hostNameOutput portNumOutput


nodeLink' :: (Read alpha, Show beta) => Socket -> (Stream alpha -> Stream beta) -> HostName -> PortNumber -> IO ()
nodeLink' sock streamOps host port = do
    (handle, stream) <- readEventStreamFromSocket sock -- read stream of Events from socket
    let result = streamOps stream  -- process stream
    sendStream result host port         -- to send stream to another node
    hClose handle
    -- print "Closed input handle"
    nodeLink' sock streamOps host port


-- A Link with 2 inputs
nodeLink2 :: (Read alpha, Read beta, Show gamma) => (Stream alpha -> Stream beta -> Stream gamma) -> PortNumber -> PortNumber -> HostName -> PortNumber -> IO ()
nodeLink2 streamGraph portNumInput1 portNumInput2 hostName portNumOutput = withSocketsDo $ do
    sock1 <- listenOn $ PortNumber portNumInput1
    sock2 <- listenOn $ PortNumber portNumInput2
    putStrLn "Starting server ..."
    hFlush stdout
    nodeLink2' sock1 sock2 streamGraph hostName portNumOutput


nodeLink2' :: (Read alpha, Read beta, Show gamma) => Socket -> Socket -> (Stream alpha -> Stream beta -> Stream gamma) -> HostName -> PortNumber -> IO ()
nodeLink2' sock1 sock2 streamOps host port = do
    (handle1, stream1) <- readEventStreamFromSocket sock1 -- read stream of Events from socket
    (handle2, stream2) <- readEventStreamFromSocket sock2 -- read stream of Events from socket
    let result = streamOps stream1 stream2 -- process stream
    sendStream result host port                      -- to send stream to another node
    hClose handle1
    hClose handle2
    -- print "Closed input handles"
    nodeLink2' sock1 sock2 streamOps host port


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

--- SOURCE FUNCTIONS ---

nodeSource :: Show beta => IO alpha -> (Stream alpha -> Stream beta) -> HostName -> PortNumber -> IO ()
nodeSource pay streamGraph host port = do
    stream <- readListFromSource pay
    let result = streamGraph stream
    sendStream result host port -- or printStream if it's a completely self contained streamGraph


--- UTILITY FUNCTIONS ---

readListFromSource :: IO alpha -> IO (Stream alpha)
readListFromSource = go 0
  where
    go i pay = System.IO.Unsafe.unsafeInterleaveIO $ do
        x  <- msg i
        xs <- go (i + 1) pay
        return (x : xs)
      where
        msg x = do
            now     <- getCurrentTime
            payload <- pay
            return (E x now payload)


readListFromSocket :: Socket -> IO [String]
readListFromSocket sock = do
    (_, stream) <- readListFromSocket' sock
    return stream


readListFromSocket' :: Socket -> IO (Handle, [String])
readListFromSocket' sockIn = do
    (sock, _) <- accept sockIn
    hdl       <- socketToHandle sock ReadWriteMode
    hSetBuffering hdl NoBuffering
    -- print "Open input connection"
    stream <- hGetLines' hdl
    return (hdl, stream)


readEventStreamFromSocket :: Read alpha => Socket -> IO (Handle, Stream alpha)
readEventStreamFromSocket sock = do
    (hdl, stringStream) <- readListFromSocket' sock
    let eventStream = map read stringStream
    return (hdl, eventStream)


sendStream :: Show alpha => Stream alpha -> HostName -> PortNumber -> IO ()
sendStream []     host port = return ()
sendStream stream host port = withSocketsDo $ do
    handle <- connectTo host (PortNumber port)
    hSetBuffering handle NoBuffering
    -- print "Open output connection"
    hPutLines'    handle stream


hGetLines' :: Handle -> IO [String]
hGetLines' handle = System.IO.Unsafe.unsafeInterleaveIO $ do
    readable <- hIsReadable handle
    eof      <- hIsEOF handle
    if not eof && readable
        then do
            x  <- hGetLine handle
            xs <- hGetLines' handle
            -- print x
            return (x : xs)
        else return []


hPutLines' :: Show alpha => Handle -> Stream alpha -> IO ()
hPutLines' handle [] = do
    hClose handle
    -- print "Closed output handle"
    return ()
hPutLines' handle (x:xs) = do
    writeable <- hIsWritable handle
    open      <- hIsOpen handle
    when (open && writeable) $ do
            -- print h
        hPrint     handle x
        hPutLines' handle xs
