{-# LANGUAGE FlexibleContexts #-}
module Striot.Nodes ( nodeSink
                    , nodeSink2
                    , nodeLink
                    , nodeLink2
                    , nodeSource
                    ) where

import           Control.Concurrent
import           Control.Concurrent.Chan.Unagi as U
import           Control.Monad                 (forever, when)
import           Data.Aeson
import qualified Data.ByteString                as B
import qualified Data.ByteString.Char8          as BC (putStrLn)
import qualified Data.ByteString.Lazy.Char8     as BLC (hPutStrLn, putStrLn)
import           Data.Maybe
import           Data.Time                      (getCurrentTime)
import           Network.Socket
import           Striot.FunctionalIoTtypes
import           System.IO
import           System.IO.Unsafe


--- SINK FUNCTIONS ---

nodeSink :: (FromJSON (Event alpha), ToJSON (Event beta)) => (Stream alpha -> Stream beta) -> (Stream beta -> IO ()) -> ServiceName -> IO ()
nodeSink streamGraph iofn portNumInput1 = withSocketsDo $ do
    sock <- listenSocket portNumInput1
    putStrLn "Starting server ..."
    hFlush stdout
    nodeSink' sock streamGraph iofn


nodeSink' :: (FromJSON (Event alpha), ToJSON (Event beta)) => Socket -> (Stream alpha -> Stream beta) -> (Stream beta -> IO ()) -> IO ()
nodeSink' sock streamOps iofn = do
    stream <- processSocket sock
    let result = streamOps stream
    iofn result


-- A Sink with 2 inputs
nodeSink2 :: (FromJSON (Event alpha), FromJSON (Event beta), ToJSON (Event gamma)) => (Stream alpha -> Stream beta -> Stream gamma) -> (Stream gamma -> IO ()) -> ServiceName -> ServiceName -> IO ()
nodeSink2 streamGraph iofn portNumInput1 portNumInput2= withSocketsDo $ do
    sock1 <- listenSocket portNumInput1
    sock2 <- listenSocket portNumInput2
    putStrLn "Starting server ..."
    hFlush stdout
    nodeSink2' sock1 sock2 streamGraph iofn


nodeSink2' :: (FromJSON (Event alpha), FromJSON (Event beta), ToJSON (Event gamma)) => Socket -> Socket -> (Stream alpha -> Stream beta -> Stream gamma) -> (Stream gamma -> IO ()) -> IO ()
nodeSink2' sock1 sock2 streamOps iofn = do
    stream1 <- processSocket sock1
    stream2 <- processSocket sock2
    let result = streamOps stream1 stream2
    iofn result


--- LINK FUNCTIONS ---

nodeLink :: (FromJSON (Event alpha), ToJSON (Event beta)) => (Stream alpha -> Stream beta) -> ServiceName -> HostName -> ServiceName -> IO ()
nodeLink streamGraph portNumInput1 hostNameOutput portNumOutput = withSocketsDo $ do
    sockIn <- listenSocket portNumInput1
    putStrLn "Starting link ..."
    hFlush stdout
    nodeLink' sockIn streamGraph hostNameOutput portNumOutput


nodeLink' :: (FromJSON (Event alpha), ToJSON (Event beta)) => Socket -> (Stream alpha -> Stream beta) -> HostName -> ServiceName -> IO ()
nodeLink' sock streamOps host port = do
    stream <- processSocket sock
    let result = streamOps stream
    sendStream result host port


-- A Link with 2 inputs
nodeLink2 :: (FromJSON (Event alpha), FromJSON (Event beta), ToJSON (Event gamma)) => (Stream alpha -> Stream beta -> Stream gamma) -> ServiceName -> ServiceName -> HostName -> ServiceName -> IO ()
nodeLink2 streamGraph portNumInput1 portNumInput2 hostName portNumOutput = withSocketsDo $ do
    sock1 <- listenSocket portNumInput1
    sock2 <- listenSocket portNumInput2
    putStrLn "Starting link ..."
    hFlush stdout
    nodeLink2' sock1 sock2 streamGraph hostName portNumOutput


nodeLink2' :: (FromJSON (Event alpha), FromJSON (Event beta), ToJSON (Event gamma)) => Socket -> Socket -> (Stream alpha -> Stream beta -> Stream gamma) -> HostName -> ServiceName -> IO ()
nodeLink2' sock1 sock2 streamOps host port = do
    stream1 <- processSocket sock1
    stream2 <- processSocket sock2
    let result = streamOps stream1 stream2
    sendStream result host port


--- SOURCE FUNCTIONS ---

nodeSource :: ToJSON (Event beta) => IO alpha -> (Stream alpha -> Stream beta) -> HostName -> ServiceName -> IO ()
nodeSource pay streamGraph host port = do
    putStrLn "Starting source ..."
    stream <- readListFromSource pay
    let result = streamGraph stream
    sendStream result host port -- or printStream if it's a completely self contained streamGraph


--- UTILITY FUNCTIONS ---

readListFromSource :: IO alpha -> IO (Stream alpha)
readListFromSource = go 0
  where
    go i pay = System.IO.Unsafe.unsafeInterleaveIO $ do
        x  <- msg i
        xs <- go (i + 1) pay    -- This will overflow eventually
        return (x : xs)
      where
        msg x = do
            now     <- getCurrentTime
            payload <- pay
            return (Event x (Just now) (Just payload))


{- processSocket is a wrapper function that handles concurrently
accepting and handling connections on the socket and reading all of the strings
into an event Stream -}
processSocket :: FromJSON (Event alpha) => Socket -> IO (Stream alpha)
processSocket sock = acceptConnections sock >>= readEventsTChan


{- acceptConnections takes a socket as an argument and spins up a new thread to
process the data received. The returned TChan object contains the data from
the socket -}
acceptConnections :: FromJSON (Event alpha) => Socket -> IO (U.OutChan (Event alpha))
acceptConnections sock = do
    (inChan, outChan) <- U.newChan
    _         <- forkIO $ connectionHandler sock inChan
    return outChan


{- connectionHandler sits accepting any new connections. Once
accepted, it is converted to a handle and a new thread is forked to handle all
reading. The function then loops to accept a new connection. forkFinally is used
to ensure the thread closes the handle before it exits -}
connectionHandler :: FromJSON (Event alpha) => Socket -> U.InChan (Event alpha) -> IO ()
connectionHandler sockIn eventChan = forever $ do
    -- putStrLn "Awaiting new connection"
    (sock, _) <- accept sockIn
    hdl <- socketToHandle sock ReadWriteMode
    -- putStrLn "Forking to process new connection"
    forkFinally   (processHandle hdl eventChan) (\_ -> hClose hdl)


{- processHandle takes a Handle and TChan. All of the events are read through
hGetLines' with the IO deferred lazily. The string list is mapped to a Stream
and passed to writeEventsTChan -}
processHandle :: FromJSON (Event alpha) => Handle -> U.InChan (Event alpha) -> IO ()
processHandle handle eventChan = do
    byteStream <- hGetLines' handle
    let eventStream = mapMaybe decodeStrict byteStream
    U.writeList2Chan eventChan eventStream


{- writeEventsTChan takes a TChan and Stream of the same type, and recursively
writes the events atomically to the TChan, until an empty list -}
writeEventsTChan :: FromJSON (Event alpha) => U.InChan (Event alpha) -> Stream alpha -> IO ()
writeEventsTChan eventChan = mapM_ (U.writeChan eventChan)

{- readEventsTChan creates a stream of events from reading the next element from
a TChan, but the IO is deferred lazily. Only when the next value of the Stream
is evaluated does the IO computation take place -}
readEventsTChan :: FromJSON (Event alpha) => U.OutChan (Event alpha) -> IO (Stream alpha)
readEventsTChan eventChan = System.IO.Unsafe.unsafeInterleaveIO $ do
    x <- U.readChan eventChan
    xs <- readEventsTChan eventChan
    return (x : xs)


readListFromSocket :: Socket -> IO [B.ByteString]
readListFromSocket sock = do
    (_, stream) <- readListFromSocket' sock
    return stream


readListFromSocket' :: Socket -> IO (Handle, [B.ByteString])
readListFromSocket' sockIn = do
    (sock,_) <- accept sockIn
    hdl <- socketToHandle sock ReadWriteMode
    -- putStrLn "Open input connection"
    stream <- hGetLines' hdl
    return (hdl, stream)


readEventStreamFromSocket :: FromJSON (Event alpha) => Socket -> IO (Handle, Stream alpha)
readEventStreamFromSocket sock = do
    (hdl, byteStream) <- readListFromSocket' sock
    let eventStream = mapMaybe decodeStrict byteStream
    return (hdl, eventStream)


sendStream :: ToJSON (Event alpha) => Stream alpha -> HostName -> ServiceName -> IO ()
sendStream []     _    _    = return ()
sendStream stream host port = withSocketsDo $ do
    sock <- connectSocket host port
    handle <- socketToHandle sock ReadWriteMode
    -- putStrLn "Open output connection"
    hPutLines'    handle stream


{- hGetLines' creates a list of Strings from a Handle, where the IO computation
is deferred lazily until the values are requested -}
hGetLines' :: Handle -> IO [B.ByteString]
hGetLines' handle = System.IO.Unsafe.unsafeInterleaveIO $ do
    readable <- hIsReadable handle
    eof      <- hIsEOF handle
    if not eof && readable
        then do
            x  <- B.hGetLine handle
            xs <- hGetLines' handle
            -- BC.putStrLn x
            return (x : xs)
        else return []


hPutLines' :: ToJSON (Event alpha) => Handle -> Stream alpha -> IO ()
hPutLines' handle [] = do
    hClose handle
    -- putStrLn "Closed output handle"
    return ()
hPutLines' handle (x:xs) = do
    writeable <- hIsWritable handle
    open      <- hIsOpen handle
    when (open && writeable) $ do
        -- BLC.putStrLn (encode x)
        BLC.hPutStrLn    handle (encode x)
        hPutLines' handle xs


--- SOCKETS ---


listenSocket :: ServiceName -> IO Socket
listenSocket port = do
    let hints = defaultHints { addrFlags = [AI_PASSIVE],
                               addrSocketType = Stream }
    (sock, addr) <- createSocket [] port hints
    setSocketOption sock ReuseAddr 1
    bind sock $ addrAddress addr
    listen sock maxQConn
    return sock
    where maxQConn = 10


connectSocket :: HostName -> ServiceName -> IO Socket
connectSocket host port = do
    let hints = defaultHints { addrSocketType = Stream }
    (sock, addr) <- createSocket host port hints
    setSocketOption sock KeepAlive 1
    connect sock $ addrAddress addr
    return sock


createSocket :: HostName -> ServiceName -> AddrInfo -> IO (Socket, AddrInfo)
createSocket host port hints = do
    addr <- resolve host port hints
    sock <- getSocket addr
    return (sock, addr)
  where
    resolve host' port' hints' = do
        addr:_ <- getAddrInfo (Just hints') (isHost host') (Just port')
        return addr
    getSocket addr = socket (addrFamily addr) (addrSocketType addr) (addrProtocol addr)
    isHost h
        | null h    = Nothing
        | otherwise = Just h
