module Striot.Nodes.TCP
( connectTCP
, sendStreamTCP
, processSocket
) where

import           Control.Concurrent                       (forkFinally)
import           Control.Concurrent.Async                 (async)
import           Control.Concurrent.Chan.Unagi.Bounded    as U
import qualified Control.Exception                        as E (bracket, catch,
                                                                evaluate)
import           Control.Lens
import           Control.Monad                            (forever)
import qualified Data.ByteString                          as B (ByteString,
                                                                length, null)
import           Data.Store                               (Store, decode,
                                                           encode)
import qualified Data.Store.Streaming                     as SS
import           Network.Socket
import           Network.Socket.ByteString
import           Striot.FunctionalIoTtypes
import           Striot.Nodes.Types
import           System.IO.ByteBuffer                     as BB
import           System.Metrics.Prometheus.Metric.Counter as PC (add, inc)
import           System.Metrics.Prometheus.Metric.Gauge   as PG (dec, inc)


processSocket :: Store alpha => String -> TCPConfig -> Metrics -> IO (Stream alpha)
processSocket name conf met = U.getChanContents =<< acceptConnections name conf met


acceptConnections :: Store alpha => String -> TCPConfig -> Metrics -> IO (U.OutChan (Event alpha))
acceptConnections name conf met = do
    (inChan, outChan) <- U.newChan defaultChanSize
    async $ connectTCP name conf met inChan
    return outChan


defaultChanSize :: Int
defaultChanSize = 10


{- connectTCP sits accepting any new connections. Once accepted, a new
thread is forked to read from the socket. The function then loops to accept any
subsequent connections -}
connectTCP :: Store alpha
           => String
           -> TCPConfig
           -> Metrics
           -> U.InChan (Event alpha)
           -> IO ()
connectTCP _ conf met chan = do
    sock <- listenSocket $ conf ^. tcpConn . port
    forever $ do
        (conn, _) <- accept sock
        forkFinally (PG.inc (_ingressConn met)
                    >> processData met conn chan)
                    (\_ -> PG.dec (_ingressConn met)
                        >> close conn)


{- processData takes a Socket and UChan. All of the events are read through
use of a ByteBuffer and recv. The events are decoded by using store-streaming
and added to the chan  -}
processData :: Store alpha => Metrics -> Socket -> U.InChan (Event alpha) -> IO ()
processData met conn eventChan =
    BB.with Nothing $ \buffer -> forever $ do
        event <- decodeMessageBS' met buffer (readFromSocket conn)
        case event of
            Just m  -> do
                        PC.inc (_ingressEvents met)
                        U.writeChan eventChan $ SS.fromMessage m
            Nothing -> print "decode failed"


{- This is a rewrite of Data.Store.Streaming decodeMessageBS, passing in
Metrics so that we can calculate ingressBytes while decoding -}
decodeMessageBS' :: Store a
                 => Metrics -> BB.ByteBuffer
                 -> IO (Maybe B.ByteString) -> IO (Maybe (SS.Message a))
decodeMessageBS' met = SS.decodeMessage (\bb _ bs -> PC.add (B.length bs)
                                                            (_ingressBytes met)
                                                     >> BB.copyByteString bb bs)


{- Read up to 4096 bytes from the socket at a time, packing into a Maybe
structure. As we use TCP sockets recv should block, and so if msg is empty
the connection has been closed -}
readFromSocket :: Socket -> IO (Maybe B.ByteString)
readFromSocket conn = do
    msg <- recv conn 4096
    if B.null msg
        then error "Upstream connection closed"
        else return $ Just msg


{- Connects to socket within a bracket to ensure the socket is closed if an
exception occurs -}
sendStreamTCP :: Store alpha => String -> TCPConfig -> Metrics -> Stream alpha -> IO ()
sendStreamTCP _ _    _   []     = return ()
sendStreamTCP _ conf met stream =
    E.bracket (PG.inc (_egressConn met)
               >> connectSocket (conf ^. tcpConn . host) (conf ^. tcpConn . port))
              (\conn -> PG.dec (_egressConn met)
                        >> close conn)
              (\conn -> writeSocket conn met stream)


{- Encode messages and send over the socket -}
writeSocket :: Store alpha => Socket -> Metrics -> Stream alpha -> IO ()
writeSocket conn met =
    mapM_ (\event ->
            let val = SS.encodeMessage . SS.Message $ event
            in  PC.inc (_egressEvents met)
                >> PC.add (B.length val) (_egressBytes met)
                >> sendAll conn val)


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
    getSocket addr = socket (addrFamily addr)
                            (addrSocketType addr)
                            (addrProtocol addr)
    isHost h
        | null h    = Nothing
        | otherwise = Just h
