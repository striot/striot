{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
module Striot.Nodes ( nodeSink
                    , nodeSink2
                    , nodeLink
                    , nodeLink2
                    , nodeSource
                    ) where

import           Conduit                          hiding (connect)
import           Control.Concurrent               hiding (yield)
import           Control.Concurrent.Async         (async)
import           Control.Concurrent.Chan.Unagi    as U
import           Data.Aeson
import qualified Data.Attoparsec.ByteString.Char8 as A
import qualified Data.ByteString.Char8            as BC
import qualified Data.ByteString.Lazy.Char8       as BLC
import           Data.Conduit.Network
import           Data.Time                        (getCurrentTime)
import           Striot.FunctionalIoTtypes
import           System.IO.Unsafe                 (unsafeInterleaveIO)

type HostName   = String
type PortNumber = Int

--- SINK FUNCTIONS ---

nodeSink :: (FromJSON alpha, ToJSON beta) => (Stream alpha -> Stream beta) -> (Stream beta -> IO ()) -> PortNumber -> IO ()
nodeSink streamOp iofn inputPort = do
    putStrLn "Starting server ..."
    stream <- processSocketC inputPort
    let result = streamOp stream
    iofn result

-- A Sink with 2 inputs
nodeSink2 :: (FromJSON alpha, FromJSON beta, ToJSON gamma) => (Stream alpha -> Stream beta -> Stream gamma) -> (Stream gamma -> IO ()) -> PortNumber -> PortNumber -> IO ()
nodeSink2 streamOp iofn inputPort1 inputPort2 =do
    putStrLn "Starting server ..."
    stream1 <- processSocketC inputPort1
    stream2 <- processSocketC inputPort2
    let result = streamOp stream1 stream2
    iofn result


--- LINK FUNCTIONS ---

nodeLink :: (Show alpha, FromJSON alpha, ToJSON beta) => (Stream alpha -> Stream beta) -> PortNumber -> HostName -> PortNumber -> IO ()
nodeLink streamOp inputPort outputHost outputPort = do
    putStrLn "Starting link ..."
    stream <- processSocketC inputPort
    let result = streamOp stream
    sendStreamC result outputHost outputPort


-- A Link with 2 inputs
nodeLink2 :: (FromJSON alpha, FromJSON beta, ToJSON gamma) => (Stream alpha -> Stream beta -> Stream gamma) -> PortNumber -> PortNumber -> HostName -> PortNumber -> IO ()
nodeLink2 streamOp inputPort1 inputPort2 outputHost outputPort = do
    putStrLn "Starting link ..."
    stream1 <- processSocketC inputPort1
    stream2 <- processSocketC inputPort2
    let result = streamOp stream1 stream2
    sendStreamC result outputHost outputPort


--- SOURCE FUNCTIONS ---

nodeSource :: ToJSON beta => IO alpha -> (Stream alpha -> Stream beta) -> HostName -> PortNumber -> IO ()
nodeSource pay streamGraph host port = do
    putStrLn "Starting source ..."
    stream <- readListFromSource pay
    let result = streamGraph stream
    sendStreamC result host port -- or printStream if it's a completely self contained streamGraph


--- UTILITY FUNCTIONS ---

readListFromSource :: IO alpha -> IO (Stream alpha)
readListFromSource = go 0
  where
    go i pay = unsafeInterleaveIO $ do
        x  <- msg i
        xs <- go (i + 1) pay    -- This will overflow eventually
        return (x : xs)
      where
        msg x = do
            now     <- getCurrentTime
            Event x (Just now) . Just <$> pay


{- Handler to receive a lazy list of Events from the socket, where the
events are read from the socket one by one in constant memory -}
processSocketC :: FromJSON alpha => PortNumber -> IO (Stream alpha)
processSocketC inputPort = U.getChanContents =<< acceptConnectionsC inputPort


{- Reads from source node, parses, deserialises, and writes to the channel,
without introducing laziness -}
acceptConnectionsC :: FromJSON alpha => PortNumber -> IO (U.OutChan (Event alpha))
acceptConnectionsC port = do
    (inChan, outChan) <- U.newChan
    async $
        runTCPServer (serverSettings port "*") $ \source ->
          runConduit
          $ appSource source
         .| parseEvents
         .| deserialise
         .| mapM_C (U.writeChan inChan)
    return outChan


{- Send stream to downstream one event at a time -}
sendStreamC :: ToJSON alpha => Stream alpha -> HostName -> PortNumber -> IO ()
sendStreamC []     _    _    = return ()
sendStreamC stream host port =
    runTCPClient (clientSettings port (BC.pack host)) $ \sink ->
        runConduit
      $ yieldMany stream
     .| serialise
     .| mapC (`BC.snoc` eventTerminationChar)
     .| appSink sink


{- Keep reading from upstream conduit and parsing for end of event character.
This is required for large events > 4096 bytes -}
parseEvents :: (Monad m) => ConduitT BC.ByteString BC.ByteString m ()
parseEvents = go BC.empty
    where
        go st = await >>= \case
                    Nothing -> go st
                    Just x  ->
                        let p = A.parse parseEventTermination x
                        in  case p of
                                A.Partial _ -> go (BC.append st x)
                                A.Done i r  -> yield (BC.append st r) >> go i


{- attoparsec parser to search for end of string character -}
parseEventTermination :: A.Parser BC.ByteString
parseEventTermination = do
    x <- A.takeWhile (/= eventTerminationChar)
    A.anyChar
    return x


{- This defines the character that we append after each event-}
eventTerminationChar :: Char
eventTerminationChar = '\NUL'


{- Conduit to serialise with aeson -}
serialise :: (Monad m, ToJSON a) => ConduitT a BC.ByteString m ()
serialise = awaitForever $ yield . BLC.toStrict . encode


{- Conduit to deserialise with aeson -}
deserialise :: (Monad m, FromJSON b) => ConduitT BC.ByteString b m ()
deserialise = awaitForever (\x -> case decodeStrict x of
                                        Just v  -> yield v
                                        Nothing -> return ())
