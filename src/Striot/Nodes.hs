{-# LANGUAGE DataKinds         #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE OverloadedStrings #-}
module Striot.Nodes ( nodeSource
                    , nodeLink
                    , nodeLink2
                    , nodeSink
                    , nodeSink2
                    , nodeSimple

                    , defaultConfig
                    , defaultSource
                    , defaultLink
                    , defaultSink

                    , mkStream
                    , unStream

                    ) where

import           Control.Concurrent.Async                      (async)
import           Control.Concurrent.Chan.Unagi.Bounded         as U
import           Control.Lens
import           Control.Monad.IO.Class
import           Control.Monad.Reader
import           Data.IORef
import           Data.Maybe
import           Data.Store                                    (Store)
import           Data.Text                                     as T (pack)
import           Data.Time                                     (getCurrentTime)
import           Network.Socket                                (HostName,
                                                                ServiceName)
import           Striot.FunctionalIoTtypes
import           Striot.Nodes.Kafka
import           Striot.Nodes.MQTT
import           Striot.Nodes.TCP
import           Striot.Nodes.Types                            hiding (nc,
                                                                readConf,
                                                                writeConf)
import           System.IO
import           System.IO.Unsafe
import           System.Metrics.Prometheus.Concurrent.Registry as PR (new, registerCounter,
                                                                      registerGauge,
                                                                      sample)
import           System.Metrics.Prometheus.Http.Scrape         (serveMetrics)
import           System.Metrics.Prometheus.Metric.Counter      as PC (inc)
import           System.Metrics.Prometheus.MetricId            (addLabel)


-- NODE FUNCTIONS

nodeSource :: (Store alpha, Store beta)
           => StriotConfig
           -> IO alpha
           -> (Stream alpha -> Stream beta)
           -> IO ()
nodeSource config iofn streamOp =
    runReaderT (unApp $ nodeSource' iofn streamOp) config


nodeSource' :: (Store alpha, Store beta,
               MonadReader r m,
               HasStriotConfig r,
               MonadIO m)
            => IO alpha
            -> (Stream alpha -> Stream beta) -> m ()
nodeSource' iofn streamOp = do
    c <- ask
    metrics <- liftIO $ startPrometheus (c ^. nodeName)
    let iofnAndMetrics = PC.inc (_ingressEvents metrics) >> iofn
    stream <- liftIO $ readListFromSource iofnAndMetrics
    let result = streamOp stream
    sendStream metrics result


nodeLink :: (Store alpha, Store beta)
         => StriotConfig
         -> (Stream alpha -> Stream beta)
         -> IO ()
nodeLink config streamOp =
    runReaderT (unApp $ nodeLink' streamOp) config


nodeLink' :: (Store alpha, Store beta,
             MonadReader r m,
             HasStriotConfig r,
             MonadIO m)
          => (Stream alpha -> Stream beta)
          -> m ()
nodeLink' streamOp = do
    c <- ask
    metrics <- liftIO $ startPrometheus (c ^. nodeName)
    stream <- processInput metrics
    let result = streamOp stream
    sendStream metrics result


-- Old style configless link with 2 inputs
nodeLink2 :: (Store alpha, Store beta, Store gamma)
          => (Stream alpha -> Stream beta -> Stream gamma)
          -> ServiceName
          -> ServiceName
          -> HostName
          -> ServiceName
          -> IO ()
nodeLink2 streamOp inputPort1 inputPort2 outputHost outputPort = do
    let nodeName = "node-link"
        (ConnTCPConfig ic1) = tcpConfig "" inputPort1
        (ConnTCPConfig ic2) = tcpConfig "" inputPort2
        (ConnTCPConfig ec)  = tcpConfig outputHost outputPort
    metrics <- startPrometheus nodeName
    putStrLn "Starting link ..."
    stream1 <- processSocket nodeName ic1 metrics
    stream2 <- processSocket nodeName ic2 metrics
    let result = streamOp stream1 stream2
    sendStreamTCP nodeName ec metrics result


nodeSink :: (Store alpha, Store beta)
         => StriotConfig
         -> (Stream alpha -> Stream beta)
         -> (Stream beta -> IO ())
         -> IO ()
nodeSink config streamOp iofn =
    runReaderT (unApp $ nodeSink' streamOp iofn) config


nodeSink' :: (Store alpha, Store beta,
             MonadReader r m,
             HasStriotConfig r,
             MonadIO m)
          => (Stream alpha -> Stream beta)
          -> (Stream beta -> IO ())
          -> m ()
nodeSink' streamOp iofn = do
    c <- ask
    metrics <- liftIO $ startPrometheus (c ^. nodeName)
    stream <- processInput metrics
    let result = streamOp stream
    liftIO $ iofn result


-- Old style configless sink with 2 inputs
nodeSink2 :: (Store alpha, Store beta, Store gamma)
          => (Stream alpha -> Stream beta -> Stream gamma)
          -> (Stream gamma -> IO ())
          -> ServiceName
          -> ServiceName
          -> IO ()
nodeSink2 streamOp iofn inputPort1 inputPort2 = do
    let nodeName = "node-sink"
        (ConnTCPConfig ic1) = tcpConfig "" inputPort1
        (ConnTCPConfig ic2) = tcpConfig "" inputPort2
    metrics <- startPrometheus nodeName
    putStrLn "Starting sink ..."
    stream1 <- processSocket nodeName ic1 metrics
    stream2 <- processSocket nodeName ic2 metrics
    let result = streamOp stream1 stream2
    iofn result

-- | a simple source-sink combined function for single-Node
-- deployments.
-- The first argument is a Source function to create data. The second is the pure
-- stream-processing function. The third is a Sink function to operate on the
-- processed Stream.
nodeSimple :: (IO a) -> (Stream a -> Stream b) -> (Stream b -> IO()) -> IO ()
nodeSimple src proc sink = sink . proc =<< readListFromSource src

--- CONFIG FUNCTIONS ---

defaultConfig :: String
              -> HostName
              -> ServiceName
              -> HostName
              -> ServiceName
              -> StriotConfig
defaultConfig = defaultConfig' TCP TCP


defaultSource :: HostName -> ServiceName -> StriotConfig
defaultSource = defaultConfig "striot-source" "" ""


defaultLink :: ServiceName -> HostName -> ServiceName -> StriotConfig
defaultLink = defaultConfig "striot-link" ""


defaultSink :: ServiceName -> StriotConfig
defaultSink port = defaultConfig "striot-sink" "" port "" ""


defaultConfig' :: ConnectProtocol
               -> ConnectProtocol
               -> String
               -> HostName
               -> ServiceName
               -> HostName
               -> ServiceName
               -> StriotConfig
defaultConfig' ict ect name inHost inPort outHost outPort =
    let ccf ct = case ct of
                    TCP   -> tcpConfig
                    KAFKA -> defaultKafkaConfig
                    MQTT  -> defaultMqttConfig
    in  baseConfig name (ccf ict inHost inPort) (ccf ect outHost outPort)


baseConfig :: String -> ConnectionConfig -> ConnectionConfig -> StriotConfig
baseConfig name icc ecc =
    StriotConfig
        { _nodeName          = name
        , _ingressConnConfig = icc
        , _egressConnConfig  = ecc
        , _chanSize          = 10
        }


tcpConfig :: HostName -> ServiceName -> ConnectionConfig
tcpConfig host port =
    ConnTCPConfig $ TCPConfig $ NetConfig host port


kafkaConfig :: String -> String -> HostName -> ServiceName -> ConnectionConfig
kafkaConfig topic conGroup host port =
    ConnKafkaConfig $ KafkaConfig (NetConfig host port) topic conGroup


defaultKafkaConfig :: HostName -> ServiceName -> ConnectionConfig
defaultKafkaConfig = kafkaConfig "striot-queue" "striot_consumer_group"


mqttConfig :: String -> HostName -> ServiceName -> ConnectionConfig
mqttConfig topic host port =
    ConnMQTTConfig $ MQTTConfig (NetConfig host port) topic


defaultMqttConfig :: HostName -> ServiceName -> ConnectionConfig
defaultMqttConfig = mqttConfig "StriotQueue"


--- INTERNAL OPS ---

processInput :: (Store alpha,
                MonadReader r m,
                HasStriotConfig r,
                MonadIO m)
             => Metrics
             -> m (Stream alpha)
processInput metrics = connectDispatch metrics >>= (liftIO . U.getChanContents)


connectDispatch :: (Store alpha,
                   MonadReader r m,
                   HasStriotConfig r,
                   MonadIO m)
                => Metrics
                -> m (U.OutChan (Event alpha))
connectDispatch metrics = do
    c <- ask
    liftIO $ do
        (inChan, outChan) <- U.newChan (c ^. chanSize)
        async $ connectDispatch' (c ^. nodeName)
                                 (c ^. ingressConnConfig)
                                 metrics
                                 inChan
        return outChan


connectDispatch' :: (Store alpha,
                    MonadIO m)
                 => String
                 -> ConnectionConfig
                 -> Metrics
                 -> U.InChan (Event alpha)
                 -> m ()
connectDispatch' name (ConnTCPConfig   cc) met chan = liftIO $ connectTCP       name cc met chan
connectDispatch' name (ConnKafkaConfig cc) met chan = liftIO $ runKafkaConsumer name cc met chan
connectDispatch' name (ConnMQTTConfig  cc) met chan = liftIO $ runMQTTSub       name cc met chan


sendStream :: (Store alpha,
              MonadReader r m,
              HasStriotConfig r,
              MonadIO m)
           => Metrics
           -> Stream alpha
           -> m ()
sendStream metrics stream = do
    c <- ask
    liftIO
        $ sendDispatch (c ^. nodeName)
                       (c ^. egressConnConfig)
                       metrics
                       stream


sendDispatch :: (Store alpha,
                MonadIO m)
             => String
             -> ConnectionConfig
             -> Metrics
             -> Stream alpha
             -> m ()
sendDispatch name (ConnTCPConfig   cc) met stream = liftIO $ sendStreamTCP   name cc met stream
sendDispatch name (ConnKafkaConfig cc) met stream = liftIO $ sendStreamKafka name cc met stream
sendDispatch name (ConnMQTTConfig  cc) met stream = liftIO $ sendStreamMQTT  name cc met stream


readListFromSource :: IO alpha -> IO (Stream alpha)
readListFromSource = go
  where
    go pay = unsafeInterleaveIO $ do
        x  <- msg
        xs <- go pay
        return (x : xs)
      where
        msg = do
            now <- getCurrentTime
            Event (Just now) . Just <$> pay


--- PROMETHEUS ---

startPrometheus :: String -> IO Metrics
startPrometheus name = do
    reg <- PR.new
    let lbl = addLabel "node" (T.pack name) mempty
        registerFn fn mName = fn mName lbl reg
        rg = registerFn registerGauge
        rc = registerFn registerCounter
    async $ serveMetrics 8080 ["metrics"] (PR.sample reg)
    Metrics
        <$> rg "striot_ingress_connection"
        <*> rc "striot_ingress_bytes_total"
        <*> rc "striot_ingress_events_total"
        <*> rg "striot_egress_connection"
        <*> rc "striot_egress_bytes_total"
        <*> rc "striot_egress_events_total"

--------------------------------------------------------------------
-- simple routines for pure streams

-- | Convenience function for creating a pure `Stream`.
mkStream :: [a] -> Stream a
mkStream = map $ Event Nothing . Just

-- | A convenience function to extract a list of values from a `Stream`.
unStream :: Stream a -> [a]
unStream = catMaybes . map value
