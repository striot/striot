{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
module Striot.Nodes ( nodeSink
                    , nodeSink2
                    , nodeLink
                    , nodeLink2
                    , nodeSource
                    , defaultConfig
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
import           System.Metrics.Prometheus.Http.Scrape         (serveHttpTextMetrics)
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
    stream <- liftIO $ readListFromSource iofn metrics
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


-- {- Old style configless link with 2 inputs }
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


-- {- Old style configless sink with 2 inputs }
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


--- CONFIG FUNCTIONS ---

defaultConfig :: String -> HostName -> ServiceName -> HostName -> ServiceName -> StriotConfig
defaultConfig = defaultConfig' TCP TCP


defaultConfig' :: ConnectProtocol -> ConnectProtocol -> String -> HostName -> ServiceName -> HostName -> ServiceName -> StriotConfig
defaultConfig' ict ect nn inHost inPort outHost outPort =
    let ccf ct = case ct of
                    TCP   -> tcpConfig
                    KAFKA -> defaultKafkaConfig
                    MQTT  -> defaultMqttConfig
    in  baseConfig nn ((ccf ict) inHost inPort) ((ccf ect) outHost outPort)


baseConfig :: String -> ConnectionConfig -> ConnectionConfig -> StriotConfig
baseConfig nn icc ecc =
    StriotConfig
        { _nodeName          = nn
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
connectDispatch' nodeName (ConnTCPConfig   cc) met chan = liftIO $ connectTCP       nodeName cc met chan
connectDispatch' nodeName (ConnKafkaConfig cc) met chan = liftIO $ runKafkaConsumer nodeName cc met chan
connectDispatch' nodeName (ConnMQTTConfig  cc) met chan = liftIO $ runMQTTSub       nodeName cc met chan


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
sendDispatch nodeName (ConnTCPConfig   cc) met stream = liftIO $ sendStreamTCP   nodeName cc met stream
sendDispatch nodeName (ConnKafkaConfig cc) met stream = liftIO $ sendStreamKafka nodeName cc met stream
sendDispatch nodeName (ConnMQTTConfig  cc) met stream = liftIO $ sendStreamMQTT  nodeName cc met stream


--- UTILITY FUNCTIONS ---

readListFromSource :: IO alpha -> Metrics -> IO (Stream alpha)
readListFromSource = go
  where
    go pay met = unsafeInterleaveIO $ do
        x  <- msg
        PC.inc (_ingressEvents met)
        xs <- go pay met
        return (x : xs)
      where
        msg = do
            now <- getCurrentTime
            Event (Just now) . Just <$> pay


--- PROMETHEUS ---

startPrometheus :: String -> IO Metrics
startPrometheus nodeName = do
    reg <- PR.new
    let lbl = addLabel "node" (T.pack nodeName) mempty
        registerFn fn name = fn name lbl reg
    ingressConn   <- registerFn registerGauge   "striot_ingress_connection"
    ingressBytes  <- registerFn registerCounter "striot_ingress_bytes_total"
    ingressEvents <- registerFn registerCounter "striot_ingress_events_total"
    egressConn    <- registerFn registerGauge   "striot_egress_connection"
    egressBytes   <- registerFn registerCounter "striot_egress_bytes_total"
    egressEvents  <- registerFn registerCounter "striot_egress_events_total"
    async $ serveHttpTextMetrics 8080 ["metrics"] (PR.sample reg)
    return Metrics { _ingressConn   = ingressConn
                   , _ingressBytes  = ingressBytes
                   , _ingressEvents = ingressEvents
                   , _egressConn    = egressConn
                   , _egressBytes   = egressBytes
                   , _egressEvents  = egressEvents }
