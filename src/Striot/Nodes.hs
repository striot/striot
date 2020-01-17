{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedStrings #-}
module Striot.Nodes ( nodeSink
                    , nodeSink2
                    , nodeLink
                    , nodeLink2
                    , nodeSource
                    , nodeSourceMqtt
                    , nodeLinkMqtt
                    , nodeSourceKafka
                    , nodeLinkKafka
                    ) where

import           Control.Concurrent                            (forkFinally,
                                                                threadDelay)
import           Control.Concurrent.Async                      (async)
import           Control.Concurrent.Chan.Unagi.Bounded         as U
import           Control.Concurrent.STM
import qualified Control.Exception                             as E (bracket,
                                                                     catch,
                                                                     evaluate)
import           Control.Monad                                 (forever, unless,
                                                                when, void)
import           Control.Monad.Reader
import           Control.Monad.IO.Class
import           Control.DeepSeq                               (force)
import           Control.Lens
import qualified Data.ByteString                               as B (ByteString,
                                                                     length,
                                                                     null)
import qualified Data.ByteString.Lazy.Char8                    as BLC
import           Data.IORef
import           Data.Maybe
import           Data.Store                                    (Store, encode, decode)
import qualified Data.Store.Streaming                          as SS
import           Data.Text                                     as T (Text, pack)
import           Data.Time                                     (getCurrentTime)
import           Network.MQTT.Client as MQTT hiding            (Timeout)
import           Network.MQTT.Types                            (RetainHandling(..))
import           Network.Socket
import           Network.Socket.ByteString
import           Network.URI                                   (parseURI)
import           Striot.FunctionalIoTtypes
import           System.Exit                                   (exitFailure)
import           Striot.Nodes.Types as NT
import           System.IO
import           System.IO.ByteBuffer                          as BB
import           System.IO.Unsafe
import           System.Metrics.Prometheus.Concurrent.Registry as PR (new, registerCounter,
                                                                      registerGauge,
                                                                      sample)
import           System.Metrics.Prometheus.Http.Scrape         (serveHttpTextMetrics)
import           System.Metrics.Prometheus.Metric.Counter      as PC (add, inc)
import           System.Metrics.Prometheus.Metric.Gauge        as PG (dec, inc)
import           System.Metrics.Prometheus.MetricId            (addLabel)
import           Kafka.Producer                                as KP
import           Kafka.Consumer                                as KC


--- SINK FUNCTIONS ---

nodeSink :: (Store alpha, Store beta)
         => (Stream alpha -> Stream beta)
         -> (Stream beta -> IO ())
         -> ServiceName
         -> IO ()
nodeSink streamOp iofn inputPort = do
    sock <- listenSocket inputPort
    metrics <- startPrometheus "node-sink"
    putStrLn "Starting server ..."
    stream <- processSocket metrics sock
    let result = streamOp stream
    iofn result


-- A Sink with 2 inputs
nodeSink2 :: (Store alpha, Store beta, Store gamma)
          => (Stream alpha -> Stream beta -> Stream gamma)
          -> (Stream gamma -> IO ())
          -> ServiceName
          -> ServiceName
          -> IO ()
nodeSink2 streamOp iofn inputPort1 inputPort2 = do
    sock1 <- listenSocket inputPort1
    sock2 <- listenSocket inputPort2
    metrics <- startPrometheus "node-sink"
    putStrLn "Starting server ..."
    stream1 <- processSocket metrics sock1
    stream2 <- processSocket metrics sock2
    let result = streamOp stream1 stream2
    iofn result


--- LINK FUNCTIONS ---

nodeLink :: (Store alpha, Store beta)
         => (Stream alpha -> Stream beta)
         -> ServiceName
         -> HostName
         -> ServiceName
         -> IO ()
nodeLink streamOp inputPort outputHost outputPort = do
    sock <- listenSocket inputPort
    metrics <- startPrometheus "node-link"
    putStrLn "Starting link ..."
    stream <- processSocket metrics sock
    let result = streamOp stream
    sendStreamTCP result metrics outputHost outputPort


-- A Link with 2 inputs
nodeLink2 :: (Store alpha, Store beta, Store gamma)
          => (Stream alpha -> Stream beta -> Stream gamma)
          -> ServiceName
          -> ServiceName
          -> HostName
          -> ServiceName
          -> IO ()
nodeLink2 streamOp inputPort1 inputPort2 outputHost outputPort = do
    sock1 <- listenSocket inputPort1
    sock2 <- listenSocket inputPort2
    metrics <- startPrometheus "node-link"
    putStrLn "Starting link ..."
    stream1 <- processSocket metrics sock1
    stream2 <- processSocket metrics sock2
    let result = streamOp stream1 stream2
    sendStreamTCP result metrics outputHost outputPort


--- SOURCE FUNCTIONS ---

nodeSource :: Store beta
           => IO alpha
           -> (Stream alpha -> Stream beta)
           -> HostName
           -> ServiceName
           -> IO ()
nodeSource pay streamGraph host port = do
    metrics <- startPrometheus "node-source"
    putStrLn "Starting source ..."
    stream <- readListFromSource pay metrics
    let result = streamGraph stream
    sendStreamTCP result metrics host port
    -- or printStream if it's a completely self contained streamGraph


--- MQTT FUNCTIONS ---

-- nodeSourceMqtt :: Store beta
--                => IO alpha
--                -> (Stream alpha -> Stream beta)
--                -> String
--                -> HostName
--                -> ServiceName
--                -> IO ()
-- nodeSourceMqtt pay streamOp nodeName host port = do
--     metrics <- startPrometheus nodeName
--     putStrLn "Starting source ..."
--     stream <- readListFromSource pay metrics
--     let result = streamOp stream
--     sendStreamMQTT result metrics nodeName host port


-- nodeLinkMqtt :: (Store alpha, Store beta)
--              => (Stream alpha -> Stream beta)
--              -> String
--              -> HostName
--              -> ServiceName
--              -> HostName
--              -> ServiceName
--              -> IO ()
-- nodeLinkMqtt streamOp nodeName inputHost inputPort outputHost outputPort = do
--     metrics <- startPrometheus nodeName
--     putStrLn "Starting link ..."
--     stream <- processSocketMQTT metrics nodeName inputHost inputPort
--     let result = streamOp stream
--     sendStreamTCP result metrics outputHost outputPort


--- KAFKA FUNCTIONS ---

-- nodeSourceKafka :: Store beta
--                 => IO alpha
--                 -> (Stream alpha -> Stream beta)
--                 -> String
--                 -> HostName
--                 -> ServiceName
--                 -> IO ()
-- nodeSourceKafka pay streamOp nodeName host port = do
--     metrics <- startPrometheus nodeName
--     putStrLn "Starting source ..."
--     stream <- readListFromSource pay metrics
--     let result = streamOp stream
--     sendStreamKafka result metrics nodeName host (read port)


-- nodeLinkKafka :: (Store alpha, Store beta)
--               => (Stream alpha -> Stream beta)
--               -> String
--               -> HostName
--               -> ServiceName
--               -> HostName
--               -> ServiceName
--               -> IO ()
-- nodeLinkKafka streamOp nodeName inputHost inputPort outputHost outputPort = do
--     metrics <- startPrometheus nodeName
--     putStrLn "Starting link ..."
--     stream <- processSocketKafka metrics nodeName inputHost (read inputPort)
--     let result = streamOp stream
--     sendStreamTCP result metrics outputHost outputPort

--- CONFIG FUNCTIONS ---

-- defaultConfig :: ServiceName -> HostName -> ServiceName -> StriotConfig
-- defaultConfig inPort outHost outPort = baseConfig (tcpConfig "" inPort) (tcpConfig outHost outPort)

-- baseConfig :: ConnectionConfig -> ConnectionConfig -> StriotConfig
-- baseConfig icc ecc = StriotConfig "" icc ecc 10

-- tcpConfig :: HostName -> ServiceName -> ConnectionConfig
-- tcpConfig host port = ConnTCPConfig $ TCPConfig $ NetConfig host port

-- mqttConfig :: String -> HostName -> ServiceName -> ConnectionConfig
-- mqttConfig topic host port = ConnMQTTConfig $ NT.MQTTConfig { _mqttConn  = NetConfig host port
--                                                             , _mqttTopic = topic }

-- defaultMqttConfig :: HostName -> ServiceName -> ConnectionConfig
-- defaultMqttConfig = mqttConfig "StriotQueue"

-- NEW NODE FUNCTIONS

nodeLinkNew :: (Store alpha, Store beta)
            => StriotConfig
            -> (Stream alpha -> Stream beta)
            -> IO ()
nodeLinkNew config streamOp = do
    ref <- newIORef Metrics {}
    let conf = set met ref config
    runReaderT (unApp $ nodeInternal streamOp) conf


nodeInternal :: (Store alpha, Store beta,
                MonadReader r m,
                HasStriotConfig r,
                MonadIO m)
             => (Stream alpha -> Stream beta) -> m ()
nodeInternal streamOp = do
    c <- ask
    liftIO $ writeIORef (c ^. met) =<< startPrometheus (c ^. nodeName)
    stream <- processInput
    let result = streamOp stream
    sendStream result


processInput :: (Store alpha,
                MonadReader r m,
                HasStriotConfig r,
                MonadIO m)
             => m (Stream alpha)
processInput = connectDispatch >>= (liftIO . U.getChanContents)


connectDispatch :: (Store alpha,
                   MonadReader r m,
                   HasStriotConfig r,
                   MonadIO m)
                => m (U.OutChan (Event alpha))
connectDispatch = do
    c <- ask
    liftIO $ do
        (inChan, outChan) <- U.newChan (c ^. chanSize)
        metrics <- readIORef $ c ^. met
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
              => Stream alpha
              -> m ()
sendStream stream = do
    c <- ask
    liftIO $ do
        metrics <- readIORef $ c ^. met
        sendDispatch (c ^. nodeName)
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


-- ==========================================================================
{- processSocket is a wrapper function that handles concurrently
accepting and handling connections on the socket and reading all of the strings
into an event Stream -}
-- processSocket :: Store alpha => Metrics -> Socket -> IO (Stream alpha)
-- processSocket met sock = U.getChanContents =<< acceptConnections met sock


-- {- acceptConnections takes a socket as an argument and spins up a new thread to
-- process the data received. The returned TChan object contains the data from
-- the socket -}
-- acceptConnections :: Store alpha => Metrics -> Socket -> IO (U.OutChan (Event alpha))
-- acceptConnections met sock = do
--     (inChan, outChan) <- U.newChan chanSize'
--     async $ connectionHandler met sock inChan
--     return outChan


-- {- We are using a bounded queue to prevent extreme memory usage when
-- input rate > consumption rate. This value may need to be increased to achieve
-- higher throughput when computation costs are low -}
-- chanSize' :: Int
-- chanSize' = 10


{- connectionHandler sits accepting any new connections. Once accepted, a new
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


--- MQTT ---

-- processSocketMqtt :: Store alpha => Metrics -> String -> HostName -> ServiceName -> IO (Stream alpha)
-- processSocketMqtt met nodeName host port = U.getChanContents =<< acceptConnectionsMqtt met nodeName host port


-- acceptConnectionsMqtt :: Store alpha => Metrics -> String -> HostName -> ServiceName -> IO (U.OutChan (Event alpha))
-- acceptConnectionsMqtt met nodeName host port = do
--     (inChan, outChan) <- U.newChan chanSize'
--     async $ runMQTTSub met nodeName mqttTopics host port inChan
--     return outChan


sendStreamMQTT :: Store alpha => String -> NT.MQTTConfig -> Metrics -> Stream alpha -> IO ()
sendStreamMQTT nodeName conf met stream = do
    mc <- runMQTTPub nodeName (conf ^. mqttConn . host) (conf ^. mqttConn . port)
    mapM_ (\x -> do
                    val <- E.evaluate . force . encode $ x
                    PC.inc (_egressEvents met)
                        >> PC.add (B.length val) (_egressBytes met)
                        >> publishq mc (read $ conf ^. mqttTopic) (BLC.fromStrict val) False QoS0 []) stream


runMQTTPub :: String -> HostName -> ServiceName -> IO MQTTClient
runMQTTPub nodeName host port =
    let (Just uri) = parseURI $ "mqtt://" ++ host ++ ":" ++ port
    in  connectURI (netmqttConf nodeName host port NoCallback) uri


runMQTTSub :: Store alpha => String -> NT.MQTTConfig -> Metrics -> U.InChan (Event alpha) -> IO ()
runMQTTSub nodeName conf met chan = do
    let h = conf ^. mqttConn . host
        p = conf ^. mqttConn . port
        (Just uri) = parseURI $ "mqtt://" ++ h ++ ":" ++ p
    mc <- connectURI (netmqttConf nodeName h p (SimpleCallback $ mqttMessageCallback met chan)) uri
    print =<< subscribe mc (map (\x -> (x, subOptions)) [read $ (conf ^. mqttTopic)]) []
    waitForClient mc


mqttMessageCallback :: Store alpha => Metrics -> U.InChan (Event alpha) -> MQTTClient -> Topic -> BLC.ByteString -> [Property] -> IO ()
mqttMessageCallback met chan mc topic msg _ =
    let bmsg = BLC.toStrict msg
    in  PC.inc (_ingressEvents met)
            >> PC.add (B.length bmsg) (_ingressBytes met)
            >> case decode bmsg of
                    Right event -> U.writeChan chan event
                    Left  _     -> return ()


-- mqttTopics :: [Topic]
-- mqttTopics = ["StriotQueue"]


netmqttConf :: String -> HostName -> ServiceName -> MessageCallback -> MQTT.MQTTConfig
netmqttConf nodeName host port msgCB =
    mqttConfig
        { MQTT._hostname = host
        , MQTT._port     = read port
        , _connID        = nodeName
        , _username      = Just "striot"
        , _password      = Just "striot"
        , _msgCB         = msgCB }


mqttSubOptions :: SubOptions
mqttSubOptions =
    subOptions
        { _retainHandling    = SendOnSubscribeNew
        , _retainAsPublished = True
        , _noLocal           = True
        , _subQoS            = QoS0 }


--- KAFKA ---

sendStreamKafka :: Store alpha => String -> KafkaConfig -> Metrics -> Stream alpha -> IO ()
sendStreamKafka nodeName conf met stream =
    E.bracket mkProducer clProducer runHandler >>= print
        where
          mkProducer              = PG.inc (_egressConn met)
                                    >> print "create new producer"
                                    >> newProducer (producerProps conf)
          clProducer (Left _)     = print "error close producer"
                                    >> return ()
          clProducer (Right prod) = PG.dec (_egressConn met)
                                    >> closeProducer prod
                                    >> print "close producer"
          runHandler (Left err)   = return $ Left err
          runHandler (Right prod) = print "runhandler producer"
                                    >> sendMessagesKafka prod (TopicName . read $ conf ^. kafkaTopic) stream met


kafkaConnectDelayMs :: Int
kafkaConnectDelayMs = 300000


producerProps :: KafkaConfig -> ProducerProperties
producerProps conf =
    KP.brokersList [BrokerAddress $ brokerAddress conf]
       <> KP.logLevel KafkaLogInfo


sendMessagesKafka :: Store alpha => KafkaProducer -> TopicName -> Stream alpha -> Metrics -> IO (Either KafkaError ())
sendMessagesKafka prod topic stream met = do
    mapM_ (\x -> do
            let val = encode x
            produceMessage prod (mkMessage topic Nothing (Just val))
                >> PC.inc (_egressEvents met)
                >> PC.add (B.length val) (_egressBytes met)
          ) stream
    return $ Right ()


mkMessage :: TopicName -> Maybe B.ByteString -> Maybe B.ByteString -> ProducerRecord
mkMessage topic k v =
    ProducerRecord
        { prTopic     = topic
        , prPartition = UnassignedPartition
        , prKey       = k
        , prValue     = v
        }


-- defaultKafkaTopic :: TopicName
-- defaultKafkaTopic = TopicName "striot-queue"


-- processSocketKafka :: Store alpha => Metrics -> String -> HostName -> PortNumber -> IO (Stream alpha)
-- processSocketKafka met nodeName host port = U.getChanContents =<< runKafkaConsumer met nodeName host port


runKafkaConsumer :: Store alpha => String -> KafkaConfig -> Metrics -> U.InChan (Event alpha) -> IO ()
runKafkaConsumer nodeName conf met chan = E.bracket mkConsumer clConsumer (runHandler chan)
    where
        mkConsumer                 = PG.inc (_ingressConn met)
                                     >> print "create new consumer"
                                     >> newConsumer (consumerProps conf)
                                                    (consumerSub $ TopicName . read $ conf ^. kafkaTopic) 
        clConsumer      (Left err) = print "error close consumer"
                                     >> return ()
        clConsumer      (Right kc) = void $ closeConsumer kc
                                          >> PG.dec (_ingressConn met)
                                          >> print "close consumer"
        runHandler _    (Left err) = print "error handler close consumer"
                                     >> return ()
        runHandler chan (Right kc) = print "runhandler consumer"
                                     >> processKafkaMessages met kc chan


processKafkaMessages :: Store alpha => Metrics -> KafkaConsumer -> U.InChan (Event alpha) -> IO ()
processKafkaMessages met kc chan = forever $ do
    threadDelay kafkaConnectDelayMs
    msg <- pollMessage kc (Timeout 50)
    either (\_ -> return ()) extractValue msg
      where
        extractValue m = maybe (print "kafka-error: crValue Nothing") writeRight (crValue m)
        writeRight   v = either (\err -> print $ "decode-error: " ++ show err)
                                (\x -> do
                                    PC.inc (_ingressEvents met)
                                        >> PC.add (B.length v) (_ingressBytes met)
                                    U.writeChan chan x)
                                (decode v)


consumerProps :: KafkaConfig -> ConsumerProperties
consumerProps conf =
    KC.brokersList [BrokerAddress $ brokerAddress conf]
         <> groupId (ConsumerGroupId . read $ conf ^. kafkaConGroup)
         <> KC.logLevel KafkaLogInfo
         <> KC.callbackPollMode CallbackPollModeSync


consumerSub :: TopicName -> Subscription
consumerSub topic = topics [topic]
                    <> offsetReset Earliest


brokerAddress :: KafkaConfig -> T.Text
brokerAddress conf = T.pack $ (conf ^. kafkaConn . host) ++ ":" ++ (conf ^. kafkaConn . port)
