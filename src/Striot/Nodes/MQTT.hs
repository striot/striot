module Striot.Nodes.MQTT
( sendStreamMQTT
, runMQTTSub
) where

import           Control.Concurrent.Chan.Unagi.Bounded    as U
import           Control.DeepSeq                          (force)
import qualified Control.Exception                        as E (evaluate)
import           Control.Lens
import qualified Data.ByteString                          as B (length)
import qualified Data.ByteString.Lazy.Char8               as BLC
import           Data.Store                               (Store, decode,
                                                           encode)
import           Network.MQTT.Client                      as MQTT hiding
                                                                   (Timeout)
import           Network.MQTT.Types                       (RetainHandling (..))
import           Network.Socket                           (HostName,
                                                           ServiceName)
import           Network.URI                              (parseURI)
import           Striot.FunctionalIoTtypes
import           Striot.Nodes.Types                       as NT
import           System.Metrics.Prometheus.Metric.Counter as PC (add, inc)
import           System.Metrics.Prometheus.Metric.Gauge   as PG (dec, inc)


sendStreamMQTT :: Store alpha => String -> NT.MQTTConfig -> Metrics -> Stream alpha -> IO ()
sendStreamMQTT name conf met stream = do
    mc <- runMQTTPub name (conf ^. mqttConn . host) (conf ^. mqttConn . port)
    mapM_ (\x -> do
                    val <- E.evaluate . force . encode $ x
                    PC.inc (_egressEvents met)
                        >> PC.add (B.length val) (_egressBytes met)
                        >> publishq mc (read $ conf ^. mqttTopic) (BLC.fromStrict val) False QoS0 []) stream


runMQTTPub :: String -> HostName -> ServiceName -> IO MQTTClient
runMQTTPub name host port =
    let (Just uri) = parseURI $ "mqtt://" ++ host ++ ":" ++ port
    in  connectURI (netmqttConf name host port NoCallback) uri


runMQTTSub :: Store alpha => String -> NT.MQTTConfig -> Metrics -> U.InChan (Event alpha) -> IO ()
runMQTTSub name conf met chan = do
    let h = conf ^. mqttConn . host
        p = conf ^. mqttConn . port
        (Just uri) = parseURI $ "mqtt://" ++ h ++ ":" ++ p
    mc <- connectURI (netmqttConf name h p (SimpleCallback $ mqttMessageCallback met chan)) uri
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


netmqttConf :: String -> HostName -> ServiceName -> MessageCallback -> MQTT.MQTTConfig
netmqttConf name host port msgCB =
    mqttConfig
        { MQTT._hostname = host
        , MQTT._port     = read port
        , _connID        = name
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
