{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE TemplateHaskell            #-}

module Striot.Nodes.Types where

import           Control.Lens                             ((^.))
import           Control.Lens.Combinators                 (makeClassy)
import           Control.Lens.TH
import           Control.Monad.Reader
import           Data.IORef
import           Network.Socket                           (HostName,
                                                           ServiceName)
import           System.Envy
import           System.Metrics.Prometheus.Metric.Counter (Counter)
import           System.Metrics.Prometheus.Metric.Gauge   (Gauge)


data Metrics = Metrics
    { _ingressConn   :: Gauge
    , _ingressBytes  :: Counter
    , _ingressEvents :: Counter
    , _egressConn    :: Gauge
    , _egressBytes   :: Counter
    , _egressEvents  :: Counter
    }

data NetConfig = NetConfig
    { _host :: HostName
    , _port :: ServiceName
    } deriving (Show)
makeLenses ''NetConfig

data TCPConfig = TCPConfig
    { _tcpConn :: NetConfig
    } deriving (Show)
makeLenses ''TCPConfig

data KafkaConfig = KafkaConfig
    { _kafkaConn     :: NetConfig
    , _kafkaTopic    :: String
    , _kafkaConGroup :: String
    } deriving (Show)
makeLenses ''KafkaConfig

data MQTTConfig = MQTTConfig
    { _mqttConn  :: NetConfig
    , _mqttTopic :: String
    } deriving (Show)
makeLenses ''MQTTConfig

data ConnectionConfig = ConnTCPConfig TCPConfig
                      | ConnKafkaConfig KafkaConfig
                      | ConnMQTTConfig MQTTConfig
                      deriving (Show)

data StriotConfig = StriotConfig
    { _nodeName          :: String
    , _ingressConnConfig :: ConnectionConfig
    , _egressConnConfig  :: ConnectionConfig
    , _chanSize          :: Int
    } deriving (Show)
makeClassy ''StriotConfig

instance ToEnv StriotConfig where
    toEnv StriotConfig {..} =
        makeEnv $
            [ "STRIOT_NODE_NAME" .= _nodeName
            , "STRIOT_CHAN_SIZE" .= _chanSize
            ] ++ writeConf INGRESS _ingressConnConfig
              ++ writeConf EGRESS  _egressConnConfig

writeConf :: ConnectType -> ConnectionConfig -> [EnvVar]
writeConf t (ConnTCPConfig   conf) =
    let base = "STRIOT_" ++ show t ++ "_"
    in  [ (base ++ "TYPE") .= "TCP"
        , (base ++ "HOST") .= (conf ^. tcpConn . host)
        , (base ++ "PORT") .= (conf ^. tcpConn . port)]
writeConf t (ConnKafkaConfig conf) =
    let base = "STRIOT_" ++ show t ++ "_"
    in  [ (base ++ "TYPE")            .= "KAFKA"
        , (base ++ "HOST")            .= (conf ^. kafkaConn . host)
        , (base ++ "PORT")            .= (conf ^. kafkaConn . port)
        , (base ++ "KAFKA_TOPIC")     .= (conf ^. kafkaTopic)
        , (base ++ "KAFKA_CON_GROUP") .= (conf ^. kafkaConGroup)]
writeConf t (ConnMQTTConfig  conf) =
    let base = "STRIOT_" ++ show t ++ "_"
    in  [ (base ++ "TYPE")       .= "MQTT"
        , (base ++ "HOST")       .= (conf ^. mqttConn . host)
        , (base ++ "PORT")       .= (conf ^. mqttConn . port)
        , (base ++ "MQTT_TOPIC") .= (conf ^. mqttTopic)]

instance FromEnv StriotConfig where
    fromEnv _ = StriotConfig
            <$> envMaybe "STRIOT_NODE_NAME" .!= "striot"
            <*> readConf INGRESS
            <*> readConf EGRESS
            <*> env "STRIOT_CHAN_SIZE"

readConf :: ConnectType -> Parser ConnectionConfig
readConf t = do
    let base = "STRIOT_" ++ show t ++ "_"
    p <- env (base ++ "TYPE")
    case p of
        "TCP"   -> ConnTCPConfig
                    <$> (TCPConfig
                            <$> nc base)
        "KAFKA" -> ConnKafkaConfig
                    <$> (KafkaConfig
                        <$> nc base
                        <*> env (base ++ "KAFKA_TOPIC")
                        <*> env (base ++ "KAFKA_CON_GROUP"))
        "MQTT"  -> ConnMQTTConfig
                    <$> (MQTTConfig
                        <$> nc base
                        <*> env (base ++ "MQTT_TOPIC"))

nc :: String -> Parser NetConfig
nc base = NetConfig
        <$> env (base ++ "HOST")
        <*> env (base ++ "PORT")

newtype StriotApp a =
    StriotApp {
        unApp :: ReaderT StriotConfig IO a
    } deriving (
        Functor,
        Applicative,
        Monad,
        MonadReader StriotConfig,
        MonadIO
    )

data ConnectType = INGRESS
                 | EGRESS
                 deriving (Show)

data ConnectProtocol = TCP
                     | KAFKA
                     | MQTT
