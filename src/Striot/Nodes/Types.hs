{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}

module Striot.Nodes.Types where

import Data.IORef
import Control.Monad.Reader
import Control.Lens.Combinators (makeClassy)
import Control.Lens.TH
import System.Metrics.Prometheus.Metric.Gauge   (Gauge)
import System.Metrics.Prometheus.Metric.Counter (Counter)
import Network.Socket (ServiceName, HostName)
-- import Network.Mqtt.Topic ()


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
    }
makeLenses ''NetConfig

data TCPConfig = TCPConfig
    { _tcpConn :: NetConfig
    }
makeLenses ''TCPConfig

data KafkaConfig = KafkaConfig
    { _kafkaConn     :: NetConfig
    , _kafkaTopic    :: String
    , _kafkaConGroup :: String
    }
makeLenses ''KafkaConfig

data MQTTConfig = MQTTConfig
    { _mqttConn  :: NetConfig
    , _mqttTopic :: String
    }
makeLenses ''MQTTConfig
 
data ConnectionConfig = ConnTCPConfig TCPConfig
                      | ConnKafkaConfig KafkaConfig
                      | ConnMQTTConfig MQTTConfig

data StriotConfig = StriotConfig
    { _nodeName          :: String
    , _ingressConnConfig :: ConnectionConfig
    , _egressConnConfig  :: ConnectionConfig
    , _met               :: IORef Metrics
    , _chanSize          :: Int
    }
makeClassy ''StriotConfig

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