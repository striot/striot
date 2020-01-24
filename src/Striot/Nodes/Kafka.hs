module Striot.Nodes.Kafka
( sendStreamKafka
, runKafkaConsumer
) where

import           Control.Concurrent                       (threadDelay)
import           Control.Concurrent.Chan.Unagi.Bounded    as U
import qualified Control.Exception                        as E (bracket)
import           Control.Lens
import           Control.Monad                            (forever, void)
import qualified Data.ByteString                          as B (ByteString,
                                                                length)
import           Data.Store                               (Store, decode,
                                                           encode)
import           Data.Text                                as T (Text, pack)
import           Kafka.Consumer                           as KC
import           Kafka.Producer                           as KP
import           Striot.FunctionalIoTtypes
import           Striot.Nodes.Types
import           System.Metrics.Prometheus.Metric.Counter as PC (add, inc)
import           System.Metrics.Prometheus.Metric.Gauge   as PG (dec, inc)


sendStreamKafka :: Store alpha => String -> KafkaConfig -> Metrics -> Stream alpha -> IO ()
sendStreamKafka name conf met stream =
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
                                    >> sendMessagesKafka prod (TopicName . T.pack $ conf ^. kafkaTopic) stream met


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


runKafkaConsumer :: Store alpha => String -> KafkaConfig -> Metrics -> U.InChan (Event alpha) -> IO ()
runKafkaConsumer name conf met chan = E.bracket mkConsumer clConsumer (runHandler chan)
    where
        mkConsumer                 = PG.inc (_ingressConn met)
                                     >> print "create new consumer"
                                     >> newConsumer (consumerProps conf)
                                                    (consumerSub $ TopicName . T.pack $ conf ^. kafkaTopic)
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
         <> groupId (ConsumerGroupId . T.pack $ conf ^. kafkaConGroup)
         <> KC.logLevel KafkaLogInfo
         <> KC.callbackPollMode CallbackPollModeSync


consumerSub :: TopicName -> Subscription
consumerSub topic = topics [topic]
                    <> offsetReset Earliest


brokerAddress :: KafkaConfig -> T.Text
brokerAddress conf = T.pack $ (conf ^. kafkaConn . host) ++ ":" ++ (conf ^. kafkaConn . port)
