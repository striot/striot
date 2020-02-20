{-# LANGUAGE DeriveGeneric #-}
module Striot.FunctionalIoTtypes where
import           Data.Store
import           Data.Time    (UTCTime)
import           GHC.Generics (Generic)

data Event alpha = Event { time    :: Maybe Timestamp
                         , value   :: Maybe alpha}
     deriving (Eq, Ord, Show, Read, Generic)

type Timestamp       = UTCTime
type Stream alpha    = [Event alpha]

instance (Store alpha) => Store (Event alpha)

dataEvent :: Event alpha -> Bool
dataEvent (Event t (Just v)) = True
dataEvent (Event t Nothing)  = False

timedEvent :: Event alpha -> Bool
timedEvent (Event (Just t) v) = True
timedEvent (Event Nothing  v) = False
