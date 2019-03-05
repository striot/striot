{-# LANGUAGE DeriveGeneric #-}
module Striot.FunctionalIoTtypes where
import Data.Time (UTCTime) -- http://two-wrongs.com/haskell-time-library-tutorial
import GHC.Generics (Generic)
import Data.Aeson
import Data.Store

data Event alpha = Event { eventId :: Int
                         , time    :: Maybe Timestamp
                         , value   :: Maybe alpha}
     deriving (Eq, Ord, Show, Read, Generic)

type Timestamp       = UTCTime
type Stream alpha    = [Event alpha]

instance (FromJSON alpha) => FromJSON (Event alpha)

instance (ToJSON alpha) => ToJSON (Event alpha) where
    toEncoding = genericToEncoding defaultOptions

instance (Store alpha) => Store (Event alpha)

dataEvent :: Event alpha -> Bool
dataEvent (Event eid t (Just v)) = True
dataEvent (Event eid t Nothing)  = False

timedEvent :: Event alpha -> Bool
timedEvent (Event eid (Just t) v) = True
timedEvent (Event eid Nothing  v) = False
