{-# LANGUAGE DeriveGeneric #-}
module Striot.FunctionalIoTtypes where
import Data.Time (UTCTime) -- http://two-wrongs.com/haskell-time-library-tutorial
import GHC.Generics (Generic)
import Data.Aeson

data Event alpha     =  E {id :: Int, time :: Timestamp, value :: alpha} |
                        T {id :: Int, time :: Timestamp                } |
                        V {id :: Int,                    value :: alpha}
     deriving (Eq, Ord, Show, Read, Generic)

type Timestamp       = UTCTime
type Stream alpha    = [Event alpha]

instance (FromJSON alpha) => FromJSON (Event alpha)

instance (ToJSON alpha) => ToJSON (Event alpha) where
    toEncoding = genericToEncoding defaultOptions

dataEvent :: Event alpha -> Bool
dataEvent (E id t v) = True
dataEvent (V id v  ) = True
dataEvent (T id t  ) = False

timedEvent :: Event alpha -> Bool
timedEvent (E id t v) = True
timedEvent (V id v  ) = False
timedEvent (T id t  ) = True
