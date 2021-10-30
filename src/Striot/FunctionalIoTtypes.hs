{-# LANGUAGE DeriveGeneric #-}
{-# OPTIONS_HADDOCK prune #-}
{-|
Module      : Striot.FunctionalIoTtypes
Description : StrIoT Functional IoT types
Copyright   : © StrIoT maintainers, 2021
License     : Apache 2.0
Maintainer  : StrIoT maintainers
Stability   : experimental

The StrIoT fundamental data types.
and functions that operate
on them. Import this module to write simple programs directly in
terms of these operators and types.
-}
module Striot.FunctionalIoTtypes where
import           Data.Store
import           Data.Time    (UTCTime)
import           GHC.Generics (Generic)

-- | The fundamental atom within StrIoT is the `Event` type, which
-- encapsulates either a datum, or the time of an occurence, or both.
data Event alpha = Event { time    :: Maybe Timestamp
                         , value   :: Maybe alpha}
     deriving (Eq, Ord, Show, Read, Generic)

type Timestamp       = UTCTime

-- | A `Stream` is simply a list of `Event`s.
type Stream alpha    = [Event alpha]

instance (Store alpha) => Store (Event alpha)

dataEvent :: Event alpha -> Bool
dataEvent (Event t (Just v)) = True
dataEvent (Event t Nothing)  = False

timedEvent :: Event alpha -> Bool
timedEvent (Event (Just t) v) = True
timedEvent (Event Nothing  v) = False
