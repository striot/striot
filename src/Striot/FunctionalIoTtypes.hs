module Striot.FunctionalIoTtypes where
import Data.Time (UTCTime) -- http://two-wrongs.com/haskell-time-library-tutorial

data Event alpha     =  E {id:: Int, time::Timestamp, value::alpha} |
                        T {id:: Int, time::Timestamp              } |
                        V {id:: Int,                  value::alpha} 
     deriving (Eq, Ord, Show, Read)

type Timestamp       = UTCTime
type Stream alpha    = [Event alpha]  
             
dataEvent:: Event alpha -> Bool
dataEvent (E id t v) = True
dataEvent (V id   v) = True
dataEvent (T id t  ) = False

timedEvent:: Event alpha -> Bool
timedEvent (E id t v) = True
timedEvent (V id   v) = False
timedEvent (T id t  ) = True
