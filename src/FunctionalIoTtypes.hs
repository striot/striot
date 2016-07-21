-- The following code is from examples/merge/server and Show and Read are necessary to
-- HandleConnections.hs to compile
module FunctionalIoTtypes where
import Data.Time (UTCTime) -- http://two-wrongs.com/haskell-time-library-tutorial

data Event alpha     =  E {time::Timestamp, value::alpha} |
                        T {time::Timestamp              } |
                        V {                 value::alpha}
     deriving (Eq, Ord, Show, Read)

--instance Show alpha => Show (Event alpha) where
--    show (E t v) = "(E " ++ show t ++ ":" ++ show v ++ ")" ++ "\n"
--    show (T t  ) = "(T " ++ show t                  ++ ")" ++ "\n"
--    show (V   v) = "(V " ++                  show v ++ ")" ++ "\n"

type Timestamp       = UTCTime
type Stream alpha    = [Event alpha]

dataEvent:: Event alpha -> Bool
dataEvent (E t v) = True
dataEvent (V   v) = True
dataEvent (T t  ) = False

timedEvent:: Event alpha -> Bool
timedEvent (E t v) = True
timedEvent (V   v) = False
timedEvent (T t  ) = True


-- The following is "newer code" according to PW email but HandleConnections.hs won't
-- compile with it.

--module FunctionalIoTtypes where
--import Data.Time (UTCTime) -- http://two-wrongs.com/haskell-time-library-tutorial
--
--data Event alpha     =  E {time::Timestamp, value::alpha} |
--                        T {time::Timestamp              } |
--                        V {                 value::alpha}
--     deriving (Eq, Ord)
--
--instance Show alpha => Show (Event alpha) where
--    show (E t v) = "(E " ++ show t ++ ":" ++ show v ++ ")" ++ "\n"
--    show (T t  ) = "(T " ++ show t                  ++ ")" ++ "\n"
--    show (V   v) = "(V " ++                  show v ++ ")" ++ "\n"
--
--type Timestamp       = UTCTime
--type Stream alpha    = [Event alpha]
--
--dataEvent:: Event alpha -> Bool
--dataEvent (E t v) = True
--dataEvent (V   v) = True
--dataEvent (T t  ) = False
--
--timedEvent:: Event alpha -> Bool
--timedEvent (E t v) = True
--timedEvent (V   v) = False
--timedEvent (T t  ) = True
