import Network
import System.IO
import Data.List
import FunctionalProcessing
import FunctionalIoTtypes
import HandleConnections

main :: IO ()
main = handleConnectionsSink streamGraph1

streamGraph1 :: Stream String -> Stream [String]
streamGraph1 s = streamWindow (chop 2) $ streamFilter (\s-> length s >10) $ streamMap (\s-> "Is mapped to:" ++ s) s
