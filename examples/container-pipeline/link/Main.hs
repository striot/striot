--import Network
--import Control.Concurrent
import System.IO
import FunctionalProcessing
import FunctionalIoTtypes
import Nodes

main :: IO ()
main = nodeLink streamGraph1

streamGraph1 :: Stream String -> Stream String
streamGraph1 s = streamMap (\st-> reverse st) s
