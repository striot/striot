-- node2
import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Nodes
import Striot.Nodes.Types
import System.Envy
import Control.Concurrent


streamGraphFn :: Stream String -> Stream String
streamGraphFn n1 = let
    n2 = (\s -> streamMap reverse s) n1
    in n2


main :: IO ()
main = do
    conf <- decodeEnv :: IO (Either String StriotConfig)
    case conf of
        Left _  -> print "Could not read from env"
        Right c -> nodeLink c streamGraphFn