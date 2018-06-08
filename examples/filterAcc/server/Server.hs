import Network.Socket (ServiceName)
import System.IO
import Data.List
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes

main :: IO ()
main = nodeSink streamGraph1 printStream ("9001"::ServiceName)

{-
    streamFilterAcc example where the accumulator is simply a copy
    of the last (Int) value received and the filter rejects if the
    next value matches.
 -}
streamGraph1 :: Stream String -> Stream String
streamGraph1 s = (head s):(streamFilterAcc (\_ s -> s) (value $ head s) (/=) (tail s))

printStream:: Show alpha => Stream alpha -> IO ()
printStream = mapM_ (\s -> putStrLn $ "receiving " ++ (show (value s)))
