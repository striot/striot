import Network
import System.IO
import Data.List
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes

main :: IO ()
main = nodeSink streamGraph1 printStream (9001::PortNumber)

{-
    streamFilterAcc example where the accumulator is simply a copy
    of the last (Int) value received and the filter rejects if the
    next value matches.
 -}
streamGraph1 :: Stream String -> Stream String
streamGraph1 = streamFilterAcc acc 0 test
    where
        test :: String -> Int -> Bool
        test s prev = (read s) /= prev
        acc :: Int -> String -> Int
        acc _ s = read s

printStream:: Show alpha => Stream alpha -> IO ()
printStream = mapM_ (\s -> putStrLn $ "receiving " ++ (show (value s)))
