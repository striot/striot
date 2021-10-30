{-
 - demonstration of a simple stream-processing program to be
 - executed on a single node.
 -}

module Simple where

import Striot.Simple

import Control.Concurrent (threadDelay)
import System.Random (getStdRandom, randomR)

-- periodically generates a random Int
streamSrc :: IO Int
streamSrc = do
    i <- getStdRandom (randomR (1,10)) :: IO Int
    threadDelay (1000*1000)
    return i

streamSink :: Stream Int -> IO ()
streamSink = mapM_ print

-- a simple pure stream-processing program
streamFn :: Stream Int -> Stream Int
streamFn = streamFilter (<15)
         . streamMap (*2)

-- plumb the stream-processing program together with the
-- source and sink functions, on a single node.
main = nodeSimple streamSrc streamFn streamSink
