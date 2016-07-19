module STMcomms where
import Control.Monad.STM
import Control.Concurrent
import Control.Concurrent.STM.TChan
import FunctionalProcessing
import FunctionalIoTtypes
import Data.Time (UTCTime,NominalDiffTime,getCurrentTime)
import System.IO.Unsafe

-- https://en.wikibooks.org/wiki/Haskell/Concurrency
-- https://hackage.haskell.org/package/stm-2.4.4.1/docs/Control-Concurrent-STM-TChan.html 

oneSecond = 1000000

writerThread :: TChan Int -> IO ()
writerThread chan = do
        atomically $ writeTChan chan 1
        threadDelay oneSecond
        atomically $ writeTChan chan 2
        threadDelay oneSecond
        atomically $ writeTChan chan 3
        threadDelay oneSecond

readerThread :: Show alpha => TChan alpha -> IO ()
readerThread chan = do
        newMsg <- atomically $ readTChan chan
        putStrLn $ "read new value: " ++ show newMsg
        readerThread chan
        
clockStreamNamed:: TChan (Event String) -> String -> Int -> IO ()
clockStreamNamed chan message period = do -- period is in ms
                                         now <- getCurrentTime
                                         let newEvent = E now message                                   
                                         atomically $ writeTChan chan newEvent
                                         threadDelay (period*1000)
                                         clockStreamNamed chan message period  

clockStream:: TChan (Event String) -> Int -> IO ()
clockStream chan period = do -- period is in ms
                            now <- getCurrentTime
                            let newEvent = (T now)::Event String                             
                            atomically $ writeTChan chan newEvent
                            threadDelay (period*1000)
                            clockStream chan period                                 

handleConnectionsSink :: Show beta => TChan (Event alpha) -> (Stream alpha -> Stream beta) -> IO ()
handleConnectionsSink chan streamOps = do
                                         stream <- readListFromChannel chan                     -- read stream of Strings from STM channel
                                         let result = streamOps stream                          -- process stream
                                         printStream result                                     -- to print stream

handleConnectionsIntermediary :: Show beta => TChan (Event alpha) -> TChan (Event beta) -> (Stream alpha -> Stream beta) -> IO ()
handleConnectionsIntermediary inChan outChan streamOps = do
                                         stream <- readListFromChannel inChan                     -- read stream of Strings from STM channel
                                         let result = streamOps stream                            -- process stream
                                         sendStream outChan result      -- to print stream
                                        
readListFromChannel :: TChan (Event alpha) -> IO (Stream alpha)
readListFromChannel chan = do {l <- go chan; return l}
  where
    go chan   = do newMsg             <- atomically $ readTChan chan                                
                   r                  <- System.IO.Unsafe.unsafeInterleaveIO (go chan)
                   return (newMsg:r)

printStream:: Show alpha => Stream alpha -> IO ()
printStream (h:t) = do
                      putStrLn $ show h
                      printStream t
printStream []    = do
                      putStrLn "End of Stream"
                      
sendStream:: TChan (Event alpha) -> Stream alpha -> IO ()
sendStream chan (h:t) = do
                          atomically $ writeTChan chan h
                          sendStream chan t
                                         
main = do
        chan <- atomically $ newTChan
        forkIO $ clockStreamNamed chan "C1" 1000
        forkIO $ clockStreamNamed chan "C2" 1000
        forkIO $ handleConnectionsSink chan (\stream->streamWindow (chopTime 10) $ streamMap (\m->".."++m) $ stream)      
--      forkIO $ readerThread chan
        threadDelay $ 5 * oneSecond

test2 = do
        chan <- atomically $ newTChan
        forkIO $ clockStream chan 1000
        forkIO $ clockStream chan 1000
        forkIO $ handleConnectionsSink chan (\stream->streamWindow (chopTime 10) $ streamMap (\m->".."++m) $ stream)      
        threadDelay $ 5 * oneSecond

test3 = do
        chan  <- atomically $ newTChan
        chan2 <- atomically $ newTChan
        forkIO $ clockStream chan 1000
        forkIO $ clockStream chan 1000
        forkIO $ handleConnectionsIntermediary chan chan2 (\stream->streamWindow (chopTime 10) $ streamMap (\m->".."++m) $ stream) 
        forkIO $ handleConnectionsSink chan2 (\stream-> streamMap (\m->".2."++m) $ streamExpand $ stream)
        threadDelay $ 5 * oneSecond
        
------------- To Do-------------
-- test handleConnectionsIntermediary
-- time windowing using thread timers and STM functions (e.g. peek) 
