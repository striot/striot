import Control.Concurrent (threadDelay)
import Striot.FunctionalProcessing
import Striot.FunctionalIoTtypes
import Striot.Nodes
import Network
import System.Random (getStdRandom, randomR)
import Control.Monad (replicateM)

main :: IO ()
main = do
    threadDelay 1000000
    nodeSource src streamGraph ("haskellserver"::HostName) (9001::PortNumber)

src :: IO String
src = do
    indices <- replicateM 10 (getStdRandom (randomR (0,39)) :: IO Int)

    let s = unwords $ map (random_words !!) indices in do
        threadDelay 1000000
        return s

streamGraph :: Stream String -> Stream [String]
streamGraph = streamMap get_hashtags

get_hashtags :: String -> [String]
get_hashtags s = filter (('#'==).head) (words s)

-- 40 words picked randomly from the dictionary, 20 of which are prefixed
-- with # to simulate hashtags
random_words = words "Angelica #Seine #sharpened sleeve consonance diabolically\
\ #bedlam #sharpener sentimentalizing amperage #quilt Ahmed #quadriceps Mia\
\ #burglaries constricted julienne #wavier #gnash #blowguns wiping somebodies\
\ nematode metaphorical Chablis #taproom disrespects #oddly ideograph rotunda\
\ #verdigrised #blazoned #murmuring #clover #saguaro #sideswipe faulted brought\
\ #Selkirk #Kshatriya"
