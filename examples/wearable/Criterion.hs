{-# LANGUAGE TemplateHaskell #-}

import Control.DeepSeq
import Criterion.Main
import Data.Function ((&))
import Data.List.Split
import Data.Time.Calendar -- Day
import Data.Time -- UTCTime
import Striot.Simple
import System.IO.Strict (readFile)
import WearableExample

------------------------------------------------------------------------------

-- operator payloads to benchmark, adjusted to model a steady state deployment
-- and the behaviour of the operator upon receiving a new Event.

vibeFilter :: PebbleMode60  -> Bool
vibeFilter ((x,y,z),vibe) = vibe == 0

squares :: PebbleMode60  -> (Int,Int,Int)
squares ((x,y,z),_) = (x*x,y*y,z*z)

appIntSqrt :: (Int,Int,Int) -> Int
appIntSqrt (x,y,z) = intSqrt (x+y+z)

-- we measure the predicate but not the accumulator update function
-- XXX: replace threshold with something non-arbitrary
filterThresh :: (Int,Int) -> Bool
filterThresh = uncurry (\new last -> (last>threshold) && (new<=threshold))

chopTime120 :: Event a -> [Stream a]
chopTime120 = (\e -> chopTime 120 (snoc sampleList e))

------------------------------------------------------------------------------
-- implementation details for chopTime 120

epoch = UTCTime (fromGregorian 1900 1 1 :: Day) 0

nextTime start interval_ms = addUTCTime (toEnum interval) start
    where interval    = (interval_ms*10^9)

-- snoc is relatively expensive (O(n)) so we keep the buffer of existing
-- events as short as possible
snoc xs x = xs ++ [x]
sampleList = [ Event (Just epoch) Nothing ]
inEvent    = Event (Just (nextTime epoch 10))  Nothing :: Event Int
outEvent   = Event (Just (nextTime epoch 200)) Nothing :: Event Int

instance NFData a => NFData (Event a) where
  rnf (Event t d) = rnf1 t `seq` rnf1 d `seq` ()

------------------------------------------------------------------------------

main = do

  csvFile <- System.IO.Strict.readFile "session1.csv"

  let str = csvFile
          & lines
          & mkStream
          & streamMap parseSessionLine
          & streamWindow pebbleTimes
          & streamExpand
          & streamMap snd -- :: Stream PebbleMode60

  -- belt and braces
  print $ str `deepseq` "CSV loaded"

  defaultMain [
    bgroup "vibe"     [ bench "vibeFilterYes" $ nf vibeFilter ((0,0,0),0)
                      , bench "vibeFilterNo"  $ nf vibeFilter ((0,0,0),1)
                      ]
   {-
   ,bgroup "vibeIO"   [ bench "vibeFilter"    $ nf (map vibeFilter) (unStream str)
                      ]
    -}
   ,bgroup "squares"  [ bench "squares1"      $ nf squares ((1,1,1),0)
                      , bench "squares4"      $ nf squares ((4,4,4),0)
                      ]
   ,bgroup "intsqrt"  [ bench "intsqrt0"      $ nf appIntSqrt (0,0,0)
                      , bench "intsqrt1"      $ nf appIntSqrt (1,1,1)
                      , bench "intsqrt8"      $ nf appIntSqrt (8,8,8)
                      ] 
   ,bgroup "filterAcc"[ bench "filterAcc1"    $ nf filterThresh (50, 0)
                      , bench "filterAcc2"    $ nf filterThresh (50, 120)
                      , bench "filterAcc3"    $ nf filterThresh (150,120)
                      ]

    -- nf to fully evaluate constructors, ideally precalc away the cost of snoc
   ,bgroup "chopTime" [ bench "chopTimeIn"    $ nf chopTime120 inEvent
                      , bench "chopTimeOut"   $ nf chopTime120 outEvent
                      ]

   ,bgroup "length"   [ bench "length25"      $ nf length (take 25 [0..])
                      , bench "length24"      $ nf length (take 24 [0..])
                      ]
    ]
