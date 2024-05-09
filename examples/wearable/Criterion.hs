{-# LANGUAGE TemplateHaskell #-}


import WearableExample
import Criterion.Main
import Data.Time -- UTCTime
import Data.Time.Calendar -- Day
import Data.Function ((&))
import Striot.Simple

jan_1_1900_day = fromGregorian 1900 1 1 :: Day
jan_1_1900_time = UTCTime jan_1_1900_day 0

nextTime start = addUTCTime (toEnum interval) start
    where interval_ms = 10
          interval    = (interval_ms*10^9)

-- a stream where each Event arrives 10ms later
stream = zip (iterate (+10) 0) (iterate nextTime jan_1_1900_time)
       & map (\(d,t) -> Event (Just t) (Just d))

-- when to use nf versus whnf?
main = defaultMain [
  bgroup "wearable" [ bench "vibeFilterYes" $ whnf (\((x,y,z),vibe)->vibe == 0) ((0,0,0),0)
                    , bench "vibeFilterNo"  $ whnf (\((x,y,z),vibe)->vibe == 0) ((0,0,0),1)
                    ]
 ,bgroup "squares"  [ bench "squares1"      $ whnf (\((x,y,z),_) -> (x*x,y*y,z*z)) ((1,1,1),0)
                    , bench "squares4"      $ whnf (\((x,y,z),_) -> (x*x,y*y,z*z)) ((4,4,4),0)
                    ]
 ,bgroup "intsqrt"  [ bench "intsqrt0"      $ whnf (\(x,y,z)-> intSqrt (x+y+z)) (0,0,0)
                    , bench "intsqrt1"      $ whnf (\(x,y,z)-> intSqrt (x+y+z)) (1,1,1)
                    , bench "intsqrt8"      $ whnf (\(x,y,z)-> intSqrt (x+y+z)) (8,8,8)
                    ] 
 ,bgroup "filterAcc"[ bench "filterAcc"     $ whnf (streamFilterAcc (\last new -> new) 0 (\new last -> (last>threshold) && (new<=threshold))) (mkStream [50])
                    ]
 
 -- what am I evaluating? what cost does this tell me?
 ,bgroup "window"   [ bench "window"        $ whnf (streamWindow (chopTime 120)) (take 1000 stream)
                    ]
 ,bgroup "length"   [ bench "length25"      $ whnf length (take 25 [0..])
                    , bench "length24"      $ whnf length (take 24 [0..])
                    ]
  ]
