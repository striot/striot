{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE TemplateHaskell #-}

-- demonstrate detecting over-utilisation


module OverUt where

import Algebra.Graph
import Data.Function ((&))
import Data.Maybe
import Striot.CompileIoT
import Striot.Jackson hiding (serviceTime)
import Striot.LogicalOptimiser
import Striot.StreamGraph
import Striot.VizGraph
import System.Random
import Test.Framework hiding ((===))

source = [| do
    i <- getStdRandom (randomR (1,10)) :: IO Int
    threadDelay 1000000
    putStrLn $ "client sending " ++ (show i)
    return i
    |]

graph = path
    [ StreamVertex 0 (Source 8)     [ source         ] "Int" "Int" 0
    , StreamVertex 1 (Filter (1/2)) [[|(>5)        |]] "Int" "Int" 0
    , StreamVertex 4 Merge          []                 "Int" "Int" (1/5)
    , StreamVertex 5 Sink           [[|mapM_ print |]] "Int" "Int" 0
    ]

-- the rewritten graph has the filter moved after the merge, so the
-- merge receives the full arrival rate into the system
test_match = assertBool $ isJust $ firstMatch graph filterMerge
rewritten  = graph & fromJust (firstMatch graph filterMerge)

test_opIds = assertEqual [0,1,4,5] $ map opId (calcAllSg graph)

isOverUtilised :: StreamGraph -> Bool
isOverUtilised = any (>1) . map util . calcAllSg

test_isOverUtilised = assertBool $ isOverUtilised rewritten

main = htfMain htf_thisModulesTests
