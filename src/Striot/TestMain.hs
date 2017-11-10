{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Main where

import Test.Framework
import {-@ HTF_TESTS @-} Striot.CompileIoT

main = htfMain htf_importedTests
