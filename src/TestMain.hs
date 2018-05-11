{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Main where

import Test.Framework
import {-@ HTF_TESTS @-} Striot.CompileIoT
import {-@ HTF_TESTS @-} VizGraph

import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Nodes

main = htfMain htf_importedTests
