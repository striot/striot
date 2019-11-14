{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Main where

import Test.Framework
import {-@ HTF_TESTS @-} Striot.CompileIoT
import {-@ HTF_TESTS @-} VizGraph

import Striot.FunctionalIoTtypes
import {-@ HTF_TESTS @-} Striot.FunctionalProcessing
import Striot.Nodes

import {-@ HTF_TESTS @-} Striot.LogicalOptimiser

main = htfMain htf_importedTests
