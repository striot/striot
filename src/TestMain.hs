{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Main where

import Test.Framework

import {-@ HTF_TESTS @-} Striot.StreamGraph

import {-@ HTF_TESTS @-} Striot.CompileIoT
import {-@ HTF_TESTS @-} Striot.VizGraph

import Striot.FunctionalIoTtypes
import {-@ HTF_TESTS @-} Striot.FunctionalProcessing
import Striot.Nodes

import {-@ HTF_TESTS @-} Striot.LogicalOptimiser

import {-@ HTF_TESTS @-} Striot.Jackson

import {-@ HTF_TESTS @-} Striot.Partition

import {-@ HTF_TESTS @-} Striot.Orchestration

main = htfMain htf_importedTests
