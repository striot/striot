{-# OPTIONS_HADDOCK prune #-}
{-|
Module      : Striot.Simple
Description : StrIoT Simple end-user interface
Copyright   : © StrIoT maintainers, 2021
License     : Apache 2.0
Maintainer  : StrIoT maintainers
Stability   : experimental

The StrIoT fundamental data types and low-level operators for
them. Import this module to write simple programs directly in
terms of these operators and types.

This module re-exports @Striot.FunctionalIoTtypes@ and
@Striot.FunctionalProcessing@, as well as three functions from
@Striot.Nodes@ for convenience.
-}
module Striot.Simple (
      module Striot.FunctionalIoTtypes
      -- $types
    , module Striot.FunctionalProcessing
      -- $processing
      --
    , mkStream
    , unStream
    , nodeSimple

        ) where

import Striot.FunctionalIoTtypes
import Striot.FunctionalProcessing
import Striot.Nodes

{- $types
the fundamental `Event` and related types for encapsulating data within
StrIoT.
 -}

{- $processing
The most basic stream processing functional primitives. These operate directly
upon the low-level `Stream` types. 
 -}
