{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}

module Striot.LogicalOptimiser.RewriteRule ( LabelledRewriteRule(..)
                                           , RewriteRule(..)
                                           , lrule
                                           ) where

import Language.Haskell.TH
import Language.Haskell.TH.Syntax
import Test.Framework

import Striot.StreamGraph

type RewriteRule = StreamGraph -> Maybe (StreamGraph -> StreamGraph)

-- | A pairing of a `RewriteRule` with its name, encoded in a `String`.
data LabelledRewriteRule = LabelledRewriteRule
    { ruleLabel :: String
    , rule :: RewriteRule }

-- | convenience function so one can write `$(lrule 'someRule)` rather than
-- `LabelledRewriteRule "someRule" someRule`.
-- This function needs to live in a separate module from LogicalOptimiser due
-- to technical limitations with Template Haskell.
lrule :: Quasi m => Name -> m Exp
lrule name = return $
    ConE 'LabelledRewriteRule `AppE` ((LitE . StringL . nameBase) name) `AppE` VarE name
