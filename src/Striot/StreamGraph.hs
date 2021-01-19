{-# LANGUAGE TemplateHaskell #-}
{-
 - Striot StreamGraph type, used for representing a stream processing program,
 - such that it can be re-written and partitioned.
 -}

module Striot.StreamGraph ( StreamGraph(..)
                          , StreamOperator(..)
                          , StreamVertex(..)
                          , PartitionedGraph(..)
                          , deQ
                          , isSource
                          , showParam
                          ) where

import Algebra.Graph
import Data.List (intercalate)
import Language.Haskell.TH
import System.IO.Unsafe (unsafePerformIO)
import Test.Framework -- Arbitrary, etc.

import Data.List.Split (splitOn)
-- SYB generic programming
import Data.Data
import Data.Generics.Schemes (everywhere)
import Data.Generics.Aliases (mkT)

-- |The `StreamOperator` and associated information required to encode a stream-processing
-- program into a Graph. Each distinct `StreamVertex` within a `StreamGraph` should have a
-- unique `vertexId` to ensure that they can be distinguished. For simple path-style graphs,
-- the IDs should be in ascending order.
data StreamVertex = StreamVertex
    { vertexId   :: Int
    , operator   :: StreamOperator
    , parameters :: [ExpQ]
    , intype     :: String
    , outtype    :: String
    , serviceTime:: Double
    }

instance Eq StreamVertex where
    a == b = and [ vertexId a == vertexId b
                 , operator a == operator b
                 , intype a   == intype b
                 , outtype a  == outtype b
                 , (map showParam (parameters a)) == (map showParam (parameters b))
                 ]

instance Show StreamVertex where
    show (StreamVertex i o ps inT outT s) =
        "StreamVertex " ++ intercalate " "
            [ show i
            , show o
            , show (map showParam ps)
            , show inT
            , show outT
            , show s
            ]

deQ :: Q Exp -> Exp
deQ = unsafePerformIO . runQ

showParam :: Q Exp -> String
showParam = pprint . unQualifyNames . deQ

-- | Walk over an Exp expression and replace all embedded Names with unqualified versions.
-- E.g. GHC.List.last => last. Special-handling for composition (.).
unQualifyNames :: Exp -> Exp
unQualifyNames = everywhere (\a -> mkT f a)
     where f :: Name -> Name
           f n = if n == '(.)
            then mkName "."
            else (mkName . last . splitOn "." . pprint) n

-- |A graph representation of a stream-processing program.
type StreamGraph = Graph StreamVertex

-- |A collection of partitioned StreamGraphs
type PartitionedGraph = ([StreamGraph], StreamGraph)

-- |An enumeration of the possible stream operators within a stream-processing program,
-- as well as `Source` and `Sink` to represent the ingress and egress points of programs.
data StreamOperator = Map
                    | Filter Double -- selectivity
                    | Expand
                    | Window
                    | Merge
                    | Join
                    | Scan
                    | FilterAcc Double -- selectivity
                    | Source Double -- arrival rate
                    | Sink
                    deriving (Show,Ord,Eq)

instance Ord StreamVertex where
    compare x y = compare (vertexId x) (vertexId y)

isSource :: StreamOperator -> Bool
isSource (Source _) = True
isSource _ = False

------------------------------------------------------------------------------
-- quickcheck experiment

instance Arbitrary StreamOperator where
    arbitrary = do
        d <- arbitrary
        elements [ Map , Filter d, Expand , Window , Merge , Join , Scan
                       , FilterAcc d, Source d, Sink ]

instance Arbitrary StreamVertex where
    arbitrary = do
        vertexId <- arbitrary
        operator <- arbitrary
        serviceT <- arbitrary
        let parameters = []
            ty = "String" in
            return $ StreamVertex vertexId operator parameters ty ty serviceT

streamgraph :: Gen StreamGraph
streamgraph = sized streamgraph'
streamgraph' 0 = return g where g = empty :: StreamGraph
streamgraph' n | n>0 = do
    v <- arbitrary
    t <- streamgraph' (n-1)
    return $ connect (vertex v) t
