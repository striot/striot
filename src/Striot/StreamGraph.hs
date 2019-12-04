{-
 - Striot StreamGraph type, used for representing a stream processing program,
 - such that it can be re-written and partitioned.
 -}

module Striot.StreamGraph ( StreamGraph(..)
                          , StreamOperator(..)
                          , StreamVertex(..)
                          ) where

import Algebra.Graph
import Test.Framework -- Arbitrary, etc.

data StreamVertex = StreamVertex
    { vertexId   :: Int
    , operator   :: StreamOperator
    , parameters :: [String]       -- operator arguments (excluding the input stream)
    , intype     :: String
    , outtype    :: String
    } deriving (Eq,Show)

type StreamGraph = Graph StreamVertex

data StreamOperator = Map
                    | Filter
                    | Expand
                    | Window
                    | Merge
                    | Join
                    | Scan
                    | FilterAcc
                    | Source
                    | Sink
                    deriving (Ord,Eq)

instance Show StreamOperator where
    show Map             = "streamMap"
    show Filter          = "streamFilter"
    show Window          = "streamWindow"
    show Merge           = "streamMerge"
    show Join            = "streamJoin"
    show Scan            = "streamScan"
    show FilterAcc       = "streamFilterAcc"
    show Expand          = "streamExpand"
    show Source          = "streamSource"
    show Sink            = "streamSink"

instance Ord StreamVertex where
    compare x y = compare (vertexId x) (vertexId y)

------------------------------------------------------------------------------
-- quickcheck experiment

instance Arbitrary StreamOperator where
    arbitrary = elements [ Map , Filter , Expand , Window , Merge , Join , Scan
                         , FilterAcc , Source , Sink ]

instance Arbitrary StreamVertex where
    arbitrary = do
        vertexId <- arbitrary
        operator <- arbitrary
        let parameters = []
            ty = "String" in
            return $ StreamVertex vertexId operator parameters ty ty

streamgraph :: Gen StreamGraph
streamgraph = sized streamgraph'
streamgraph' 0 = return g where g = empty :: StreamGraph
streamgraph' n | n>0 = do
    v <- arbitrary
    t <- streamgraph' (n-1)
    return $ connect (vertex v) t
