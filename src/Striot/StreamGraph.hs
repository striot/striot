{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell, FlexibleInstances #-}
{-|
Module      : Striot.StreamGraph
Description : StrIoT StreamGraph
Copyright   : © Jonathan Dowland, 2021
License     : Apache 2.0
Maintainer  : jon@dow.land
Stability   : experimental

StrIoT `StreamGraph` type, used for representing a stream-processing program,
such that it can be re-written, partitioned and translated into code in terms
of `Striot.FunctionalIoTTypes` for execution on distributed nodes.

 -}

module Striot.StreamGraph ( StreamGraph(..)
                          , StreamOperator(..)
                          , StreamVertex(..)
                          , PartitionedGraph(..)
                          , deQ
                          , isSource
                          , showParam

                          , simpleStream

                          -- QuickCheck generators
                          , streamgraph
                          , streamgraph'

                          -- Equality
                          , graphEq

                          , htf_thisModulesTests
                          ) where

import Algebra.Graph
import Data.List (intercalate)
import Data.Function ((&))
import Language.Haskell.TH
import System.IO.Unsafe (unsafePerformIO)
import Test.Framework -- Arbitrary, etc.

import Data.List.Split (splitOn)
-- SYB generic programming
import Data.Data
import Data.Generics.Schemes (everywhere)
import Data.Generics.Aliases (mkT)

import Data.Tree

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
    , serviceRate:: Double -- number of events processed per time interval
    }

instance Eq StreamVertex where
    a == b = and [ vertexId a == vertexId b
                 , operator a == operator b
                 , intype a   == intype b
                 , outtype a  == outtype b
                 , (map showParam (parameters a)) == (map showParam (parameters b))
                 , serviceRate a == serviceRate b
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

-- |Convenience function for specifying a simple path-style of stream
-- processing program, with no merge or join operations. The list of tuples are
-- converted into a series of connected Stream Vertices in a Graph. The tuple
-- arguments are the relevant `StreamOperator` for the node; the parameters;the
-- *output* type and the service time. The other parameters to `StreamVertex`
-- are inferred from the neighbouring tuples. Unique and ascending `vertexId`
-- values are assigned.
simpleStream :: [(StreamOperator, [ExpQ], String, Double)] -> Graph StreamVertex
simpleStream tupes = path lst

    where
        intypes = "IO ()" : (map (\(_,_,ty,_) -> ty) (init tupes))
        tupes3 = zip3 [1..] intypes tupes
        lst = map (\ (i,intype,(op,params,outtype,srate)) ->
            StreamVertex i op params intype outtype srate) tupes3


------------------------------------------------------------------------------
-- quickcheck experiment

-- never generates sources or sinks
instance Arbitrary StreamOperator where
    arbitrary = do
        d <- getPositive <$> arbitrary
        elements [Map, Filter d, Expand, Window, Merge, Join, Scan, FilterAcc d]

instance Arbitrary StreamVertex where
    arbitrary = do
        vertexId <- getPositive <$> arbitrary
        serviceT <- getPositive <$> arbitrary
        operator <- arbitrary

        let parameters = []
            ty = "?" in
            return $ StreamVertex vertexId operator parameters ty ty serviceT

streamgraph :: Gen StreamGraph
streamgraph = sized streamgraph'

streamgraph' 0 = return empty

streamgraph' n | n>=1 =
    treegraph' n >>= return . transpose . tree

-- requires FlexibleInstances
instance Arbitrary StreamGraph where
    arbitrary = streamgraph

------------------------------------------------------------------------------
-- Tree StreamVertex: private type for convenience of algorithms that fit Tree
-- better than Graph.

treegraph :: Gen (Tree StreamVertex)
treegraph = sized treegraph'

treegraph' :: Int -> Gen (Tree StreamVertex)
treegraph' 0 = error "can't represent an empty tree"
treegraph' 1 = arbitrary >>= \v ->
    return (Node (v { operator = Sink , vertexId = 0 }) [])

treegraph' n | n >1 = treegraph' 1 >>= extendTree (n-1)

chOp :: StreamOperator -> StreamVertex -> StreamVertex
chOp o x = x { operator = o }

chId :: Int -> StreamVertex -> StreamVertex
chId i x = x { vertexId = i }

extendTree :: Int -> Tree StreamVertex -> Gen (Tree StreamVertex)
extendTree 0 n = return n

-- only one more Node to create; it must be a source
extendTree 1 (Node v []) = do
    n   <- arbitrary
    src <- chId 1 <$> chOp (Source n) <$> arbitrary
    return $ Node v [Node src []]

extendTree n t@(Node v []) | n>1 = do
    case operator v of
        Join   -> extendJoin n t
        Merge  -> extendMerge n t
        _      -> do
            d    <- arbitrary
            let ops = [Map, Filter d, Expand, Window, Merge, Scan, FilterAcc d]
                    -- Join needs at least 3 nodes (two Sources) and cannot precede
                    -- Expand (type (a,b) ≠ [a])
            op   <- if   n < 3 || operator v == Expand
                    then elements ops
                    else elements (Join:ops)

            new  <- chOp op <$> chId n <$> arbitrary
            rest <- extendTree (n-1) (Node new [])
            return $ Node v [rest]

incrId :: Int -> StreamVertex -> StreamVertex
incrId i v = v { vertexId = vertexId v + i }

-- in the specialised extend* functions below, we can't generate
-- the branches from the real node of consideration or we'll loop
fakeroot = Node (StreamVertex 0 Sink [] "" "" 0) []

extendJoin :: Int -> Tree (StreamVertex) -> Gen (Tree StreamVertex)
extendJoin n (Node v@(StreamVertex _ Join _ _ _ _) []) = do
    let n1 = n `div` 2
    let n2 = n - n1

    Node _ children  <- extendTree n1 fakeroot
    Node _ children' <- fmap (fmap (incrId n1)) (extendTree n2 fakeroot)

    return $ Node v (children ++ children')

extendMerge :: Int -> Tree (StreamVertex) -> Gen (Tree StreamVertex)
extendMerge n (Node v@(StreamVertex _ Merge _ _ _ _) []) = do
    -- XXX extend to >2 incoming streams?
    let n1 = n `div` 2
    let n2 = n - n1

    -- XXX determine type from one child, force the other
    Node _ children  <- extendTree n1 fakeroot
    Node _ children' <- extendTree n2 fakeroot

    return $ Node v (children ++ (map (fmap (incrId n1)) children'))

-- for debugging in GHCi
draw :: Gen (Tree StreamVertex) -> IO ()
draw g = generate g >>= putStrLn . drawTree . fmap show

prop_noShortJoin = forAll (treegraph' 3) $
    not . elem Join . map operator . flatten

prop_noJoinExpand = forAll ((fmap (chOp Expand)) <$> (treegraph' 1) >>= extendTree 3) $
    (/=) Join . operator . rootLabel . head . subForest

------------------------------------------------------------------------------
-- `StreamGraph` (`Graph StreamVertex`) inherits its `Eq` instance from `Graph`,
-- which relies upon `Eq StreamVertex`. A problem we have with graph rewriting
-- is sometimes a graph variant is effectively equivalent but has different
-- vertexId values. We cannot address this for `Eq` so we instead provide `graphEq`.
--
--  `graphEq` relies on a `StreamProg` type to ignore vertexId renumbering.
--  `StreamProg` does not have a `vertexId` field, instead the graph
--  structure is captured in the ADT itself.

data StreamProg = StreamProg StreamOperator [Exp] String String Double [StreamProg]
    deriving (Show, Eq)

-- since Eq StreamGraph is derived via `Graph a`, we cannot provide an Eq which
-- avoids the vertexId problem.
graphEq :: StreamGraph -> StreamGraph -> Bool
graphEq a b = (fromStreamGraph a) == (fromStreamGraph b)

fromStreamGraph :: StreamGraph -> StreamProg
fromStreamGraph sg = let
    sink = (snd . head . filter ((==) Sink . operator . snd) . edgeList) sg
    in fromStreamGraph' sg sink

fromStreamGraph' :: StreamGraph -> StreamVertex -> StreamProg
fromStreamGraph' sg v = let
    incoming = (map fst . filter ((==) v . snd) . edgeList) sg
    in fromStreamVertex v $ map (fromStreamGraph' sg) incoming

-- construct a partially-applied StreamProg from a StreamVertex; lacking the
-- final parent StreamProg parameter.
fromStreamVertex :: StreamVertex -> ([StreamProg] -> StreamProg)
fromStreamVertex (StreamVertex _ o p it ot s) = StreamProg o (map (unQualifyNames.deQ) p) it ot s

-- test data
sample1d = StreamProg (Source 1)   [deQ [| sourceFn |]] "IO ()" "Int"      0 []
         & StreamProg (Filter 0.5) [deQ [| (>5) |]]     "Int"   "Int"      1 .(:[])
         & StreamProg (Filter 0.5) [deQ [| (<8) |]]     "Int"   "Int"      1 .(:[])
         & StreamProg Window       [deQ [| chop 1 |]]   "Int"   "[Int]"    1 .(:[])
         & StreamProg Sink         [deQ [| sinkFn |]]   "[Int]" "[String]" 0 .(:[])

sample1 = simpleStream
  [ ((Source 1) , [[| sourceFn |]], "Int", 0)
  , ((Filter 0.5), [[| (>5) |]], "Int", 1)
  , ((Filter 0.5), [[| (<8) |]], "Int", 1)
  , (Window , [[| \n -> par n (chop 1) |]], "[Int]", 1)
  , (Sink   , [[|sinkFn|]], "[String]", 0)
  ]
-- permutation of sample1 with vertexIds incremented
sample2 = fmap (\v -> v { vertexId = vertexId v + 1 }) sample1
-- different structure
sample3 = simpleStream
  [ ((Source 1) , [[| sourceFn |]], "Int", 0)
  , ((Filter 0.5), [[|\i -> i>5 && i<8 |]], "Int", 1)
  , (Window , [[| chop 1 |]], "[Int]", 1)
  , (Sink   , [[|sinkFn|]], "[String]", 0)
  ]
-- Eq fails when the vertexIds differ,
test_vertexId_notEq = assertNotEqual sample1 sample2
-- but graphEq succeeds
test_vertexId_graphEq = assertBool $ graphEq sample1 sample2
-- sample3 is clearly different
test_eq_diffStructure = assertNotEqual sample1 sample2
-- and remains so in StreamProg form
test_graphEq_diffStructure = assertBool $ not $ graphEq sample1 sample3
