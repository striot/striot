{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# OPTIONS_HADDOCK prune #-}
{-# LANGUAGE TemplateHaskell #-}

module Striot.LogicalOptimiser ( applyRules

                               , applyRule
                               , firstMatch
                               , rewriteGraph

                               , Variant(..)
                               , variantSequence

                               -- $fromRewriteRule
                               , RewriteRule(..)
                               , LabelledRewriteRule(..)

                               , pureRules
                               , reorderingRules
                               , reshapingRules
                               , defaultRewriteRules

                               , mapFilter
                               , filterFilterAcc
                               , filterAccFilter
                               , filterAccFilterAcc
                               , filterFuse
                               , mapFuse
                               , mapScan
                               , expandFilter
                               , mapFilterAcc
                               , mapWindow
                               , expandMap
                               , expandScan
                               , expandExpand
                               , mergeFilter
                               , mergeExpand
                               , mergeMap
                               , mapMerge
                               , filterMerge
                               , expandMerge
                               , mergeFuse
                               , expandFilterAcc

                               , filterWindow
                               , filterAccWindow

                               , htf_thisModulesTests
                               ) where

import Striot.StreamGraph
import Striot.LogicalOptimiser.RewriteRule
import Striot.FunctionalProcessing
import Algebra.Graph
import Test.Framework hiding ((===))
import Data.Char (isLower)
import Data.Maybe (mapMaybe, fromMaybe)
import Data.Function ((&))
import Data.List (nub, sort, intercalate)
import Control.Arrow ((>>>))

------------------------------------------------------------------------------

-- | A variant (`variantGraph`) of a stream-processing program (`variantParent`),
-- produced by applying a rewrite rule (`variantRule`). The `Original` constructor
-- is to represent the original, unmodified, input stream-processing program.
data Variant = Variant
    { variantGraph :: StreamGraph
    , variantRule  :: String
    , variantParent:: Variant
    } | Original
    { variantGraph :: StreamGraph
    } deriving (Show, Eq)

-- | Extract a list of `RewriteRule` labels from a `Variant`, to see the sequence
-- of rules that produced it.
variantSequence :: Variant -> [String]
variantSequence = reverse . variantSequence'
variantSequence' (Original _) = []
variantSequence' (Variant _ r p) = r:(variantSequence' p)

applyRule :: RewriteRule -> StreamGraph -> StreamGraph
applyRule f g = g & fromMaybe id (firstMatch g f)

-- recursively attempt to apply the rule to the graph, but stop
-- as soon as we get a match
firstMatch :: StreamGraph -> RewriteRule -> Maybe (StreamGraph -> StreamGraph)
firstMatch g r = case r g of
        Just f -> Just f
        _      -> case g of
            Empty       -> Nothing
            Vertex _    -> Nothing
            Overlay a b -> tryLeftThenRight a b r
            Connect a b -> tryLeftThenRight a b r
        where
            tryLeftThenRight a b r = case firstMatch a r of
                                Just f  -> Just f
                                Nothing -> firstMatch b r

-- | Apply a list of `LabelledRewriteRule`s to a `Variant` stream program
-- to derive rewritten programs. Bound the depth of rule application by
-- the supplied parameter.
-- The resulting list may include many equivalent `StreamGraph`s: the caller
-- may wish to use `nub . variantGraph` or `nubBy (on (==) variantGraph)` to
-- de-duplicate.
applyRules :: [LabelledRewriteRule] -> Int -> Variant -> [Variant]
applyRules lrules n v =
    if n < 1 then [v]
    else let
        sg = variantGraph v
        vs = map (\(l,f) -> Variant (f sg) l v)
           $ mapMaybe (\(LabelledRewriteRule l r) -> fmap ((,) l) (firstMatch sg r))
             lrules
        in v : vs ++ (concatMap (applyRules lrules (n-1)) vs)

-- | Convenience function to call `applyRules` with an initial `StreamGraph`,
-- encoding a sensible depth limit of 5.
rewriteGraph :: [LabelledRewriteRule] -> StreamGraph -> [Variant]
rewriteGraph rs = applyRules rs 5 . Original

------------------------------------------------------------------------------

-- | Semantically-preserving rules. XXX pureRules is not a good name
pureRules :: [LabelledRewriteRule]
pureRules =
        [ $(lrule 'filterFuse)
        , $(lrule 'mapFilter)
        , $(lrule 'filterFilterAcc)
        , $(lrule 'filterAccFilter)
        , $(lrule 'filterAccFilterAcc)
        , $(lrule 'mapFuse)
        , $(lrule 'mapScan)
        , $(lrule 'expandFilter)
        , $(lrule 'mapFilterAcc)
        , $(lrule 'mapWindow)
        , $(lrule 'expandMap)
        , $(lrule 'expandScan)
        , $(lrule 'expandExpand)
        , $(lrule 'mergeMap)
        , $(lrule 'mapMerge)
        , $(lrule 'expandFilterAcc)
        ]

-- | A list of rules which cause Stream re-ordering.
-- These are included in 'defaultRewriteRules'.
reorderingRules =
    [ $(lrule 'filterMerge)
    , $(lrule 'expandMerge)
    , $(lrule 'mergeFilter)
    , $(lrule 'mergeExpand)
    , $(lrule 'mergeFuse)
    ]

-- | A list of rules which cause re-shaping of Windows.
-- These are not included in 'defaultRewriteRules'.
reshapingRules =
    [ $(lrule 'filterWindow)
    , $(lrule 'filterAccWindow)
    ]

defaultRewriteRules =
    pureRules ++ reorderingRules

-- streamFilter f >>> streamFilter g = streamFilter (\x -> f x && g x) -------

filterFuse :: RewriteRule
filterFuse (Connect (Vertex a@(StreamVertex i (Filter sel1) (p:_) ty _ s1))
                    (Vertex b@(StreamVertex _ (Filter sel2) (q:_) _ _ s2))) =
    let c = a { operator    = Filter (sel1 * sel2)
              , parameters  = [[| (\p q x -> p x && q x) $(p) $(q) |]]
              , serviceRate = sumRates s1 sel1 s2
              }
    in Just (removeEdge c c . mergeVertices (`elem` [a,b]) c)

filterFuse _ = Nothing

gt3 = [| (>3) |]
lt5 = [| (<5) |]

so' = StreamVertex 0 (Source 1)   []    "String" "String" 1
f3  = StreamVertex 1 (Filter 0.5) [gt3] "String" "String" 10
f4  = StreamVertex 2 (Filter 0.5) [lt5] "String" "String" 5
si' = StreamVertex 3 Sink         []    "String" "String" 1

fused = [| (\p q x -> p x && q x) (>3) (<5) |]

filterFusePre = path [so', f3, f4, si']

filterFusePost = path [ so'
    , StreamVertex 1 (Filter 0.25) [fused] "String" "String" 5
    , si' ]

test_filterFuse = assertEqual (applyRule filterFuse filterFusePre)
    filterFusePost

-- streamMap f >>> streamFilter p = streamFilter (f >>> p) >>> streamMap f ---
-- TODO: should we decrease the service rate of the filter?

mapFilter :: RewriteRule
mapFilter (Connect (Vertex m@(StreamVertex i Map (f:_) intype _ sm))
                   (Vertex f1@(StreamVertex j (Filter sel) (p:_) _ _ sf))) =

    let f2 = StreamVertex i (Filter sel) [[| $(p) . $(f) |]] intype intype (sm+sf)
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilter _ = Nothing

m1 = StreamVertex 1 Map [[| show |]] "Int" "String" 1
f1 = StreamVertex 2 (Filter 0.5) [[| \x -> length x <3 |]] "String" "String" 1

f2 = StreamVertex 1 (Filter 0.5) [[| (\x -> length x <3) . (show) |]] "Int" "Int" 2
m2 = StreamVertex 2 Map [[| show |]] "Int" "String" 1

so = StreamVertex 0 (Source 1) [] "Int" "Int" 1
si = StreamVertex 3 Sink [] "String" "String" 1

mapFilterPre  = path [ so, m1, f1, si ]
mapFilterPost = path [ so, f2, m2, si ]

test_mapfilter2 = assertEqual mapFilterPost
    $ applyRule mapFilter mapFilterPre

-- test it finds matches in sub-graphs
mapFilterSub = mapFilterPre `Overlay` Empty
test_mapfilter3 = assertEqual mapFilterPost
    $ applyRule mapFilter mapFilterSub

-- deeper sub-graphs and some redundancy
mapFilterSub2 = Empty `Overlay` Empty `Overlay` mapFilterPre `Overlay` mapFilterPre
test_mapfilter4 = assertEqual mapFilterPost
    $ applyRule mapFilter mapFilterSub2

-- vertex ordering is preserved
test_mapFilter_ord = assertBool $ sorted $ map vertexId (vertexList mapFilterPost)

sorted :: Ord a => [a] -> Bool
sorted [] = True
sorted [x] = True
sorted (x:y:zz) = (x <= y) && sorted (y:zz)

-- streamFilter >>> streamFilterAcc f a q ------------------------------------

filterFilterAcc :: RewriteRule
filterFilterAcc (Connect (Vertex v1@(StreamVertex i (Filter sel1) (p:_) ty _ s1))
                         (Vertex v2@(StreamVertex _ (FilterAcc sel2) (f:a:q:_) _ _ s2))) =
    let v3 = StreamVertex i (FilterAcc (sel1*sel2))
          [ [| \a v -> if $(p) v then $(f) a v else a |]
          , a
          , [| \v a -> $(p) v && $(q) v a |] ] ty ty (sumRates s1 sel1 s2)
    in  Just (removeEdge v3 v3 . mergeVertices (`elem` [v1,v2]) v3)
filterFilterAcc _ = Nothing

filterFilterAccPre = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 (Filter 0.5) [p] "Int" "Int" 10
    , StreamVertex 2 (FilterAcc 0.5) [f , a , q] "Int" "Int" 5
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where p = [| (>3) |]
          f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          q = [| \new (b,old) -> b || old /= new |]

filterFilterAccPost = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.25)
        [ [| \a v -> if $(p) v then $(f) a v else a |]
        , a
        , [| \v a -> $(p) v && $(q) v a |]
        ] "Int" "Int" 5
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where p = [| (>3) |]
          f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          q = [| \new (b,old) -> b || old /= new |]

test_filterFilterAcc = assertEqual (applyRule filterFilterAcc filterFilterAccPre)
    filterFilterAccPost

-- streamFilterAcc >>> streamFilter ------------------------------------------

filterAccFilter :: RewriteRule
filterAccFilter (Connect (Vertex v1@(StreamVertex i (FilterAcc sel1) (f:a:p:_) ty _ s1))
                         (Vertex v2@(StreamVertex _ (Filter sel2) (q:_) _ _ s2))) =
    let p' = [| \v a -> $(p) v a && $(q) v |]
        v  = StreamVertex i (FilterAcc (sel1*sel2)) [f,a,p'] ty ty (sumRates s1 sel1 s2)
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilter _ = Nothing

filterAccFilterPre = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.5) [f,a,p] "Int" "Int" 10
    , StreamVertex 2 (Filter 0.5)    [q] "Int" "Int" 5
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          p = [| \new (b,old) -> b || old /= new |]
          q = [| (>3) |]

filterAccFilterPost = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.25) [f, a, p'] "Int" "Int" 5
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          p = [| \new (b,old) -> b || old /= new |]
          q = [| (>3) |]
          p'= [| \v a -> $(p) v a && $(q) v |]

test_filterAccFilter = assertEqual (applyRule filterAccFilter filterAccFilterPre)
    filterAccFilterPost

-- streamFilterAcc >>> streamFilterAcc ---------------------------------------



filterAccFilterAcc :: RewriteRule
filterAccFilterAcc (Connect (Vertex v1@(StreamVertex i (FilterAcc sel1) (f:a:p:ss) ty _ s1))
                            (Vertex v2@(StreamVertex _ (FilterAcc sel2) (g:b:q:_) _ _ s2))) =
    let f' = [| \ (a,b) v -> ($(f) a v, if $(p) v a then $(g) b v else b) |]
        a' = [| ($(a), $(b)) |]
        q' = [| \v (y,z) -> $(p) v y && $(q) v z |]
        v  = StreamVertex i (FilterAcc (sel1*sel2)) (f':a':q':ss) ty ty (sumRates s1 sel1 s2)
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilterAcc _ = Nothing

filterAccFilterAccPre = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.5) [f,a,p] "Int" "Int" 10
    , StreamVertex 2 (FilterAcc 0.5) [g,b,q] "Int" "Int" 5
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where
        -- remove repeating elements
        f = [| (\_ h -> (False, h)) |]
        a = [| (True, undefined) |]
        p = [| \new (b,old) -> b || old /= new |]
        -- increasing +ve values only
        g = [| \_ v -> v |]
        b = [| 0 |]
        q = [| (>=) |]

filterAccFilterAccPost = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.25)
        [ [| \(a,b) v -> ($(f) a v, if $(p) v a then $(g) b v else b) |]
        , [| ($(a),$(b)) |]
        , [| \v (y,z) -> $(p) v y && $(q) v z |]
        ] "Int" "Int" 5
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          p = [| \new (b,old) -> b || old /= new |]
          g = [| \_ v -> v |]
          b = [| 0 |]
          q = [| (>=) |]

test_filterAccFilterAcc = assertEqual (applyRule filterAccFilterAcc filterAccFilterAccPre)
    filterAccFilterAccPost

-- streamMap >>> streamMap ---------------------------------------------------

mapFuse :: RewriteRule
mapFuse (Connect (Vertex v1@(StreamVertex i Map (f:ss) t1 _ s1))
                 (Vertex v2@(StreamVertex _ Map (g:_) _ t2 s2))) =
    let v = StreamVertex i Map ([| $(f) >>> $(g) |]:ss) t1 t2 (sumRates s1 1 s2)
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapFuse _ = Nothing

mapFusePre = path
    [ StreamVertex 0 (Source 1) [] "String" "String" 1
    , StreamVertex 1 Map [[| show |]] "Int" "String" 1
    , StreamVertex 2 Map [[| length |]] "String" "Int" 1
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]

mapFusePost = path
    [ StreamVertex 0 (Source 1) [] "String" "String" 1
    , StreamVertex 1 Map [[| show >>> length |]] "Int" "Int" (1/2)
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
test_mapFuse = assertEqual (applyRule mapFuse mapFusePre) mapFusePost

-- streamMap >>> streamScan --------------------------------------------------

mapScan :: RewriteRule
mapScan (Connect (Vertex v1@(StreamVertex i Map (f:ss) t1 _ s1))
                 (Vertex v2@(StreamVertex _ Scan (g:a:_) _ t2 s2))) =
    let v = StreamVertex i Scan ([| flip (flip $(f) >>> $(g)) |]:a:ss) t1 t2 (sumRates s1 1 s2) 
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapScan _ = Nothing

mapScanPre = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Map [f] "Int" "Int" 1
    , StreamVertex 2 Scan [g,a] "Int" "Int" 1
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where
        f = [| (+1) |]
        g = [| \c _ -> c +1|]
        a = [| 0 |]

mapScanPost = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Scan [[| flip (flip $(f) >>> $(g))|], [| $(a) |]] "Int" "Int" (1/2)
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where
        f = [| (+1) |]
        g = [| \c _ -> c +1|]
        a = [| 0 |]

test_mapScan = assertEqual (applyRule mapScan mapScanPre) mapScanPost

-- streamExpand >>> streamFilter f == streamMap (filter f) >>> streamExpand --
-- TODO: assuming that the serviceRate for the new map matches the old filter
-- Note that the filter selectivity information is lost

expandFilter :: RewriteRule
expandFilter (Connect (Vertex e@(StreamVertex j Expand _ t1 t2 se))
                      (Vertex f@(StreamVertex i (Filter _) (p:_) _ _ sf))) =
    let m = StreamVertex j Map [[| filter $(p) |]] t1 t1 sf
        e'= StreamVertex i Expand [] t1 t2 se
    in  Just (replaceVertex f e' . replaceVertex e m)
expandFilter _ = Nothing

expandFilterPre = path
    [ StreamVertex 0 (Source 1)       [] "[Int]" "[Int]" 1
    , StreamVertex 1 Expand       [] "[Int]" "Int" 2
    , StreamVertex 2 (Filter 0.5) [[|$(p)|]] "Int" "Int" 3
    , StreamVertex 3 Sink         [] "Int" "Int" 4
    ]
    where
        p = [| (>3) |]

expandFilterPost = path
    [ StreamVertex 0 (Source 1) [] "[Int]" "[Int]" 1
    , StreamVertex 1 Map [[|filter $(p) |]] "[Int]" "[Int]" 3
    , StreamVertex 2 Expand [] "[Int]" "Int" 2
    , StreamVertex 3 Sink [] "Int" "Int" 4
    ]
    where
        p = [| (>3) |]

test_expandFilter = assertEqual (applyRule expandFilter expandFilterPre) expandFilterPost

-- streamMap f >>> streamFilterAcc g a p == streamFilterAcc g a (f >>> p) >>> streamMap f

mapFilterAcc :: RewriteRule
mapFilterAcc (Connect (Vertex m@(StreamVertex i Map (f:_) t1 _ sm))
                      (Vertex f1@(StreamVertex j (FilterAcc sel) (g:a:p:_) _ _ sf))) =

    let f2 = StreamVertex i (FilterAcc sel) [g, a, [| ($f) >>> $(p) |]] t1 t1 (sumRates sm 1 sf)
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilterAcc _ = Nothing

mapFilterAccPre = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Map [f] "Int" "String" 1
    , StreamVertex 2 (FilterAcc 0.5) [g,a,p] "String" "String" 1
    , StreamVertex 3 Sink [] "String" "String" 1
    ]
    where
        f = [| (+1) |]
        g = [| (\_ h -> (False, h)) |]
        a = [| (True, undefined) |]
        p = [| \new (b,old) -> b || old /= new |]

mapFilterAccPost = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.5) [g,a, [| $(f) >>> $(p) |]] "Int" "Int" (1/2)
    , StreamVertex 2 Map [f] "Int" "String" 1
    , StreamVertex 3 Sink [] "String" "String" 1
    ]
    where
        f = [| (+1) |]
        g = [| (\_ h -> (False, h)) |]
        a = [| (True, undefined) |]
        p = [| \new (b,old) -> b || old /= new |]

test_mapFilterAcc = assertEqual (applyRule mapFilterAcc mapFilterAccPre) mapFilterAccPost

-- streamMap f >>> streamWindow wm == streamWindow wm >>> streamMap (map f) --
-- TODO: assuming serviceRate for map is the same
-- This is only applicable when the map parameter has type (a -> a)

isTypeVariable :: String -> Bool
isTypeVariable = isLower . head

-- could two types be plugged together?
-- we do not handle any typeclass constraints
compatibleTypes :: String -> String -> Bool
compatibleTypes outT inT | outT == inT        = True -- matching concrete types
                         | isTypeVariable inT = True
                         | isTypeVariable outT= True
                         | otherwise          = False

test_compatibleTypes = assertBool $ compatibleTypes "Int" "a"

mapWindow :: RewriteRule
mapWindow (Connect (Vertex m@(StreamVertex i Map    (f:_) mapInT mapOutT sm))
                   (Vertex w@(StreamVertex j Window (wm:_) windowInT windowOutT sw))) =

    if   not (compatibleTypes mapInT windowInT)
    then Nothing
    else let
        w2 = StreamVertex i Window [wm] windowInT windowOutT sw
        m2 = StreamVertex j Map [[| map $(f) |]] ("["++mapInT++"]") ("["++mapOutT++"]") sm
    in  Just (replaceVertex m w2 . replaceVertex w m2)

mapWindow _ = Nothing

-- concrete map, concrete window, map types match
mapWindowPre = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Map    [[| (+1) |]] "Int" "Int" 2
    , StreamVertex 2 Window [[| chop 2 |]] "Int" "[Int]" 3
    , StreamVertex 3 Sink   [] "[Int]" "[Int]" 4
    ]

mapWindowPost = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Window [[| chop 2 |]] "Int" "[Int]" 3
    , StreamVertex 2 Map    [[| map (+1) |]] "[Int]" "[Int]" 2
    , StreamVertex 3 Sink   [] "[Int]" "[Int]" 4
    ]

test_mapWindow = assertEqual (applyRule mapWindow mapWindowPre) mapWindowPost

-- concrete map, concrete window, map types don't match
mapWindowPre2 = path
    [ StreamVertex 0 Map    [[| read   :: String -> Int  |]] "String" "Int"   1
    , StreamVertex 1 Window [[| chop 2 :: WindowMaker Int|]] "Int"    "[Int]" 1
    ]

test_mapWindow2 = assertNothingNoShow (mapWindow mapWindowPre2)

-- concrete map, polymorphic window
mapWindowPre3 = path
    [ StreamVertex 0 (Source 1) [] "(Int,Int)" "(Int,Int)" 1
    , StreamVertex 1 Map [[|fst|]] "(Int,Int)" "Int" 0.0
    , StreamVertex 2 Window [[|chop 2|]] "a" "[a]" 0.0
    , StreamVertex 3 Sink   [] "[Int]" "[Int]" 4
    ]
mapWindowPost3 = path
    [ StreamVertex 0 (Source 1) [] "(Int,Int)" "(Int,Int)" 1
    , StreamVertex 1 Window [[|chop 2|]] "a" "[a]" 0.0
    , StreamVertex 2 Map [[|map fst|]] "[(Int,Int)]" "[Int]" 0.0
    , StreamVertex 3 Sink   [] "[Int]" "[Int]" 4
    ]
test_mapWindow3 = assertEqual mapWindowPost3 (applyRule mapWindow mapWindowPre3)

-- polymorphic map, concrete window, map types match
mapWindowPre4 = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Map [[|(+1)|]] "a" "a" 0.0
    , StreamVertex 2 Window [[|chop 2|]] "Int" "[Int]" 0.0
    , StreamVertex 3 Sink   [] "[Int]" "[Int]" 4
    ]
mapWindowPost4 = path
    [ StreamVertex 0 (Source 1) [] "Int" "Int" 1
    , StreamVertex 1 Window [[|chop 2|]] "Int" "[Int]" 0.0
    , StreamVertex 2 Map [[|map (+1)|]] "[a]" "[a]" 0.0
    , StreamVertex 3 Sink   [] "[Int]" "[Int]" 4
    ]
test_mapWindow4 = assertEqual mapWindowPost4 (applyRule mapWindow mapWindowPre4)

-- polymorphic map, concrete window, map types don't match
mapWindowPre5 = path
    [ StreamVertex 0 (Source 1) [] "String" "String" 1
    , StreamVertex 1 Map [[|show|]] "a" "b" 0.0
    , StreamVertex 2 Window [[|chop 2|]] "String" "[String]" 0.0
    , StreamVertex 3 Sink   [] "[String]" "[String]" 4
    ]
test_mapWindow5 = assertNothingNoShow (mapWindow mapWindowPre5)

-- polymorphic map, polymorphic window
mapWindowPre6 = path
    [ StreamVertex 0 (Source 1) []       "String"   "String" 1
    , StreamVertex 1 Map [[|show|]]      "a"        "b" 0.0
    , StreamVertex 2 Window [[|chop 2|]] "c"        "[c]" 0.0
    , StreamVertex 3 Sink   []           "[String]" "[String]" 4
    ]
mapWindowPost6 = path
    [ StreamVertex 0 (Source 1) []       "String"   "String" 1
    , StreamVertex 1 Window [[|chop 2|]] "c"        "[c]" 0.0
    , StreamVertex 2 Map [[|map show|]]  "[a]"      "[b]" 0.0
    , StreamVertex 3 Sink   []           "[String]" "[String]" 4
    ]
test_mapWindow6 = assertEqual mapWindowPost6 (applyRule mapWindow mapWindowPre6)

-- streamExpand >>> streamMap f == streamMap (map f) >>> streamExpand --------
-- [a]           a            b   [a]               [b]               b
-- TODO: assuming serviceRate for map unaffected

expandMap :: RewriteRule
expandMap (Connect (Vertex e@(StreamVertex i Expand _ t1 _ se))
                   (Vertex m@(StreamVertex j Map (f:_) _ t4 sm))) =
    let t5 = "[" ++ t4 ++ "]"
        m2 = StreamVertex i Map [[| map $(f) |]] t1 t5 sm
        e2 = StreamVertex j Expand [] t5 t4 se
    in  Just (replaceVertex m e2 . replaceVertex e m2)

expandMap _ = Nothing

expandMapPre = path
    [ StreamVertex 0 (Source 1) [] "[Int]" "[Int]" 1
    , StreamVertex 1 Expand [] "[Int]" "Int" 2
    , StreamVertex 2 Map [[| show |]] "Int" "String" 3
    , StreamVertex 3 Sink [] "String" "String" 4
    ]

expandMapPost = path
    [ StreamVertex 0 (Source 1) [] "[Int]" "[Int]" 1
    , StreamVertex 1 Map [[| map (show) |]] "[Int]" "[String]" 3
    , StreamVertex 2 Expand [] "[String]" "String" 2
    , StreamVertex 3 Sink [] "String" "String" 4
    ]

test_expandMap = assertEqual (applyRule expandMap expandMapPre) expandMapPost

-- streamExpand >>> streamScan f a == ----------------------------------------
--     streamFilter (not.null)
--         >>> streamScan (\b a' -> tail $ scanl f (last b) a') [a]
--         >>> streamExpand

-- TODO: assuming zero service time for the new filter, and the same for the
-- two scans. And 50% selectivity for the new filter! Which is a stab in the
-- dark.
expandScan :: RewriteRule
expandScan (Connect (Vertex  e@(StreamVertex i Expand (_)     t1 t2 se))
                    (Vertex sc@(StreamVertex j Scan   (f:a:_) _  t3 ss))) =
    Just $ \g ->
        let t4 = "[" ++ t3 ++ "]"
            k  = newVertexId g
            p  = [| \b a' -> tail $ scanl $(f) (last b) a' |]

            f' = StreamVertex i (Filter 0.5) [[| not.null |]]  t1 t1 0
            sc'= StreamVertex j Scan         [p, [| [$(a)] |]] t1 t4 ss
            e' = StreamVertex k Expand       []                t4 t3 se

        in  overlay (path [f',sc',e']) $
            (removeEdge f' e' . replaceVertex e f' . replaceVertex sc e') g

expandScan _ = Nothing

expandScanPre = path
    [ StreamVertex 0 (Source 1) []    "[Int]" "[Int]" 1
    , StreamVertex 1 Expand []    "[Int]" "Int" 2
    , StreamVertex 2 Scan   [f,a] "Int"   "Int" 3
    , StreamVertex 3 Sink   []    "Int"   "Int" 4
    ]
    where
        f = [| \c _ -> c + 1 |]
        a = [| 0 |]

expandScanPost = path
    [ StreamVertex 0 (Source 1)       []     "[Int]" "[Int]" 1
    , StreamVertex 1 (Filter 0.5) [p]    "[Int]" "[Int]" 0
    , StreamVertex 2 Scan         [g,as] "[Int]" "[Int]" 3
    , StreamVertex 4 Expand       []     "[Int]" "Int" 2
    , StreamVertex 3 Sink         []     "Int"   "Int" 4
    ]
    where
        p  = [| not . null |]
        g  = [| \b a' -> tail $ scanl $(f) (last b) a' |]
        f  = [| \c _ -> c + 1 |]
        as = [| [$(a)] |]
        a  = [| 0 |]

test_expandScan = assertEqual (simplify $ applyRule expandScan expandScanPre) expandScanPost

-- streamExpand >>> streamExpand == streamMap concat >>> streamExpand --------
-- [[a]]        [a]             a  [[a]]            [a]              a

expandExpand :: RewriteRule
expandExpand (Connect (Vertex e@(StreamVertex i Expand _ t1 t2 s))
                      (Vertex   (StreamVertex j Expand _ _ _ _))) =
    let m = StreamVertex i Map [[| concat |]] t1 t2 s
    in  Just (replaceVertex e m)

expandExpand _ = Nothing

expandExpandPre = path
    [ StreamVertex 0 (Source 1) [] "[Int]" "[Int]" 1
    , StreamVertex 1 Expand [] "[[Int]]" "[Int]" 2
    , StreamVertex 2 Expand [] "[Int]" "Int" 3
    , StreamVertex 3 Sink   [] "Int" "[Int]" 4
    ]

expandExpandPost = path
    [ StreamVertex 0 (Source 1) [] "[Int]" "[Int]" 1
    , StreamVertex 1 Map [[| concat |]] "[[Int]]" "[Int]" 2
    , StreamVertex 2 Expand [] "[Int]" "Int" 3
    , StreamVertex 3 Sink   [] "Int" "[Int]" 4
    ]

test_expandExpand = assertEqual (applyRule expandExpand expandExpandPre)
    expandExpandPost

-- streamFilter f (streamMerge [s1, s2]) -------------------------------------
-- = streamMerge [streamFilter f s1, streamFilter f s2]

mergeFilter :: RewriteRule
mergeFilter = hoistOp (Filter 0)

-- TODO address Filter selectivities
-- | "hoist" an Operator (such as a Filter) upstream through a Merge operator.
hoistOp op (Connect (Vertex m@(StreamVertex i Merge _ _ ty _))
                    (Vertex f@(StreamVertex j o pred _ ty' s))) =

    if not(cmpOps o op) then Nothing
    else Just $ \g -> let

        mkOp g = StreamVertex (newVertexId g) o pred ty ty' s

        -- for each NODE that connects to Merge: (:: [StreamVertex])
        inbound    = map fst . filter ((m==) . snd) . edgeList $ g

        -- * remove that edge (:: [StreamGraph -> StreamGraph])
        snipMerge  = map (\v -> removeEdge v m) inbound

        -- * insert new Operators (:: [StreamGraph -> StreamGraph])
        newOps = map (\v -> \g -> overlay g $ path [v, mkOp g, m]) inbound

        m' = m { intype = ty', outtype = ty' }

  in (removeEdge m f
      >>> replaceVertex f m
      >>> foldRules snipMerge
      >>> foldRules newOps
      >>> replaceVertex m m') g

hoistOp _ _ = Nothing

v1 = StreamVertex 0 (Source 1)       []           "Int" "Int" 1
v2 = StreamVertex 1 (Source 1)       []           "Int" "Int" 2
v3 = StreamVertex 2 Merge        []           "Int" "Int" 3
v4 = StreamVertex 3 (Filter 0.5) [[| (>3) |]] "Int" "Int" 4
v5 = StreamVertex 4 Sink         []           "Int" "Int" 5

mergeFilterPre = overlay (path [v1,v3,v4,v5]) (path [v2,v3])

v6 = StreamVertex 5 (Filter 0.5) [[| (>3) |]] "Int" "Int" 4
v7 = StreamVertex 6 (Filter 0.5) [[| (>3) |]] "Int" "Int" 4

mergeFilterPost = overlay (path [v1,v6,v3,v5]) (path [v2,v7,v3])

test_mergeFilter = assertEqual (applyRule mergeFilter mergeFilterPre)
    mergeFilterPost

-- streamExpand (streamMerge [s1, s2]) -------------------------------------
-- = streamMerge [streamExpand s1, streamExpand s2]

mergeExpand :: RewriteRule
mergeExpand = hoistOp Expand

v8  = StreamVertex 0 (Source 1) [] "[Int]" "[Int]" 1
v9  = StreamVertex 1 (Source 1) [] "[Int]" "[Int]" 2
v10 = StreamVertex 2 Merge  [] "[Int]" "[Int]" 3
v11 = StreamVertex 3 Expand [] "[Int]" "Int"   4

mergeExpandPre  = overlay (path [v8, v10, v11, v5]) (path [v9, v10])

v12 = StreamVertex 2 Merge  [] "Int" "Int" 3
v13 = StreamVertex 5 Expand [] "[Int]" "Int" 4
v14 = StreamVertex 6 Expand [] "[Int]" "Int" 4

mergeExpandPost = overlay (path [v8, v13, v12, v5]) (path [v9, v14, v12])

test_mergeExpand = assertEqual (applyRule mergeExpand mergeExpandPre)
    mergeExpandPost

-- streamMap (streamMerge [s1, s2]) ------------------------------------------
-- = streamMerge [streamMap s1, streamMap s2]

mergeMap :: RewriteRule
mergeMap = hoistOp Map

v15 = StreamVertex 0 (Source 1) [] "Int" "Int" 1
v16 = StreamVertex 1 (Source 1) [] "Int" "Int" 2
v17 = StreamVertex 2 Merge []  "Int" "Int" 3
v18 = StreamVertex 3 Map [[| show |]]  "Int" "String" 4
v19 = StreamVertex 4 Sink [] "String" "String" 5

mergeMapPre = overlay (path [v15,v17,v18,v19]) (path [v16,v17])

v20 = StreamVertex 5 Map [[| show |]]  "Int" "String" 4
v21 = StreamVertex 6 Map [[| show |]]  "Int" "String" 4
v22 = StreamVertex 2 Merge [] "String" "String" 3

mergeMapPost = overlay (path [v15,v20,v22,v19]) (path [v16,v21,v22])

test_mergeMap = assertEqual (applyRule mergeMap mergeMapPre) mergeMapPost

-- streamMerge [streamMap f s1, streamMap f s2]
--     == streamMap f (streamMerge [s1,s2])

identicalParams :: [StreamVertex] -> Bool
identicalParams inbound =
    let params = (map.map) deQ (map parameters inbound)
    in  and (map (==(head params)) (tail params))

-- TODO: for filters, this will only match when all the upstream filters
-- have the same selectivity. An alternative would be to average them.
pushOp op (Connect (Vertex ma@(StreamVertex i o fs t1 t2 _))
                   (Vertex me@(StreamVertex j Merge _ t3 _ _))) =

    if not(cmpOps o op) then Nothing
    else Just $ \g -> let

        inbound = map fst . filter ((me==) . snd) . edgeList $ g
        -- the pattern match is not enough to be conclusive that this applies
        in if identicalOperators inbound && identicalParams inbound
           then let
               me' = me { intype = t1, outtype = t1 }
               ma' = ma { vertexId =  newVertexId g }
               on  = snd . head . filter ((==me) . fst) . edgeList $ g
               in ( removeEdge me on
                   -- remove all the inbound operators
                   >>> mergeVertices (`elem` inbound) me
                   >>> replaceVertex me me' -- fix Merge type
                   >>> removeEdge me' me'
                   -- new Operator after Merge
                   >>> overlay (path [me', ma', on])
               ) g
           else g

pushOp _ _ = Nothing

mapMerge :: RewriteRule
mapMerge = pushOp Map

mapMergePre  = mergeMapPost
mapMergePost = overlay (path [v15,v17,v18 {vertexId = 7}, v19]) (path [v16,v17])

test_mapMerge = assertEqual (applyRule mapMerge mapMergePre) mapMergePost

-- streamMerge [streamFilter p s1, streamFilter p s2] ------------------------
-- == streamFilter p (streamMerge [s1,s2])

-- TODO: here we assume that the filters have the same selectivity. Alternative
-- we could/should average them.
filterMerge :: RewriteRule
filterMerge = pushOp (Filter 0)

filterMergePre  = mergeFilterPost
filterMergePost = overlay (path [v1,v3,v4 {vertexId=7},v5]) (path [v2,v3])
test_filterMerge = assertEqual (applyRule filterMerge filterMergePre)
    filterMergePost

-- streamMerge [streamExpand s1, streamExpand s2] ----------------------------
-- == streamExpand (streamMerge [s1,s2])

expandMerge :: RewriteRule
expandMerge = pushOp Expand

expandMergePre  = mergeExpandPost
expandMergePost = overlay (path [v8, v10, v11 {vertexId=7}, v5]) (path [v9, v10])
test_expandMerge = assertEqual (applyRule expandMerge expandMergePre)
    expandMergePost

-- streamMerge merge ---------------------------------------------------------

-- | fuse two Merge operators. The order-preserving transformation is strictly
-- right-oriented i.e. merge [s1, merge [s2,s3]] == merge [s1,s2,s3] but for
-- non-order-preserving we can write a much more generic rule.
mergeFuse :: RewriteRule
mergeFuse (Connect (Vertex m1@(StreamVertex i Merge _ _ _ _))
                   (Vertex m2@(StreamVertex j Merge _ _ _ _))) =
    Just (removeEdge m1 m1 . mergeVertices (`elem` [m1,m2]) m1)

mergeFuse _ = Nothing

v23 = StreamVertex 0 (Source 1) [] "Int" "Int" 1
v24 = StreamVertex 1 (Source 1) [] "Int" "Int" 2
v25 = StreamVertex 2 (Source 1) [] "Int" "Int" 3
v26 = StreamVertex 3 Merge []  "Int" "Int" 4
v27 = StreamVertex 4 Merge []  "Int" "Int" 5
v28 = StreamVertex 5 Sink []   "Int" "Int" 6

mergeFusePre = path [v23,v26,v27,v28]
    `Overlay`  path [v24,v26,v27]
    `Overlay`  path [v25,v27]

mergeFusePost = path [v23,v26,v28]
    `Overlay`   path [v24,v26]
    `Overlay`   path [v25,v26]

test_mergeFuse = assertEqual (applyRule mergeFuse mergeFusePre) mergeFusePost

-- streamExpand >>> streamFilterAcc ... = streamScan ... >>> streamExpand-----
-- [a]           a                   a  [a]               [a]              a

expandFilterAcc :: RewriteRule
expandFilterAcc (Connect (Vertex e@(StreamVertex i Expand _ t1 t2 _))
                         (Vertex fa@(StreamVertex j (FilterAcc _) (f:a:p:_) _ _ s))) =
    Just $ \g -> let
        scan = StreamVertex i Scan
            [ [| \(_,acc) a -> filterAcc $(f) acc $(p) a |]
            , [| ([],$(a)) |]
            ] t1 t1 s
        mapr = StreamVertex j Map [[| reverse.fst |]] t1 t1 0
        k    = newVertexId g
        expd = e { vertexId = k }

        in g & removeEdge e fa
             & replaceVertex e scan
             & replaceVertex fa expd
             & overlay (path [scan,mapr,expd])

expandFilterAcc  _ = Nothing

expandFilterAccPre = path
    [ StreamVertex 0 (Source 1)      []        "[Int]" "[Int]" 1
    , StreamVertex 1 Expand          []        "[Int]" "Int"   1
    , StreamVertex 2 (FilterAcc 0.5) [f, a, p] "Int"   "Int"   2
    ] where f = [| (\_ h -> (False, h)) |]
            a = [| (True, undefined) |]
            p = [| \new (b,old) -> b || old /= new |]

expandFilterAccPost = path
    [ StreamVertex 0 (Source 1)      []        "[Int]" "[Int]" 1
    , StreamVertex 1 Scan            [sa, si]  "[Int]" "[Int]" 2
    , StreamVertex 2 Map             [f']      "[Int]" "[Int]" 0
    , StreamVertex 3 Expand          []        "[Int]" "Int"   1
    ] where f  = [| (\_ h -> (False, h)) |]
            a  = [| (True, undefined) |]
            p  = [| \new (b,old) -> b || old /= new |]
            f' = [| reverse.fst |]
            sa = [| \(_,acc) a -> filterAcc $(f) acc $(p) a |]
            si = [| ([],$(a)) |]

test_expandFilterAcc = assertEqual expandFilterAccPost
    $ applyRule expandFilterAcc expandFilterAccPre

-- utility/boilerplate -------------------------------------------------------

sumRates :: Double -> Double -> Double -> Double
sumRates a f b = 1 / ((1 / a) + (f / b))

test_sumRates1 = assertEqual (1/2) $ sumRates 1 1 1
test_sumRates2 = assertEqual 4     $ sumRates 5 (1/2) 10

-- compare operators, ignoring filter selectivity
cmpOps :: StreamOperator -> StreamOperator -> Bool
cmpOps (Filter _) (Filter _) = True
cmpOps (FilterAcc _) (FilterAcc _) = True
cmpOps x y = x == y

-- this will only match filter operators with the same selectivity
identicalOperators :: [StreamVertex] -> Bool
identicalOperators = (==1) . length . nub . map operator

-- | left-biased rule application via fold
foldRules :: [Graph a -> Graph a] -> Graph a -> Graph a
foldRules rules g = foldl (&) g rules

-- generate a new vertexId which doesn't clash with any existing ones
-- TODO this will still break the assumption that CompileIoT makes regarding
-- vertexId ordering in a path. We either need to renumber all subsequent nodes
-- or remove that requirement in CompileIoT
newVertexId :: StreamGraph -> Int
newVertexId = succ . last . sort . map vertexId . vertexList

main = htfMain htf_thisModulesTests

-- streamFilter p >>> streamWindow w = streamWindow w >>> streamMap (filter p)
-- this is not a generally-applicable rule:
--  * if the WindowMaker makes decisions based on the Event values or their
--    sequencing, then this is altered by removing the pre-filter 
--  * even if it doesn't, the windows might change
-- the filter's selectivity is lost

filterWindow :: RewriteRule
filterWindow (Connect (Vertex f@(StreamVertex i (Filter _) (p:_) _ _ s))
                      (Vertex w@(StreamVertex j Window     _     _ t _))) =
    let m = StreamVertex j Map [[| filter $(p) |]] t t s
        w'= w { vertexId = i }
    in  Just (replaceVertex f w' . replaceVertex w m)

filterWindow _ = Nothing

filterWindowPre = path
    [ StreamVertex 0 (Source 1)   []         "Int"   "Int"   1
    , StreamVertex 1 (Filter 0.5) [[|(>3)|]] "Int"   "Int"   2
    , StreamVertex 2 Window       []         "Int"   "[Int]" 3
    , StreamVertex 3 Sink         []         "[Int]" "[Int]" 4
    ]

filterWindowPost = path
    [ StreamVertex 0 (Source 1)   []                "Int"   "Int"   1
    , StreamVertex 1 Window       []                "Int"   "[Int]" 3
    , StreamVertex 2 Map          [[|filter (>3)|]] "[Int]" "[Int]" 2
    , StreamVertex 3 Sink         []                "[Int]" "[Int]" 4
    ]

test_filterWindow = assertEqual (applyRule filterWindow filterWindowPre) filterWindowPost

-- streamFilterAcc f a p >>> streamWindow w =
-- streamWindow w >>> streamScan (\ (_,acc) a -> filterAcc f acc p a) ([],a)
--                >>> streamMap (reverse.fst) 

-- Same caveats as filterWindow

filterAccWindow :: RewriteRule
filterAccWindow (Connect (Vertex fa@(StreamVertex i (FilterAcc _) (f:a:p:_) _ _ s))
                         (Vertex  w@(StreamVertex j Window _ _ t _))) =
    Just $ \g -> let
      w' = w { vertexId = i }
      sc = StreamVertex j Scan
         [ [| \ (_, acc) a -> filterAcc $(f) acc $(p) a |]
         , [| ([], $(a)) |]
         ] t t s
      m  = StreamVertex (newVertexId g) Map [[| reverse.fst |]] t t 0
      in g & replaceVertex fa w'
           & replaceVertex w  m
           & removeEdge w' m
           & overlay (path [w', sc, m])

filterAccWindow _ = Nothing

filterAccWindowPre = path
    [ StreamVertex 0 (Source 1)      []        "Int" "Int"   1
    , StreamVertex 1 (FilterAcc 0.5) [f, a, p] "Int" "Int"   2
    , StreamVertex 2 Window          []        "Int" "[Int]" 1
    ] where f = [| (\_ h -> (False, h)) |]
            a = [| (True, undefined) |]
            p = [| \new (b,old) -> b || old /= new |]

filterAccWindowPost = path
    [ StreamVertex 0 (Source 1) []       "Int"   "Int"   1
    , StreamVertex 1 Window     []       "Int"   "[Int]" 1
    , StreamVertex 2 Scan       [sa, si] "[Int]" "[Int]" 2
    , StreamVertex 3 Map        [f']     "[Int]" "[Int]" 0
    ] where f = [| (\_ h -> (False, h)) |]
            a = [| (True, undefined) |]
            p = [| \new (b,old) -> b || old /= new |]
            f' = [| reverse.fst |]
            sa = [| \(_,acc) a -> filterAcc $(f) acc $(p) a |]
            si = [| ([],$(a)) |]

test_filterAccWindow = assertEqual filterAccWindowPost
    $ applyRule filterAccWindow filterAccWindowPre

{- $fromRewriteRule
== RewriteRule
`RewriteRule` and `LabelledRewriteRule` are defined in a sub-module
`Striot.LogicalOptimiser.RewriteRule` and re-exported here, due to
technical restrictions with Template Haskell.
 -}
