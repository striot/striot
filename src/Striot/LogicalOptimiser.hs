{-# OPTIONS_GHC -F -pgmF htfpp #-}
{-# LANGUAGE TemplateHaskell #-}

module Striot.LogicalOptimiser ( applyRules
                               , costModel
                               , optimise

                               , htf_thisModulesTests
                               ) where

import Striot.StreamGraph
import Algebra.Graph
import Test.Framework hiding ((===))
import Data.Maybe (mapMaybe, fromMaybe)
import Data.Function ((&))
import Data.List (nub, sort, intercalate)
import Control.Arrow ((>>>))

-- applying encoded rules and their resulting ReWriteOps ----------------------

type RewriteRule = StreamGraph -> Maybe (StreamGraph -> StreamGraph)

applyRule :: RewriteRule -> StreamGraph -> StreamGraph
applyRule f g = g & fromMaybe id (firstMatch g f)

-- recursively attempt to apply the rule to the graph, but stop
-- as soon as we get a match
firstMatch :: StreamGraph -> RewriteRule -> Maybe (StreamGraph -> StreamGraph)
firstMatch g f = case f g of
        Just f -> Just f
        _      -> case g of
            Empty       -> Nothing
            Vertex v    -> Nothing
            Overlay a b -> case firstMatch a f of
                                Just f  -> Just f
                                Nothing -> firstMatch b f
            Connect a b -> case firstMatch a f of
                                Just f  -> Just f
                                Nothing -> firstMatch b f

-- thoughts about cost model
-- higher is better
costModel :: StreamGraph -> Int
costModel = negate . length . vertexList

-- N-bounded recursive rule traversal
-- (caller may wish to apply 'nub')
applyRules :: Int -> StreamGraph -> [StreamGraph]
applyRules n sg =
        if   n < 1 then [sg]
        else let
             sgs = map ((&) sg) $ mapMaybe (firstMatch sg) rules
             in    sg : sgs ++ (concatMap (applyRules (n-1)) sgs)

-- | Return an optimised version of the supplied StreamGraph, or the
-- graph itself if no better alternative is found.
optimise :: StreamGraph -> StreamGraph
optimise sg = let
    base              = costModel sg
    sgs               = nub $ applyRules 5 sg
    (score,candidate) = maximum $ map (\g -> (costModel g, g) ) sgs
    in if score > base
       then candidate
       else sg

------------------------------------------------------------------------------

rules :: [RewriteRule]
rules = [ filterFuse
        , mapFilter
        , filterFilterAcc
        , filterAccFilter
        , filterAccFilterAcc
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
        ]

-- streamFilter f >>> streamFilter g = streamFilter (\x -> f x && g x) -------

filterFuse :: RewriteRule
filterFuse (Connect (Vertex a@(StreamVertex i (Filter sel1) (p:_) ty _ s1))
                    (Vertex b@(StreamVertex _ (Filter sel2) (q:_) _ _ s2))) =
    let c = a { operator    = Filter (sel1 * sel2)
              , parameters  = [[| (\p q x -> p x && q x) $(p) $(q) |]]
              , serviceTime = s1 + s2
              }
    in Just (removeEdge c c . mergeVertices (`elem` [a,b]) c)

filterFuse _ = Nothing

gt3 = [| (>3) |]
lt5 = [| (<5) |]

so' = StreamVertex 0 Source       []    "String" "String" 1
f3  = StreamVertex 1 (Filter 0.5) [gt3] "String" "String" 1
f4  = StreamVertex 2 (Filter 0.5) [lt5] "String" "String" 1
si' = StreamVertex 3 Sink         []    "String" "String" 1

fused = [| (\p q x -> p x && q x) (>3) (<5) |]

filterFusePre = path [so', f3, f4, si']

filterFusePost = path [ so'
    , StreamVertex 1 (Filter 0.25) [fused] "String" "String" 2
    , si' ]

test_filterFuse = assertEqual (applyRule filterFuse filterFusePre)
    filterFusePost

-- streamMap f >>> streamFilter p = streamFilter (f >>> p) >>> streamMap f ---

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

so = StreamVertex 0 Source [] "Int" "Int" 1
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
          , [| \v a -> $(p) v && $(q) v a |] ] ty ty (s1+s2)
    in  Just (removeEdge v3 v3 . mergeVertices (`elem` [v1,v2]) v3)
filterFilterAcc _ = Nothing

filterFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 (Filter 0.5) [p] "Int" "Int" 1
    , StreamVertex 2 (FilterAcc 0.5) [f , a , q] "Int" "Int" 1
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where p = [| (>3) |]
          f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          q = [| \new (b,old) -> b || old /= new |]

filterFilterAccPost = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.25)
        [ [| \a v -> if $(p) v then $(f) a v else a |]
        , a
        , [| \v a -> $(p) v && $(q) v a |]
        ] "Int" "Int" 2
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
        v  = StreamVertex i (FilterAcc (sel1*sel2)) [f,a,p'] ty ty (s1+s2)
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilter _ = Nothing

filterAccFilterPre = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.5) [f,a,p] "Int" "Int" 1
    , StreamVertex 2 (Filter 0.5)    [q] "Int" "Int" 1
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          p = [| \new (b,old) -> b || old /= new |]
          q = [| (>3) |]

filterAccFilterPost = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.25) [f, a, p'] "Int" "Int" 2
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
        v  = StreamVertex i (FilterAcc (sel1*sel2)) (f':a':q':ss) ty ty (s1+s2)
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilterAcc _ = Nothing

filterAccFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.5) [f,a,p] "Int" "Int" 1
    , StreamVertex 2 (FilterAcc 0.5) [g,b,q] "Int" "Int" 1
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
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.25)
        [ [| \(a,b) v -> ($(f) a v, if $(p) v a then $(g) b v else b) |]
        , [| ($(a),$(b)) |]
        , [| \v (y,z) -> $(p) v y && $(q) v z |]
        ] "Int" "Int" 2
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
    let v = StreamVertex i Map ([| $(f) >>> $(g) |]:ss) t1 t2 (s1+s2)
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapFuse _ = Nothing

mapFusePre = path
    [ StreamVertex 0 Source [] "String" "String" 1
    , StreamVertex 1 Map [[| show |]] "Int" "String" 1
    , StreamVertex 2 Map [[| length |]] "String" "Int" 1
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]

mapFusePost = path
    [ StreamVertex 0 Source [] "String" "String" 1
    , StreamVertex 1 Map [[| show >>> length |]] "Int" "Int" 2
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
test_mapFuse = assertEqual (applyRule mapFuse mapFusePre) mapFusePost

-- streamMap >>> streamScan --------------------------------------------------

mapScan :: RewriteRule
mapScan (Connect (Vertex v1@(StreamVertex i Map (f:ss) t1 _ s1))
                 (Vertex v2@(StreamVertex _ Scan (g:a:_) _ t2 s2))) =
    let v = StreamVertex i Scan ([| flip (flip $(f) >>> $(g)) |]:a:ss) t1 t2 (s1+s2)
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapScan _ = Nothing

mapScanPre = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 Map [f] "Int" "Int" 1
    , StreamVertex 2 Scan [g,a] "Int" "Int" 1
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where
        f = [| (+1) |]
        g = [| \c _ -> c +1|]
        a = [| 0 |]

mapScanPost = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 Scan [[| flip (flip $(f) >>> $(g))|], [| $(a) |]] "Int" "Int" 2
    , StreamVertex 3 Sink [] "Int" "Int" 1
    ]
    where
        f = [| (+1) |]
        g = [| \c _ -> c +1|]
        a = [| 0 |]

test_mapScan = assertEqual (applyRule mapScan mapScanPre) mapScanPost

-- streamExpand >>> streamFilter f == streamMap (filter f) >>> streamExpand --
-- TODO: assuming that the serviceTime for the new map matches the old filter
-- Note that the filter selectivity information is lost

expandFilter :: RewriteRule
expandFilter (Connect (Vertex e@(StreamVertex j Expand _ t1 t2 se))
                      (Vertex f@(StreamVertex i (Filter _) (p:_) _ _ sf))) =
    let m = StreamVertex j Map [[| filter $(p) |]] t1 t1 sf
        e'= StreamVertex i Expand [] t1 t2 se
    in  Just (replaceVertex f e' . replaceVertex e m)
expandFilter _ = Nothing

expandFilterPre = path
    [ StreamVertex 0 Source       [] "[Int]" "[Int]" 1
    , StreamVertex 1 Expand       [] "[Int]" "Int" 2
    , StreamVertex 2 (Filter 0.5) [[|$(p)|]] "Int" "Int" 3
    , StreamVertex 3 Sink         [] "Int" "Int" 4
    ]
    where
        p = [| (>3) |]

expandFilterPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]" 1
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

    let f2 = StreamVertex i (FilterAcc sel) [g, a, [| ($f) >>> $(p) |]] t1 t1 (sm+sf)
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilterAcc _ = Nothing

mapFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
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
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 (FilterAcc 0.5) [g,a, [| $(f) >>> $(p) |]] "Int" "Int" 2
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
-- TODO: assuming serviceTime for map is the same

mapWindow :: RewriteRule
mapWindow (Connect (Vertex m@(StreamVertex i Map (f:_) t1 _ sm))
                   (Vertex w@(StreamVertex j Window (wm:_) _ t2 sw))) =
    let t3 = "[" ++ t1 ++ "]"
        w2 = StreamVertex i Window [wm] t1 t3 sw
        m2 = StreamVertex j Map [[| map $(f) |]] t3 t2 sm
    in  Just (replaceVertex m w2 . replaceVertex w m2)

mapWindow _ = Nothing

mapWindowPre = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 Map    [[| show |]] "Int" "String" 2
    , StreamVertex 2 Window [[| chop 2 |]] "String" "[String]" 3
    , StreamVertex 3 Sink   [] "[String]" "[String]" 4
    ]

mapWindowPost = path
    [ StreamVertex 0 Source [] "Int" "Int" 1
    , StreamVertex 1 Window [[| chop 2 |]] "Int" "[Int]" 3
    , StreamVertex 2 Map    [[| map show |]] "[Int]" "[String]" 2
    , StreamVertex 3 Sink   [] "[String]" "[String]" 4
    ]

test_mapWindow = assertEqual (applyRule mapWindow mapWindowPre) mapWindowPost

-- streamExpand >>> streamMap f == streamMap (map f) >>> streamExpand --------
-- [a]           a            b   [a]               [b]               b
-- TODO: assuming serviceTime for map unaffected

expandMap :: RewriteRule
expandMap (Connect (Vertex e@(StreamVertex i Expand _ t1 _ se))
                   (Vertex m@(StreamVertex j Map (f:_) _ t4 sm))) =
    let t5 = "[" ++ t4 ++ "]"
        m2 = StreamVertex i Map [[| map $(f) |]] t1 t5 sm
        e2 = StreamVertex j Expand [] t5 t4 se
    in  Just (replaceVertex m e2 . replaceVertex e m2)

expandMap _ = Nothing

expandMapPre = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]" 1
    , StreamVertex 1 Expand [] "[Int]" "Int" 2
    , StreamVertex 2 Map [[| show |]] "Int" "String" 3
    , StreamVertex 3 Sink [] "String" "String" 4
    ]

expandMapPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]" 1
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
    [ StreamVertex 0 Source []    "[Int]" "[Int]" 1
    , StreamVertex 1 Expand []    "[Int]" "Int" 2
    , StreamVertex 2 Scan   [f,a] "Int"   "Int" 3
    , StreamVertex 3 Sink   []    "Int"   "Int" 4
    ]
    where
        f = [| \c _ -> c + 1 |]
        a = [| 0 |]

expandScanPost = path
    [ StreamVertex 0 Source       []     "[Int]" "[Int]" 1
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
    [ StreamVertex 0 Source [] "[Int]" "[Int]" 1
    , StreamVertex 1 Expand [] "[[Int]]" "[Int]" 2
    , StreamVertex 2 Expand [] "[Int]" "Int" 3
    , StreamVertex 3 Sink   [] "Int" "[Int]" 4
    ]

expandExpandPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]" 1
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

v1 = StreamVertex 0 Source       []           "Int" "Int" 1
v2 = StreamVertex 1 Source       []           "Int" "Int" 2
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

v8  = StreamVertex 0 Source [] "[Int]" "[Int]" 1
v9  = StreamVertex 1 Source [] "[Int]" "[Int]" 2
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

v15 = StreamVertex 0 Source [] "Int" "Int" 1
v16 = StreamVertex 1 Source [] "Int" "Int" 2
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

-- compare operators, ignoring filter selectivity
cmpOps :: StreamOperator -> StreamOperator -> Bool
cmpOps (Filter _) (Filter _) = True
cmpOps (FilterAcc _) (FilterAcc _) = True
cmpOps x y = x == y

-- this will only match filter operators with the same selectivity
identicalOperators :: [StreamVertex] -> Bool
identicalOperators = (==1) . length . nub . map operator

-- TODO: for filters, this will only match when all the upstream filters
-- have the same selectivity. An alternative would be to average them.
pushOp op (Connect (Vertex ma@(StreamVertex i o fs t1 t2 sma))
                   (Vertex me@(StreamVertex j Merge _ t3 _ sme))) =

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

v23 = StreamVertex 0 Source [] "Int" "Int" 1
v24 = StreamVertex 1 Source [] "Int" "Int" 2
v25 = StreamVertex 2 Source [] "Int" "Int" 3
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

-- utility/boilerplate -------------------------------------------------------

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
