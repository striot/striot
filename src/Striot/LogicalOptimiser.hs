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
filterFuse (Connect (Vertex a@(StreamVertex i Filter (p:_) ty _))
                    (Vertex b@(StreamVertex _ Filter (q:_) _ _))) =
    let c = a { parameters = [[| (\p q x -> p x && q x) $(p) $(q) |]] }
    in Just (removeEdge c c . mergeVertices (`elem` [a,b]) c)

filterFuse _ = Nothing

gt3 = [| (>3) |]
lt5 = [| (<5) |]

so' = StreamVertex 0 Source []    "String" "String"
f3  = StreamVertex 1 Filter [gt3] "String" "String"
f4  = StreamVertex 2 Filter [lt5] "String" "String"
si' = StreamVertex 3 Sink   []    "String" "String"

fused = [| (\p q x -> p x && q x) (>3) (<5) |]

filterFusePre = path [so', f3, f4, si']

filterFusePost = path [ so'
    , StreamVertex 1 Filter [fused] "String" "String"
    , si' ]

test_filterFuse = assertEqual (applyRule filterFuse filterFusePre)
    filterFusePost

-- streamMap f >>> streamFilter p = streamFilter (f >>> p) >>> streamMap f ---

mapFilter :: RewriteRule
mapFilter (Connect (Vertex m@(StreamVertex i Map (f:_) intype _))
                   (Vertex f1@(StreamVertex j Filter (p:_) _ _))) =

    let f2 = StreamVertex i Filter [[| $(p) . $(f) |]] intype intype
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilter _ = Nothing

m1 = StreamVertex 1 Map [[| show |]] "Int" "String"
f1 = StreamVertex 2 Filter [[| \x -> length x <3 |]] "String" "String"

f2 = StreamVertex 1 Filter [[| (\x -> length x <3) . (show) |]] "Int" "Int"
m2 = StreamVertex 2 Map [[| show |]] "Int" "String"

so = StreamVertex 0 Source [] "Int" "Int"
si = StreamVertex 3 Sink [] "String" "String"

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
filterFilterAcc (Connect (Vertex v1@(StreamVertex i Filter (p:_) ty _))
                         (Vertex v2@(StreamVertex _ FilterAcc (f:a:q:_) _ _))) =
    let v3 = StreamVertex i FilterAcc
          [ [| \a v -> if $(p) v then $(f) a v else a |]
          , a
          , [| \v a -> $(p) v && $(q) v a |] ] ty ty
    in  Just (removeEdge v3 v3 . mergeVertices (`elem` [v1,v2]) v3)
filterFilterAcc _ = Nothing

filterFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Filter [p] "Int" "Int"
    , StreamVertex 2 FilterAcc [f , a , q] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
    where p = [| (>3) |]
          f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          q = [| \new (b,old) -> b || old /= new |]

filterFilterAccPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [ [| \a v -> if $(p) v then $(f) a v else a |]
                               , a
                               , [| \v a -> $(p) v && $(q) v a |]
                               ] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
    where p = [| (>3) |]
          f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          q = [| \new (b,old) -> b || old /= new |]

test_filterFilterAcc = assertEqual (applyRule filterFilterAcc filterFilterAccPre)
    filterFilterAccPost

-- streamFilterAcc >>> streamFilter ------------------------------------------

filterAccFilter :: RewriteRule
filterAccFilter (Connect (Vertex v1@(StreamVertex i FilterAcc (f:a:p:_) ty _))
                         (Vertex v2@(StreamVertex _ Filter (q:_) _ _))) =
    let p' = [| \v a -> $(p) v a && $(q) v |]
        v  = StreamVertex i FilterAcc [f,a,p'] ty ty
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilter _ = Nothing

filterAccFilterPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [f,a,p] "Int" "Int"
    , StreamVertex 2 Filter [q] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
    where f = [| (\_ h -> (False, h)) |]
          a = [| (True, undefined) |]
          p = [| \new (b,old) -> b || old /= new |]
          q = [| (>3) |]

filterAccFilterPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [f, a, p'] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
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
filterAccFilterAcc (Connect (Vertex v1@(StreamVertex i FilterAcc (f:a:p:ss) ty _))
                            (Vertex v2@(StreamVertex _ FilterAcc (g:b:q:_) _ _))) =
    let f' = [| \ (a,b) v -> ($(f) a v, if $(p) v a then $(g) b v else b) |]
        a' = [| ($(a), $(b)) |]
        q' = [| \v (y,z) -> $(p) v y && $(q) v z |]
        v  = StreamVertex i FilterAcc (f':a':q':ss) ty ty
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilterAcc _ = Nothing

filterAccFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [f,a,p] "Int" "Int"
    , StreamVertex 2 FilterAcc [g,b,q] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
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
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [ [| \(a,b) v -> ($(f) a v, if $(p) v a then $(g) b v else b) |]
                               , [| ($(a),$(b)) |]
                               , [| \v (y,z) -> $(p) v y && $(q) v z |]
                               ] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
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
mapFuse (Connect (Vertex v1@(StreamVertex i Map (f:ss) t1 _))
                 (Vertex v2@(StreamVertex _ Map (g:_) _ t2))) =
    let v = StreamVertex i Map ([| $(f) >>> $(g) |]:ss) t1 t2
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapFuse _ = Nothing

mapFusePre = path
    [ StreamVertex 0 Source [] "String" "String"
    , StreamVertex 1 Map [[| show |]] "Int" "String"
    , StreamVertex 2 Map [[| length |]] "String" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

mapFusePost = path
    [ StreamVertex 0 Source [] "String" "String"
    , StreamVertex 1 Map [[| show >>> length |]] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
test_mapFuse = assertEqual (applyRule mapFuse mapFusePre) mapFusePost

-- streamMap >>> streamScan --------------------------------------------------

mapScan :: RewriteRule
mapScan (Connect (Vertex v1@(StreamVertex i Map (f:ss) t1 _))
                 (Vertex v2@(StreamVertex _ Scan (g:a:_) _ t2))) =
    let v = StreamVertex i Scan ([| flip (flip $(f) >>> $(g)) |]:a:ss) t1 t2
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapScan _ = Nothing

mapScanPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Map [f] "Int" "Int"
    , StreamVertex 2 Scan [g,a] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
    where
        f = [| (+1) |]
        g = [| \c _ -> c +1|]
        a = [| 0 |]

mapScanPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Scan [[| flip (flip $(f) >>> $(g))|], [| $(a) |]] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
    where
        f = [| (+1) |]
        g = [| \c _ -> c +1|]
        a = [| 0 |]

test_mapScan = assertEqual (applyRule mapScan mapScanPre) mapScanPost

-- streamExpand >>> streamFilter f == streamMap (filter f) >>> streamExpand --

expandFilter :: RewriteRule
expandFilter (Connect (Vertex e@(StreamVertex j Expand _ t1 t2))
                      (Vertex f@(StreamVertex i Filter (p:_) _ _))) =
    let m = StreamVertex j Map [[| filter $(p) |]] t1 t1
        e'= StreamVertex i Expand [] t1 t2
    in  Just (replaceVertex f e' . replaceVertex e m)
expandFilter _ = Nothing

expandFilterPre = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Expand [] "[Int]" "Int"
    , StreamVertex 2 Filter [[|$(p)|]] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
    where
        p = [| (>3) |]

expandFilterPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Map [[|filter $(p) |]] "[Int]" "[Int]"
    , StreamVertex 2 Expand [] "[Int]" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
    where
        p = [| (>3) |]

test_expandFilter = assertEqual (applyRule expandFilter expandFilterPre) expandFilterPost

-- streamMap f >>> streamFilterAcc g a p == streamFilterAcc g a (f >>> p) >>> streamMap f

mapFilterAcc :: RewriteRule
mapFilterAcc (Connect (Vertex m@(StreamVertex i Map (f:_) t1 _))
                      (Vertex f1@(StreamVertex j FilterAcc (g:a:p:_) _ _))) =

    let f2 = StreamVertex i FilterAcc [g, a, [| ($f) >>> $(p) |]] t1 t1
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilterAcc _ = Nothing

mapFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Map [f] "Int" "String"
    , StreamVertex 2 FilterAcc [g,a,p] "String" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]
    where
        f = [| (+1) |]
        g = [| (\_ h -> (False, h)) |]
        a = [| (True, undefined) |]
        p = [| \new (b,old) -> b || old /= new |]

mapFilterAccPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [g,a, [| $(f) >>> $(p) |]] "Int" "Int"
    , StreamVertex 2 Map [f] "Int" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]
    where
        f = [| (+1) |]
        g = [| (\_ h -> (False, h)) |]
        a = [| (True, undefined) |]
        p = [| \new (b,old) -> b || old /= new |]

test_mapFilterAcc = assertEqual (applyRule mapFilterAcc mapFilterAccPre) mapFilterAccPost

-- streamMap f >>> streamWindow wm == streamWindow wm >>> streamMap (map f) --

mapWindow :: RewriteRule
mapWindow (Connect (Vertex m@(StreamVertex i Map (f:_) t1 _))
                   (Vertex w@(StreamVertex j Window (wm:_) _ t2))) =
    let t3 = "[" ++ t1 ++ "]"
        w2 = StreamVertex i Window [wm] t1 t3
        m2 = StreamVertex j Map [[| map $(f) |]] t3 t2
    in  Just (replaceVertex m w2 . replaceVertex w m2)

mapWindow _ = Nothing

mapWindowPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Map    [[| show |]] "Int" "String"
    , StreamVertex 2 Window [[| chop 2 |]] "String" "[String]"
    , StreamVertex 3 Sink   [] "[String]" "[String]"
    ]

mapWindowPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Window [[| chop 2 |]] "Int" "[Int]"
    , StreamVertex 2 Map    [[| map show |]] "[Int]" "[String]"
    , StreamVertex 3 Sink   [] "[String]" "[String]"
    ]

test_mapWindow = assertEqual (applyRule mapWindow mapWindowPre) mapWindowPost

-- streamExpand >>> streamMap f == streamMap (map f) >>> streamExpand --------
-- [a]           a            b   [a]               [b]               b

expandMap :: RewriteRule
expandMap (Connect (Vertex e@(StreamVertex i Expand _ t1 _))
                   (Vertex m@(StreamVertex j Map (f:_) _ t4))) =
    let t5 = "[" ++ t4 ++ "]"
        m2 = StreamVertex i Map [[| map $(f) |]] t1 t5
        e2 = StreamVertex j Expand [] t5 t4
    in  Just (replaceVertex m e2 . replaceVertex e m2)

expandMap _ = Nothing

expandMapPre = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Expand [] "[Int]" "Int"
    , StreamVertex 2 Map [[| show |]] "Int" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]

expandMapPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Map [[| map (show) |]] "[Int]" "[String]"
    , StreamVertex 2 Expand [] "[String]" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]

test_expandMap = assertEqual (applyRule expandMap expandMapPre) expandMapPost

-- streamExpand >>> streamScan f a == ----------------------------------------
--     streamFilter (not.null)
--         >>> streamScan (\b a' -> tail $ scanl f (last b) a') [a]
--         >>> streamExpand

expandScan :: RewriteRule
expandScan (Connect (Vertex  e@(StreamVertex i Expand (_)     t1 t2))
                    (Vertex sc@(StreamVertex j Scan   (f:a:_) _  t3))) =
    Just $ \g ->
        let t4 = "[" ++ t3 ++ "]"
            k  = newVertexId g
            p  = [| \b a' -> tail $ scanl $(f) (last b) a' |]

            f' = StreamVertex i Filter [[| not.null |]]  t1 t1
            sc'= StreamVertex j Scan   [p, [| [$(a)] |]] t1 t4
            e' = StreamVertex k Expand []                t4 t3

        in  overlay (path [f',sc',e']) $
            (removeEdge f' e' . replaceVertex e f' . replaceVertex sc e') g

expandScan _ = Nothing

expandScanPre = path
    [ StreamVertex 0 Source []    "[Int]" "[Int]"
    , StreamVertex 1 Expand []    "[Int]" "Int"
    , StreamVertex 2 Scan   [f,a] "Int"   "Int"
    , StreamVertex 3 Sink   []    "Int"   "Int"
    ]
    where
        f = [| \c _ -> c + 1 |]
        a = [| 0 |]

expandScanPost = path
    [ StreamVertex 0 Source []     "[Int]" "[Int]"
    , StreamVertex 1 Filter [p]    "[Int]" "[Int]"
    , StreamVertex 2 Scan   [g,as] "[Int]" "[Int]"
    , StreamVertex 4 Expand []     "[Int]" "Int"
    , StreamVertex 3 Sink   []     "Int"   "Int"
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
expandExpand (Connect (Vertex e@(StreamVertex i Expand _ t1 t2))
                      (Vertex   (StreamVertex j Expand _ _ _))) =
    let m = StreamVertex i Map [[| concat |]] t1 t2
    in  Just (replaceVertex e m)

expandExpand _ = Nothing

expandExpandPre = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Expand [] "[[Int]]" "[Int]"
    , StreamVertex 2 Expand [] "[Int]" "Int"
    , StreamVertex 3 Sink   [] "Int" "[Int]"
    ]

expandExpandPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Map [[| concat |]] "[[Int]]" "[Int]"
    , StreamVertex 2 Expand [] "[Int]" "Int"
    , StreamVertex 3 Sink   [] "Int" "[Int]"
    ]

test_expandExpand = assertEqual (applyRule expandExpand expandExpandPre)
    expandExpandPost

-- streamFilter f (streamMerge [s1, s2]) -------------------------------------
-- = streamMerge [streamFilter f s1, streamFilter f s2]

mergeFilter :: RewriteRule
mergeFilter = hoistOp Filter

-- | "hoist" an Operator (such as a Filter) upstream through a Merge operator.
hoistOp op (Connect (Vertex m@(StreamVertex i Merge _ _ ty))
                      (Vertex f@(StreamVertex j o pred _ ty'))) =

    if o /= op then Nothing
    else Just $ \g -> let

        mkOp g = StreamVertex (newVertexId g) op pred ty ty'

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

v1 = StreamVertex 0 Source []           "Int" "Int"
v2 = StreamVertex 1 Source []           "Int" "Int"
v3 = StreamVertex 2 Merge  []           "Int" "Int"
v4 = StreamVertex 3 Filter [[| (>3) |]] "Int" "Int"
v5 = StreamVertex 4 Sink   []           "Int" "Int"

mergeFilterPre = overlay (path [v1,v3,v4,v5]) (path [v2,v3])

v6 = StreamVertex 5 Filter [[| (>3) |]] "Int" "Int"
v7 = StreamVertex 6 Filter [[| (>3) |]] "Int" "Int"

mergeFilterPost = overlay (path [v1,v6,v3,v5]) (path [v2,v7,v3])

test_mergeFilter = assertEqual (applyRule mergeFilter mergeFilterPre)
    mergeFilterPost

-- streamExpand (streamMerge [s1, s2]) -------------------------------------
-- = streamMerge [streamExpand s1, streamExpand s2]

mergeExpand :: RewriteRule
mergeExpand = hoistOp Expand

v8  = StreamVertex 0 Source [] "[Int]" "[Int]"
v9  = StreamVertex 1 Source [] "[Int]" "[Int]"
v10 = StreamVertex 2 Merge  [] "[Int]" "[Int]"
v11 = StreamVertex 3 Expand [] "[Int]" "Int"

mergeExpandPre  = overlay (path [v8, v10, v11, v5]) (path [v9, v10])

v12 = StreamVertex 2 Merge  [] "Int" "Int"
v13 = StreamVertex 5 Expand [] "[Int]" "Int"
v14 = StreamVertex 6 Expand [] "[Int]" "Int"

mergeExpandPost = overlay (path [v8, v13, v12, v5]) (path [v9, v14, v12])

test_mergeExpand = assertEqual (applyRule mergeExpand mergeExpandPre)
    mergeExpandPost

-- streamMap (streamMerge [s1, s2]) ------------------------------------------
-- = streamMerge [streamMap s1, streamMap s2]

mergeMap :: RewriteRule
mergeMap = hoistOp Map

v15 = StreamVertex 0 Source [] "Int" "Int"
v16 = StreamVertex 1 Source [] "Int" "Int"
v17 = StreamVertex 2 Merge []  "Int" "Int"
v18 = StreamVertex 3 Map [[| show |]]  "Int" "String"
v19 = StreamVertex 4 Sink [] "String" "String"

mergeMapPre = overlay (path [v15,v17,v18,v19]) (path [v16,v17])

v20 = StreamVertex 5 Map [[| show |]]  "Int" "String"
v21 = StreamVertex 6 Map [[| show |]]  "Int" "String"
v22 = StreamVertex 2 Merge [] "String" "String"

mergeMapPost = overlay (path [v15,v20,v22,v19]) (path [v16,v21,v22])

test_mergeMap = assertEqual (applyRule mergeMap mergeMapPre) mergeMapPost

-- streamMerge [streamMap f s1, streamMap f s2]
--     == streamMap f (streamMerge [s1,s2])

identicalParams :: [StreamVertex] -> Bool
identicalParams inbound =
    let params = (map.map) deQ (map parameters inbound)
    in  and (map (==(head params)) (tail params))

mapMerge :: RewriteRule
mapMerge = pushOp Map

pushOp op (Connect (Vertex ma@(StreamVertex i o fs t1 t2))
                  (Vertex me@(StreamVertex j Merge _ t3 _))) =

    if o /= op then Nothing
    else Just $ \g -> let

        inbound = map fst . filter ((me==) . snd) . edgeList $ g
        -- the pattern match is not enough to be conclusive that this applies
        in  if [op] == nub (map operator inbound) && identicalParams inbound
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

mapMergePre  = mergeMapPost
mapMergePost = overlay (path [v15,v17,v18 {vertexId = 7}, v19]) (path [v16,v17])

test_mapMerge = assertEqual (applyRule mapMerge mapMergePre) mapMergePost

-- streamMerge [streamFilter p s1, streamFilter p s2] ------------------------
-- == streamFilter p (streamMerge [s1,s2])

filterMerge :: RewriteRule
filterMerge = pushOp Filter

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
mergeFuse (Connect (Vertex m1@(StreamVertex i Merge _ _ _))
                   (Vertex m2@(StreamVertex j Merge _ _ _))) =
    Just (removeEdge m1 m1 . mergeVertices (`elem` [m1,m2]) m1)

mergeFuse _ = Nothing

v23 = StreamVertex 0 Source [] "Int" "Int"
v24 = StreamVertex 1 Source [] "Int" "Int"
v25 = StreamVertex 2 Source [] "Int" "Int"
v26 = StreamVertex 3 Merge []  "Int" "Int"
v27 = StreamVertex 4 Merge []  "Int" "Int"
v28 = StreamVertex 5 Sink []   "Int" "Int"

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
