{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Striot.LogicalOptimiser ( applyRules
                               , costModel
                               , optimise
                               , optimiseWriteOut

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

-- | Optimise a StreamGraph and write the result out as Haskell source
-- code to the supplied FilePath, along with some of the necessary
-- supporting code.
optimiseWriteOut :: FilePath -> StreamGraph -> IO ()
optimiseWriteOut fn =
    writeFile fn . template . show . simplify . optimise

template g = intercalate "\n"
    [ "import Striot.StreamGraph"
    , "import Algebra.Graph"
    , "\n"
    , "graph = " ++ g
    ]

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
        , windowExpandWindow
        , mergeFilter 
        , mergeExpand
        , mergeMap
        , mapMerge
        , filterMerge
        ]

-- streamFilter f >>> streamFilter g = streamFilter (\x -> f x && g x) -------

filterFuse :: RewriteRule
filterFuse (Connect (Vertex a@(StreamVertex i Filter (p:s:[]) ty _))
                    (Vertex b@(StreamVertex _ Filter (q:_) _ _))) =

    let c = StreamVertex i Filter ["((\\p q x -> p x && q x) ("++p++") ("++q++"))", s] ty ty
    in  Just (removeEdge c c . mergeVertices (`elem` [a,b]) c)

filterFuse _ = Nothing

so' = StreamVertex 0 Source [] "String" "String"
f3  = StreamVertex 1 Filter ["(>3)", "s"] "String" "String"
f4  = StreamVertex 2 Filter ["(<5)", "s"] "String" "String"
si' = StreamVertex 3 Sink [] "String" "String"

filterFusePre = path [so', f3, f4, si']

filterFusePost = path [ so'
    , StreamVertex 1 Filter ["((\\p q x -> p x && q x) ((>3)) ((<5)))", "s"] "String" "String"
    , si' ]

test_filterFuse = assertEqual (applyRule filterFuse filterFusePre)
    filterFusePost

-- streamMap f >>> streamFilter p = streamFilter (f >>> p) >>> streamMap f ---

mapFilter :: RewriteRule
mapFilter (Connect (Vertex m@(StreamVertex i Map (f:s:[]) intype _))
                   (Vertex f1@(StreamVertex j Filter (p:ps) _ _))) =

    let f2 = StreamVertex i Filter [("("++p++").("++f++")"),s] intype intype
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilter _ = Nothing

m1 = StreamVertex 1 Map ["show","s"] "Int" "String"
f1 = StreamVertex 2 Filter ["\\x -> length x <3","s"] "String" "String"

f2 = StreamVertex 1 Filter ["(\\x -> length x <3).(show)","s"] "Int" "Int"
m2 = StreamVertex 2 Map ["show","s"] "Int" "String"

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
filterFilterAcc (Connect (Vertex v1@(StreamVertex i Filter (p:s:[]) ty _))
                         (Vertex v2@(StreamVertex _ FilterAcc (f:a:q:_) _ _))) =
    let v3 = StreamVertex i FilterAcc [ "(let p = ("++p++"); f = ("++f++") in \\a v -> if p v then f a v else a)"
                                      , a
                                      , "(let p = ("++p++"); q = ("++q++") in \\v a -> p v && q v a)"
                                      , s ] ty ty
    in  Just (removeEdge v3 v3 . mergeVertices (`elem` [v1,v2]) v3)
filterFilterAcc _ = Nothing

filterFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Filter ["p","s"] "Int" "Int"
    , StreamVertex 2 FilterAcc ["f","a","q","s"] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

filterFilterAccPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [ "(let p = (p); f = (f) in \\a v -> if p v then f a v else a)"
                               , "a"
                               , "(let p = (p); q = (q) in \\v a -> p v && q v a)"
                               , "s"
                               ] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

test_filterFilterAcc = assertEqual (applyRule filterFilterAcc filterFilterAccPre)
    filterFilterAccPost

-- streamFilterAcc >>> streamFilter ------------------------------------------

filterAccFilter :: RewriteRule
filterAccFilter (Connect (Vertex v1@(StreamVertex i FilterAcc (f:a:p:s:[]) ty _))
                         (Vertex v2@(StreamVertex _ Filter (q:_) _ _))) =
    let p' = "(let p = ("++p++"); q = ("++q++") in \\v a -> p v a && q v)"
        v  = StreamVertex i FilterAcc [f,a,p',s] ty ty
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilter _ = Nothing

filterAccFilterPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc ["f","a","p","s"] "Int" "Int"
    , StreamVertex 2 Filter ["q","s"] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

filterAccFilterPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [ "f", "a"
                               , "(let p = (p); q = (q) in \\v a -> p v a && q v)"
                               , "s"
                               ] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

test_filterAccFilter = assertEqual (applyRule filterAccFilter filterAccFilterPre)
    filterAccFilterPost

-- streamFilterAcc >>> streamFilterAcc ---------------------------------------

filterAccFilterAcc :: RewriteRule
filterAccFilterAcc (Connect (Vertex v1@(StreamVertex i FilterAcc (f:a:p:s:[]) ty _))
                            (Vertex v2@(StreamVertex _ FilterAcc (g:b:q:_) _ _))) =
    let f' = "(let f = ("++f++"); p = ("++p++"); g = ("++g++") in\
             \ \\ (a,b) v -> (f a v, if p v a then g b v else b))"
        a' = "("++a++","++b++")"
        q' = "(let p = ("++p++"); q = ("++q++") in \\v (y,z) -> p v y && q v z)"
        v  = StreamVertex i FilterAcc [f',a',q',s] ty ty
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
filterAccFilterAcc _ = Nothing

filterAccFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc ["f","a","p","s"] "Int" "Int"
    , StreamVertex 2 FilterAcc ["g","b","q","s"] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

filterAccFilterAccPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc [ "(let f = (f); p = (p); g = (g) in \\ (a,b) v -> (f a v, if p v a then g b v else b))"
                               , "(a,b)"
                               , "(let p = (p); q = (q) in \\v (y,z) -> p v y && q v z)"
                               , "s"
                               ] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
test_filterAccFilterAcc = assertEqual (applyRule filterAccFilterAcc filterAccFilterAccPre)
    filterAccFilterAccPost

-- streamMap >>> streamMap ---------------------------------------------------

mapFuse :: RewriteRule
mapFuse (Connect (Vertex v1@(StreamVertex i Map (f:s:[]) t1 _))
                 (Vertex v2@(StreamVertex _ Map (g:_) _ t2))) =
    let v = StreamVertex i Map ["(let f = ("++f++"); g = ("++g++") in (f >>> g))",s] t1 t2
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapFuse _ = Nothing

mapFusePre = path
    [ StreamVertex 0 Source [] "String" "String"
    , StreamVertex 1 Map ["show", "s"] "Int" "String"
    , StreamVertex 2 Map ["length"] "String" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

mapFusePost = path
    [ StreamVertex 0 Source [] "String" "String"
    , StreamVertex 1 Map ["(let f = (show); g = (length) in (f >>> g))", "s"] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]
test_mapFuse = assertEqual (applyRule mapFuse mapFusePre) mapFusePost

-- streamMap >>> streamScan --------------------------------------------------

mapScan :: RewriteRule
mapScan (Connect (Vertex v1@(StreamVertex i Map (f:s:[]) t1 _))
                 (Vertex v2@(StreamVertex _ Scan (g:a:_) _ t2))) =
    let v = StreamVertex i Scan ["(let f = ("++f++"); g = ("++g++") in (flip (flip f >>> g)))", a, s] t1 t2
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapScan _ = Nothing

mapScanPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Map ["f","s"] "Int" "Int"
    , StreamVertex 2 Scan ["g","a"] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

mapScanPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Scan ["(let f = (f); g = (g) in (flip (flip f >>> g)))", "a", "s"] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

test_mapScan = assertEqual (applyRule mapScan mapScanPre) mapScanPost

-- streamExpand >>> streamFilter f == streamMap (filter f) >>> streamExpand --

expandFilter :: RewriteRule
expandFilter (Connect (Vertex e@(StreamVertex j Expand [s] t1 t2))
                      (Vertex f@(StreamVertex i Filter (p:s':[]) _ _))) =
    let m = StreamVertex j Map ["(filter ("++p++"))",s] t1 t1
        e'= StreamVertex i Expand [s'] t1 t2
    in  Just (replaceVertex f e' . replaceVertex e m)
expandFilter _ = Nothing

expandFilterPre = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Expand ["foo"] "[Int]" "Int"
    , StreamVertex 2 Filter ["p","bar"] "Int" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

expandFilterPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Map ["(filter (p))", "foo"] "[Int]" "[Int]"
    , StreamVertex 2 Expand ["bar"] "[Int]" "Int"
    , StreamVertex 3 Sink [] "Int" "Int"
    ]

test_expandFilter = assertEqual (applyRule expandFilter expandFilterPre) expandFilterPost

-- streamMap f >>> streamFilterAcc g a p == streamFilterAcc g a (f >>> p) >>> streamMap f

mapFilterAcc :: RewriteRule
mapFilterAcc (Connect (Vertex m@(StreamVertex i Map (f:s:[]) t1 _))
                      (Vertex f1@(StreamVertex j FilterAcc (g:a:p:ps) _ _))) =

    let f2 = StreamVertex i FilterAcc [g, a, ("("++f++")>>>("++p++")"), s] t1 t1
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilterAcc _ = Nothing

mapFilterAccPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Map ["f","s"] "Int" "String"
    , StreamVertex 2 FilterAcc ["g","a","p","s'"] "String" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]

mapFilterAccPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 FilterAcc ["g","a","(f)>>>(p)","s"] "Int" "Int"
    , StreamVertex 2 Map ["f","s"] "Int" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]

test_mapFilterAcc = assertEqual (applyRule mapFilterAcc mapFilterAccPre) mapFilterAccPost

-- streamMap f >>> streamWindow wm == streamWindow wm >>> streamMap (map f) --

mapWindow :: RewriteRule
mapWindow (Connect (Vertex m@(StreamVertex i Map (f:s:[]) t1 _))
                   (Vertex w@(StreamVertex j Window (wm:s':[]) _ t2))) =
    let t3 = "[" ++ t1 ++ "]"
        w2 = StreamVertex i Window [wm,s] t1 t3
        m2 = StreamVertex j Map ["map ("++f++")",s'] t3 t2
    in  Just (replaceVertex m w2 . replaceVertex w m2)

mapWindow _ = Nothing

mapWindowPre = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Map ["show","s"] "Int" "String"
    , StreamVertex 2 Window ["chop 2", "s'"] "String" "[String]"
    , StreamVertex 3 Sink [] "[String]" "[String]"
    ]

mapWindowPost = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Window ["chop 2", "s"] "Int" "[Int]"
    , StreamVertex 2 Map ["map (show)", "s'"] "[Int]" "[String]"
    , StreamVertex 3 Sink [] "[String]" "[String]"
    ]

test_mapWindow = assertEqual (applyRule mapWindow mapWindowPre) mapWindowPost

-- streamExpand >>> streamMap f == streamMap (map f) >>> streamExpand --------
-- [a]           a            b   [a]               [b]               b

expandMap :: RewriteRule
expandMap (Connect (Vertex e@(StreamVertex i Expand _ t1 _))
                   (Vertex m@(StreamVertex j Map (f:s:[]) _ t4))) =
    let t5 = "[" ++ t4 ++ "]"
        m2 = StreamVertex i Map ["map ("++f++")",s] t1 t5
        e2 = StreamVertex j Expand ["s"] t5 t4
    in  Just (replaceVertex m e2 . replaceVertex e m2)

expandMap _ = Nothing

expandMapPre = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Expand [] "[Int]" "Int"
    , StreamVertex 2 Map ["show","s"] "Int" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]

expandMapPost = path
    [ StreamVertex 0 Source [] "[Int]" "[Int]"
    , StreamVertex 1 Map ["map (show)","s"] "[Int]" "[String]"
    , StreamVertex 2 Expand ["s"] "[String]" "String"
    , StreamVertex 3 Sink [] "String" "String"
    ]

test_expandMap = assertEqual (applyRule expandMap expandMapPre) expandMapPost

-- streamExpand >>> streamScan f a == ----------------------------------------
--     streamFilter (not.null)
--         >>> streamScan (\b a' -> tail $ scanl f (last b) a') [a]
--         >>> streamExpand

expandScan :: RewriteRule
expandScan (Connect (Vertex  e@(StreamVertex i Expand (s:[])      t1 t2))
                    (Vertex sc@(StreamVertex j Scan   (f:a:s':[]) _  t3))) =
    Just $ \g ->
        let t4 = "[" ++ t3 ++ "]"
            k  = newVertexId g
            p  = "(\\b a' -> tail $ scanl ("++f++") (last b) a')"

            f' = StreamVertex i Filter ["not.null", s]    t1 t1
            sc'= StreamVertex j Scan   [p,"["++a++"]",s'] t1 t4
            e' = StreamVertex k Expand []                 t4 t3

        in  overlay (path [f',sc',e']) $
            (removeEdge f' e' . replaceVertex e f' . replaceVertex sc e') g

expandScan _ = Nothing

expandScanPre = path
    [ StreamVertex 0 Source []             "[Int]" "[Int]"
    , StreamVertex 1 Expand ["s"]          "[Int]" "Int"
    , StreamVertex 2 Scan   ["f","a","s'"] "Int"   "Int"
    , StreamVertex 3 Sink   []             "Int"   "Int"
    ]

expandScanPost = path
    [ StreamVertex 0 Source []               "[Int]" "[Int]"
    , StreamVertex 1 Filter ["not.null","s"] "[Int]" "[Int]"
    , StreamVertex 2 Scan   [p,"[a]","s'"]   "[Int]" "[Int]"
    , StreamVertex 4 Expand []               "[Int]" "Int"
    , StreamVertex 3 Sink   []               "Int"   "Int"
    ] where
    p  = "(\\b a' -> tail $ scanl (f) (last b) a')"

test_expandScan = assertEqual (simplify$applyRule expandScan expandScanPre) expandScanPost

-- streamExpand >>> streamExpand == streamMap concat >>> streamExpand --------
-- [[a]]        [a]             a  [[a]]            [a]              a

expandExpand :: RewriteRule
expandExpand (Connect (Vertex e@(StreamVertex i Expand _ t1 t2))
                      (Vertex   (StreamVertex j Expand _ _ _))) =
    let m = StreamVertex i Map ["concat","s"] t1 t2
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
    , StreamVertex 1 Map ["concat","s"] "[[Int]]" "[Int]"
    , StreamVertex 2 Expand [] "[Int]" "Int"
    , StreamVertex 3 Sink   [] "Int" "[Int]"
    ]

test_expandExpand = assertEqual (applyRule expandExpand expandExpandPre)
    expandExpandPost

-- streamWindow w >>> streamExpand >>> streamWindow w == streamWindow w ------

windowExpandWindow :: RewriteRule
windowExpandWindow g =
    let vs = take 3 (vertexList g)
        appropriate = map operator vs == [Window, Expand, Window] &&
                      (parameters . head) vs == (parameters . last) vs
        [w,e,w'] = vs
    in if   not appropriate then Nothing
       else Just (simplify . removeEdge w' w' . mergeVertices (`elem` [w,e]) w')

windowExpandWindowPre = path
    [ StreamVertex 0 Window ["chop 2"] "Int" "[Int]"
    , StreamVertex 1 Expand [] "[Int]" "Int"
    , StreamVertex 2 Window ["chop 2"] "Int" "[Int]"
    ]
windowExpandWindowPost = Vertex $ StreamVertex 2 Window ["chop 2"] "Int" "[Int]"

test_windowExpandWindow = assertEqual (applyRule windowExpandWindow windowExpandWindowPre)
    windowExpandWindowPost

-- additional test to ensure that edges from nodes outside of the matching sub-graph
-- are correctly rewritten 
windowExpandWindowPre2 = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 1 Window ["chop 2"] "Int" "[Int]"
    , StreamVertex 2 Expand [] "[Int]" "Int"
    , StreamVertex 3 Window ["chop 2"] "Int" "[Int]"
    , StreamVertex 4 Sink [] "[Int]" "[Int]"
    ]
windowExpandWindowPost2 = path
    [ StreamVertex 0 Source [] "Int" "Int"
    , StreamVertex 3 Window ["chop 2"] "Int" "[Int]"
    , StreamVertex 4 Sink [] "[Int]" "[Int]"
    ]

test_windowExpandWindow2 = assertEqual (applyRule windowExpandWindow windowExpandWindowPre2)
    windowExpandWindowPost2

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
v4 = StreamVertex 3 Filter ["(>3)","s"] "Int" "Int"
v5 = StreamVertex 4 Sink   []           "Int" "Int"

mergeFilterPre = overlay (path [v1,v3,v4,v5]) (path [v2,v3])

v6 = StreamVertex 5 Filter ["(>3)","s"] "Int" "Int"
v7 = StreamVertex 6 Filter ["(>3)","s"] "Int" "Int"

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
v18 = StreamVertex 3 Map ["show","s"]  "Int" "String"
v19 = StreamVertex 4 Sink [] "String" "String"

mergeMapPre = overlay (path [v15,v17,v18,v19]) (path [v16,v17])

v20 = StreamVertex 5 Map ["show","s"]  "Int" "String"
v21 = StreamVertex 6 Map ["show","s"]  "Int" "String"
v22 = StreamVertex 2 Merge [] "String" "String"

mergeMapPost = overlay (path [v15,v20,v22,v19]) (path [v16,v21,v22])

test_mergeMap = assertEqual (applyRule mergeMap mergeMapPre) mergeMapPost

-- streamMerge [streamMap s1, streamMap s2] == streamMap (streamMerge [s1,s2])

mapMerge :: RewriteRule
mapMerge = pushOp Map

pushOp op (Connect (Vertex ma@(StreamVertex i o fs t1 t2))
                  (Vertex me@(StreamVertex j Merge _ t3 _))) =

    if o /= op then Nothing
    else Just $ \g -> let

        inbound = map fst . filter ((me==) . snd) . edgeList $ g
        -- the pattern match is not enough to be conclusive that this applies
        in  if [op] == nub (map operator inbound) &&
            1 == length (nub (map parameters inbound))
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
