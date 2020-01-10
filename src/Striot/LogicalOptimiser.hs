{-# OPTIONS_GHC -F -pgmF htfpp #-}

module Striot.LogicalOptimiser ( applyRules
                               , costModel

                               , htf_thisModulesTests
                               ) where

import Striot.StreamGraph
import Algebra.Graph
import Test.Framework hiding ((===))
import Data.Maybe (mapMaybe, fromMaybe)
import Data.Function ((&))
import Data.List (nub, sort)

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
        ]

-- streamFilter f >>> streamFilter g = streamFilter (\x -> f x && g x) -------

filterFuse :: RewriteRule
filterFuse (Connect (Vertex a@(StreamVertex i Filter (p:s:[]) ty _))
                    (Vertex b@(StreamVertex _ Filter (q:_) _ _))) =

    let c = StreamVertex i Filter ["((\\p q x -> p x && q x) ("++p++") ("++q++"))", s] ty ty
    in  Just (removeEdge c c . mergeVertices (`elem` [a,b]) c)

filterFuse _ = Nothing

f3 = Vertex $ StreamVertex 0 Filter ["(>3)", "s"] "String" "String"
f4 = Vertex $ StreamVertex 1 Filter ["(<5)", "s"] "String" "String"
filterFusePre = Connect f3 f4
filterFusePost = Vertex $ StreamVertex 0 Filter
    ["((\\p q x -> p x && q x) ((>3)) ((<5)))", "s"] "String" "String"

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

m1 = Vertex $ StreamVertex 0 Map ["show","s"] "Int" "String"
f1 = Vertex $ StreamVertex 1 Filter ["\\x -> length x <3","s"] "String" "String"

f2 = Vertex $ StreamVertex 0 Filter ["(\\x -> length x <3).(show)","s"] "Int" "Int"
m2 = Vertex $ StreamVertex 1 Map ["show","s"] "Int" "String"

mapFilterPre = m1 `Connect` f1
mapFilterPost = f2 `Connect` m2

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

filterFilterAccPre = Vertex (StreamVertex 3 Filter ["p","s"] "Int" "Int")
                     `Connect`
                     Vertex (StreamVertex 2 FilterAcc ["f","a","q","s"] "Int" "Int")
filterFilterAccPost = Vertex $
  StreamVertex 3 FilterAcc [ "(let p = (p); f = (f) in \\a v -> if p v then f a v else a)"
                           , "a"
                           , "(let p = (p); q = (q) in \\v a -> p v && q v a)"
                           , "s"
                           ] "Int" "Int"

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

filterAccFilterPre  = Vertex (StreamVertex 1 FilterAcc ["f","a","p","s"] "Int" "Int")
                      `Connect`
                      Vertex (StreamVertex 2 Filter ["q","s"] "Int" "Int")
filterAccFilterPost = Vertex $
    StreamVertex 1 FilterAcc [ "f", "a"
                             , "(let p = (p); q = (q) in \\v a -> p v a && q v)"
                             , "s"
                             ] "Int" "Int"

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

filterAccFilterAccPre  = Vertex (StreamVertex 1 FilterAcc ["f","a","p","s"] "Int" "Int")
                         `Connect`
                         Vertex (StreamVertex 2 FilterAcc ["g","b","q","s"] "Int" "Int")
filterAccFilterAccPost = Vertex $
    StreamVertex 1 FilterAcc [ "(let f = (f); p = (p); g = (g) in \\ (a,b) v -> (f a v, if p v a then g b v else b))"
                             , "(a,b)"
                             , "(let p = (p); q = (q) in \\v (y,z) -> p v y && q v z)"
                             , "s"
                             ] "Int" "Int"
test_filterAccFilterAcc = assertEqual (applyRule filterAccFilterAcc filterAccFilterAccPre)
    filterAccFilterAccPost

-- streamMap >>> streamMap ---------------------------------------------------

mapFuse :: RewriteRule
mapFuse (Connect (Vertex v1@(StreamVertex i Map (f:s:[]) t1 _))
                 (Vertex v2@(StreamVertex _ Map (g:_) _ t2))) =
    let v = StreamVertex i Map ["(let f = ("++f++"); g = ("++g++") in (f >>> g))",s] t1 t2
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapFuse _ = Nothing

mapFusePre = Vertex (StreamVertex 0 Map ["show", "s"] "Int" "String") `Connect`
  Vertex (StreamVertex 1 Map ["length"] "String" "Int")
mapFusePost = Vertex $ StreamVertex 0 Map ["(let f = (show); g = (length) in (f >>> g))", "s"] "Int" "Int"
test_mapFuse = assertEqual (applyRule mapFuse mapFusePre) mapFusePost

-- streamMap >>> streamScan --------------------------------------------------

mapScan :: RewriteRule
mapScan (Connect (Vertex v1@(StreamVertex i Map (f:s:[]) t1 _))
                 (Vertex v2@(StreamVertex _ Scan (g:a:_) _ t2))) =
    let v = StreamVertex i Scan ["(let f = ("++f++"); g = ("++g++") in (flip (flip f >>> g)))", a, s] t1 t2
    in  Just (removeEdge v v . mergeVertices (`elem` [v1,v2]) v)
mapScan _ = Nothing

mapScanPre = Vertex (StreamVertex 1 Map ["f","s"] "Int" "Int") `Connect`
    Vertex (StreamVertex 2 Scan ["g","a"] "Int" "Int")
mapScanPost = Vertex $
    StreamVertex 1 Scan ["(let f = (f); g = (g) in (flip (flip f >>> g)))", "a", "s"] "Int" "Int"
test_mapScan = assertEqual (applyRule mapScan mapScanPre) mapScanPost

-- streamExpand >>> streamFilter f == streamMap (filter f) >>> streamExpand --

expandFilter :: RewriteRule
expandFilter (Connect (Vertex e@(StreamVertex j Expand [s] t1 t2))
                      (Vertex f@(StreamVertex i Filter (p:s':[]) _ _))) =
    let m = StreamVertex j Map ["(filter ("++p++"))",s] t1 t1
        e'= StreamVertex i Expand [s'] t1 t2
    in  Just (replaceVertex f e' . replaceVertex e m)
expandFilter _ = Nothing

expandFilterPre =
    Vertex (StreamVertex 1 Expand ["foo"] "[Int]" "Int")
    `Connect`
    Vertex (StreamVertex 2 Filter ["p","bar"] "Int" "Int")
expandFilterPost =
    Vertex (StreamVertex 1 Map ["(filter (p))", "foo"] "[Int]" "[Int]")
    `Connect`
    Vertex (StreamVertex 2 Expand ["bar"] "[Int]" "Int")

test_expandFilter = assertEqual (applyRule expandFilter expandFilterPre) expandFilterPost

-- streamMap f >>> streamFilterAcc g a p == streamFilterAcc g a (f >>> p) >>> streamMap f

mapFilterAcc :: RewriteRule
mapFilterAcc (Connect (Vertex m@(StreamVertex i Map (f:s:[]) t1 _))
                      (Vertex f1@(StreamVertex j FilterAcc (g:a:p:ps) _ _))) =

    let f2 = StreamVertex i FilterAcc [g, a, ("("++f++")>>>("++p++")"), s] t1 t1
        m2 = m { vertexId = j }
    in  Just (replaceVertex f1 m2 . replaceVertex m f2)

mapFilterAcc _ = Nothing

mapFilterAccPre =
    Vertex (StreamVertex 0 Map ["f","s"] "Int" "String")
    `Connect`
    Vertex (StreamVertex 1 FilterAcc ["g","a","p","s'"] "String" "String")

mapFilterAccPost =
    Vertex (StreamVertex 0 FilterAcc ["g","a","(f)>>>(p)","s"] "Int" "Int")
    `Connect`
    Vertex (StreamVertex 1 Map ["f","s"] "Int" "String")

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

mapWindowPre =
    Vertex (StreamVertex 0 Map ["show","s"] "Int" "String")
    `Connect`
    Vertex (StreamVertex 1 Window ["chop 2", "s'"] "String" "[String]")

mapWindowPost =
    Vertex (StreamVertex 0 Window ["chop 2", "s"] "Int" "[Int]")
    `Connect`
    Vertex (StreamVertex 1 Map ["map (show)", "s'"] "[Int]" "[String]")

test_mapWindow = assertEqual (applyRule mapWindow mapWindowPre) mapWindowPost

-- streamExpand >>> streamMap f == streamMap (map f) >>> streamExpand --------
-- [a]           a            b   [a]               [b]               b

expandMap :: RewriteRule
expandMap (Connect (Vertex e@(StreamVertex i Expand _ t1 _))
                   (Vertex m@(StreamVertex j Map (f:s:[]) _ t4))) =
    let t5 = "[" ++ t4 ++ "]"
        m2 = StreamVertex i Map ["map ("++f++")",s] t1 t5
        e2 = StreamVertex j Expand [] t5 t4
    in  Just (replaceVertex m e2 . replaceVertex e m2)

expandMap _ = Nothing

expandMapPre =
    Vertex (StreamVertex 0 Expand [] "[Int]" "Int")
    `Connect`
    Vertex (StreamVertex 1 Map ["show","s"] "Int" "String")

expandMapPost =
    Vertex (StreamVertex 0 Map ["map (show)","s"] "[Int]" "[String]")
    `Connect`
    Vertex (StreamVertex 1 Expand [] "[String]" "String")

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

expandExpandPre =
    Vertex (StreamVertex 0 Expand [] "[[Int]]" "[Int]")
    `Connect`
    Vertex (StreamVertex 1 Expand [] "[Int]" "Int")

expandExpandPost =
    Vertex (StreamVertex 0 Map ["concat","s"] "[[Int]]" "[Int]")
    `Connect`
    Vertex (StreamVertex 1 Expand [] "[Int]" "Int")

test_expandExpand = assertEqual (applyRule expandExpand expandExpandPre)
    expandExpandPost

-- utility/boilerplate -------------------------------------------------------

-- generate a new vertexId which doesn't clash with any existing ones
-- TODO this will still break the assumption that CompileIoT makes regarding
-- vertexId ordering in a path. We either need to renumber all subsequent nodes
-- or remove that requirement in CompileIoT
newVertexId :: StreamGraph -> Int
newVertexId = succ . last . sort . map vertexId . vertexList

main = htfMain htf_thisModulesTests
