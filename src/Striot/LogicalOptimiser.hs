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
import Data.List (nub)

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

-- utility/boilerplate -------------------------------------------------------

main = htfMain htf_thisModulesTests
