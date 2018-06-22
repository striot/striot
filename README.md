# Striot — functional stream processing for IoT

This is a haskell stream processing engine for IoT.  It is built as a cabal module, to build it:

```
$ cabal build
$ cabal install
```

Following this it can be imported in a standard way.

We model a stream as a (possibly infinite) list of events. Each event may or may not be timestamped:


```haskell

data Event alpha = Event { eventId :: Int
                         , time    :: Maybe Timestamp
                         , value   :: Maybe alpha}
     deriving (Eq, Ord, Show, Read, Generic)

type Timestamp    = UTCTime

type Stream alpha = [Event alpha]
```

The Event can be defined without a value (a `timedEvent`), and is used for time-based operations as will be described later.
Note that an Event can hold data of any type (e.g. integers, strings, tuples, lists, trees, graphs and even functions). An important principle is that separation between the view of the system seen by the user and the infrastructure: the details of the processing infrastructure are hidden allowing for multiple different implementations, and the addition of functionality to transparently support non-functional requirements. The functions that we provide for the user to process streams allow the user to only see a simple view of a stream as a list of type alpha: [alpha]

In the rest of the  note we describe the stream operators that can be contained in a stream graph.

## Stream Operators

One approach to functional stream processing would be to define the stream datatype (so allowing interoperability) but then allow the user to define any functions require that consume and generate streams. We have taken a different approach - by analysis of the literature on stream and complex event processing, and by experimenting with the implementation of a range of applications, we have defined and implemented a library of key functions that an application developer can use to create applications. We believe that this will make it easier to create applications, and it also means that – as will be described – other parts of the infrastructure (including an optimiser) can take advantage of knowledge of the semantics and performance characteristics of these functions that would be difficult to derive automatically from arbitrary, user-defined, functions.
There are 6 main tasks any stream processing system must perform:

* Filter   (select only those events that match a criterion)
* Map      (transform all events into another type of event)
* Window   (break a stream of events into a stream of windows of events)
* Merge    (combine streams of the same type)
* Join     (combine streams of different types)

We now define a set of Haskell functions that implement these operators on the stream datatype defined in the previous section.

### Filter

The streamFilter function takes a stream as input, and generates a stream containing only those events that meet a user-provided criteria. The type signature is:


```haskell
streamFilter:: EventFilter alpha -> -- Function applied to each input
                                    --   event to determine if it
                                    --   meets the criteria
               Stream alpha      -> -- The input  stream
               Stream alpha         -- The output stream

type EventFilter alpha = alpha -> Bool -- type of the filter function
```

This illustrates the fact that the user does not need to know or understand the exact format of the events as seen by the infrastructure: they only need provide a function of type EventFilter to filter out all events whose value does not meet a particular criterion. This user-provided function is applied by the infrastructure to each the value in each element of the stream, and only those that return “True” are selected.

To show how filterStream can be used, assume a temperature sensor generates a stream of events of type Int.

```haskell
tempSensor:: Stream Int
```

If we are interested only in temperatures of over 100 then the user can define a function:

```haskell
over100:: EventFilter Int
over100 temp = temp>100
```

and stream can be filtered:

```haskell
streamFilter over100 tempSensor
```

Alternatively, the user may choose to use lambda notation to provide an unnamed function, and so the filter can also be written as:

```haskell
streamFilter (\temp->temp>100) tempSensor
```

### Filtering with an Accumulating Parameter

Sometimes, it is necessary to filter events based partly on past results. This can be done using the function streamFilterAcc whose signature is as follows:

```haskell
streamFilterAcc:: (beta -> alpha -> beta) ->  -- the accumulator fn:
                                              --- takes accumulator &
                                              --- head of stream
                                              --- and computes
                                              --- the new accumulator
                  beta ->                     -- initial accumulator value
                  (alpha -> beta -> Bool) ->  -- the filter fn: takes head
                                              --- of stream & accumulator
                  Stream alpha ->             -- input stream
                  Stream alpha                -- output stream
```

An example of its use is where we only want to propagate an event if its value is different to the previous event (though the timestamp may differ). This can be written:

```haskell
changes:: Eq alpha=> Stream alpha -> Stream alpha
changes s = streamFilterAcc (\_ h -> h)
                            (value $ head s)
                            (/=)
                            (tail s)
```

Another example is a function that samples its input, only passing through one event in n:

```haskell
sample:: Int -> Stream alpha -> Stream alpha
sample n s = streamFilterAcc (\acc h -> if acc==0 then n else acc-1)
                             n
                             (\h acc -> acc==0)
                             s
```

### Mapping Streams

The function mapStream is used to transform the values in a stream. The user supplies a function of type EventMap which is applied to the value within each event in the input stream to generate the output stream. The function streamMap has the signature:

```haskell
streamMap:: EventMap alpha beta -> -- the user-supplied map function
            Stream alpha        -> -- input stream
            Stream beta            -- output stream

type EventMap alpha beta = alpha -> beta -- type of the
                                         -- user-supplied map function
```

To illustrate this with the running example, let us say that after filtering all temperatures over 100, the user wants to use 100 as the baseline temperature, and represent all temperatures as their value over 100. To do this we can define a function:

```haskell
amountOver100:: EventMap Int Int
amountOver100 temp = temp-100
```

and then include this in the application:

```haskell
streamMap amountOver100 $ streamFilter over100 tempSensor
```

again, this can also be written using anonymous functions:

```haskell
streamMap (\temp->temp-100) $ streamFilter (\temp->temp>100) tempSensor
```

Note that the user does not have to understand the format of events, nor how map or filter are enacted. Instead they can focus solely on how the data in the events is to be processed.

### Scan

There is sometimes the need for a map function whose output depends on the history of events.

```haskell
streamScan:: (beta -> alpha -> beta) ->  -- the accumulator fn:
                                         --  takes head of stream &
                                         --  accumulator and computes
                                         --  the new accumulator
              beta ->                    -- initial accumulator value
              Stream alpha ->            -- input stream
              Stream beta                -- output stream
```

An example is emitting an event giving a count of the number of events received so far in the stream:

```haskell
counter:: Stream alpha -> Stream Int
counter s = scanStream (+1) 0 s
```

### Expand

Sometimes, it is necessary to generate multiple events from each input event. A function provided for this is:

```haskell
streamExpand::  Stream [alpha] ->          -- input  stream
                Stream alpha               -- output stream
```

An example is a function that takes in a stream of Twitter tweets, and generates a stream of the hashtags found in those tweets. Assume a function getHashtags with signature:

```haskell
getHashtags:: String -> [String]
```

If we apply this to a Stream of type String via `streamMap`, the result will be of type `Stream [String]`.
We could then expand that back to `Stream String` using `streamExpand`:

```haskell
streamExpand $ streamMap getHashtags s
```

### Windowing

Windowing provides a way to break a stream into a stream whose elements contain subset of events from the original stream. Other functions can then be used to filter the events in each window. Experience shows that it is useful in many stream applications, and as a result, most CEP systems support a range of types of windowing.
The basic windowing function is:

```haskell
streamWindow:: WindowMaker alpha ->            -- turns stream
                                               --  into stream of
                                               --  windows
               Stream alpha ->                 -- input stream
               Stream [alpha]                  -- output stream

type WindowMaker alpha = Stream alpha -> [Stream alpha]
```

WindowMaker is a function that generates a list of windows.

A pre-defined set of functions covering the common WindowMaker cases is provided. However, users are free to define their own windowing functions if they require an alternative, specialist window creator. Examples of pre-defined functions are:

```haskell
-- create sliding windows that are a fixed number of events in length
sliding:: Int -> WindowMaker alpha

-- create sliding windows that are a fixed time length
slidingTime:: NominalDiffTime -> WindowMaker alpha

-- create non-overlapping windows that are a fixed # of events in length
chop:: Int -> WindowMaker alpha

-- create non-overlapping windows that are a fixed time length
chopTime:: NominalDiffTime -> WindowMaker alpha
```

Building on this, we can then provide a function that combines windowing and aggregation:

```haskell
streamWindowAggregate:: WindowMaker alpha ->            -- turns stream
                                                        --  into stream of
                                                        --  windows
                        WindowAggregator alpha beta -> -- aggregates
                                                        --  window into
                                                        --  single value
                        Stream alpha ->                 -- input stream
                        Stream beta                     -- output stream

type WindowAggregator alpha beta = [alpha] -> beta
```

WindowAggregator takes a list of the values contained in a window of events, and aggregates them into a single value.
An implementation of streamWindowAggregate in Haskell is:

```haskell
streamWindowAggregate fwm fwa s = streamMap fwa $ streamWindow fwm s
```

As an example, consider an extension to the running example in which we in each want to know how may times in each hour the sensor has measured a temperature over 100. This can be done using the function:

```haskell
streamWindowAggregate (chopTime 3600) length
$ streamFilter over100 tempSensor
```

Here, length is the standard Haskell function that returns the length of a list.

### Merge

Sometimes there is a need to merge multiple streams of the same type into one. This is done with the streamMerge function.

```haskell
streamMerge:: [Stream alpha]-> Stream alpha
```

For example, if there are three temperature sensors then we can combine them as follows:

```haskell
streamMerge [tempSensor1,tempSensor2,tempSensor3]
```

In the running example, the merged stream can then be used as input to one of the previously described stream processing functions, e.g.:

```haskell
streamWindowAggregate (chopTime 3600) length
$ streamMerge over100
$ streamMerge [tempSensor1,tempSensor2,tempSensor3]
```
### Join

The join pattern is used to combine data from two streams that may be of different types.
A basic join is provided, along with two others that build on it.

The basic stream join has the signature

```haskell
streamJoin:: Stream alpha -> Stream beta -> Stream (alpha,beta)
```

This takes two streams as input and generates a stream where each event holds pair containing the payload of the events in each stream. The streams are combined in a pairwise manner.

Two higher level join functions are provided that operate on windows of events.

The first - streamJoinE – has the signature:

```haskell
type JoinFilter alpha beta        = alpha -> beta -> Bool
type JoinMap    alpha beta gamma  = alpha -> beta -> gamma

streamJoinE:: WindowMaker alpha ->         -- create windows from stream 1
              WindowMaker beta ->          -- create windows from stream 2
              JoinFilter alpha beta ->     -- determines if this pair of
                                           --   values meets join criteria
              JoinMap alpha beta gamma ->  -- combines the pair of values
                                           --  into the output event
              Stream alpha ->              -- 1st input stream
              Stream beta  ->              -- 2nd input stream
              Stream gamma                 -- the output stream
```

The user provides a WindowMaker function for each of the two input streams. The Cartesian product of the values in the events in each window is then formed. The resulting pairs of values are then filtered using the user-defined JoinFilter function to determine if the pair of values meets the join criteria. A user-defined map function – JoinMap – is then applied to each conforming pair and the result appears on the output stream.

This can be implemented using the core stream processing functions:
```haskell
streamJoinE fwm1 fwm2 fwj fwm s1 s2 = streamExpand $
                                      streamMap  (cartesianJoin fwj fwm) $
                                      streamJoin (streamWindow fwm1 s1)
                                                 (streamWindow fwm2 s2)
```

For example, if our running example temperature sensors are upgraded to ones which give a wider range of data, including location and carbon dioxide:

```haskell
data EnvSensor = EnvSensor {loc::Location,temp::Int,co2::Int}
```

And are joined by another set of sensors with traffic data:

```haskell
data Traffic   = Traffic {loc::Location,load::Int}
```

where load is a measure of the volume of traffic per second. Then, we may want to understand the correlation between load and CO2 levels every minute. As a first step we can combine data at all the locations where there are both types of sensor.
We can do this using the following function (where envSensors is the merge of the data from all the environmental sensors, and trafficSensors is the merge of the data from all the traffic sensors):

```haskell
streamJoinE (chopTime 60)
            (chopTime 60)
            (close 50)
            (\(loc1,temp1,co2level)(loc2,ld) -> (loc1,temp1,ld,c02level))
            envSensors
            trafficSensors
```

Here close is a function that is true if the locations given by its second and third parameters are within a number of meters specified by its first parameter. Its signature is:

```haskell
close:: Int -> Location -> Location -> Bool
```

We also provide another join function – streamJoinW - that does not perform a cartesian product of the events in the two windows that are being joined. Instead, it operates on whole windows, applying a user defined function to the two windows of data taken from the two input streams. It is defined as:

```haskell
streamJoinW:: WindowMaker alpha ->        -- create windows from stream 1
              WindowMaker beta ->         -- create windows from stream 2
              ([alpha]->[beta]->gamma) -> -- combines the two windows
                                          --  into the output event
              Stream alpha ->             -- 1st input stream
              Stream beta  ->             -- 2nd input stream
              Stream gamma                -- the output stream
```

This can be implemented using the core stream processing functions as:

```haskell
streamJoinW fwm1 fwm2 fwj s1 s2 = streamMap  (\(w1,w2)->fwj w1 w2) $
                                  streamJoin (streamWindow fwm1 s1)
                                             (streamWindow fwm2 s2)
```

## Dynamically creating Functional Stream Graphs

If the programmer composes the functional stream operations introduced above then the stream graph is statically defined. However, as Haskell supports higher-order functions we can introduce some new functions that can be used to create sub-graphs either statically or dynamically.

Computer science thrives on recursion. However, a stream processing system composed of the functions described in the previous section doe not directly support recursion. We can provide it by adding a new function:


```haskell
streamGraph  :: (Stream alpha->Stream beta)->   -- stream graph
                Stream alpha ->                 -- input stream
                Stream beta                     -- output stream
```

This can be implemented as:

```haskell
streamGraph g s = streamMerge $ map (\e->g [e]) s
```

This function applies the stream processing graph defined by the first parameter (g) to each event in the input stream, and merges the resulting streams. An example which maps and filters each window created from a stream is:

```haskell
streamGraph (\ss-> streamMap mf $ streamFilter ff ss)
            $ streamWindow (sliding 10) s
```

As streamGraph can operate on finite length streams (as well as infinite streams), it is useful to define another function that reduces a finite list:

```haskell
streamReduce::(beta -> alpha -> beta) ->  -- the accumulator fn:
                                           --  takes head of stream &
                                           --  accumulator and computes
                                           --  the new accumulator
              beta ->                      -- initial accumulator value
              Stream alpha ->              -- input stream
              Stream beta                  -- output stream
```
The output is a stream with one Event - the result of reducing the list using the first parameter.

This could be implemented as
```haskell
streamReduce f acc s = streamMap (foldl f acc) $ streamWindow complete s
```

where "complete" is a WindowMaker that takes all elements of a finite stream that contain a value.

An example application is adding up the contents of a finite stream of integers:
```haskell
sumEvents:: Stream Int -> Stream Int
sumEvents s = streamReduce (\acc a ->acc+a) 0 s
```
