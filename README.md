# Trending Topic Extraction with Spark (PoC)

This is a hobby project I created in order to learn Scala and the Spark programming API in under 24 programming hours. Note that the implementation currently still contains some inefficiencies.

The Spark MapReduce job calculates the top N trending topics in a Twitter dump over a configurable period of time X.

We define a [Trending Topic](https://en.wikipedia.org/wiki/Twitter#Trending_topics) as a word for which the [slope](https://en.wikipedia.org/wiki/Slope) of frequency of occurence in a certain period is largest. 

# Design

## Pipeline Component Decomposition
![Design](doc/flow.png?raw=true)

## Spark Stage Decomposition (current)

The current setup uses 3 shuffles over the network.

| **Stage** | **Description**                | **Map Side A => B**                                                  | **Reduce Side (C , C) => C**                                                       |
|-----------|--------------------------------|----------------------------------------------------------------------|------------------------------------------------------------------------------------|
| 1         | Preprocess in / Frequency Calc | line => ((period, topic), 1)                                         | (x, y) => frequency                                                                |
| 2         | Prepare Slope Calc             | ((period, topic), frequency) => (topic, List[(period, frequency)]    | (List[(period,frequency)], List[(period, frequency)]) => List[(period, frequency)] |
| 3         | Slope Calc / Top N prep        | (topic, List[(period, frequency)]) => (period, List[(slope, topic)]) | (List[(slope, topic)], List[(slope, topic)]) => List[(slope,topic)]                |
| 4         | Top N prep / Save out          | (period, (slope, topic)) => (period, List[topic])                    | N/A                                                                                |


## Spark Stage Decomposition (improvement, TODO)

An improvement of the current setup uses just 2 instead of 3 shuffles over the network.
It takes advantage of the fact that there are much less periods per topic, than there are topics per period. This enables sequential map operations to still be scalable.

| **Stage** | **Description**                | **Map Side A => B**                                                  | **Reduce Side (C , C) => C**                                        |
|-----------|--------------------------------|----------------------------------------------------------------------|---------------------------------------------------------------------|
| 1         | Preprocess in / Frequency Calc | line => (topic, <singleton> List[(period, 1)])                       | (List[(period,x)] ,  List[(period,y)]) => List[(period, frequency)] |
| 2         | Slope Calc / Top N             | (topic, List[(period, frequency)]) => (period, List[(topic, slope)]) | (List[(topic,slope)], List[(topic, slope)]) => List[(topic, slope)] |
| 3         | Save out                       | (period, List[(topic, slope)] => (period, List[topic])               | N/A                                                                 |

# Getting Started

Simply build using Maven :)

# Contributing

You are welcome to contribute to this hobby project.
Suggested future features:

- [ ] Add stemming and/or stopword removal in TweetExtractor.
- [ ] Create a streaming version using Spark Streaming.
- [ ] Reduce the number of stages (currently 4) to 3 by clever rewriting.
- [ ] Add distributed top N calculation for memory efficiency.
- [ ] Add unit tests for TopFilter.
- [ ] Add unit tests for SlopeCalculator.

