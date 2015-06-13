# Trending Topic Extraction with Spark (PoC)

This is a hobby project I created in order to learn Scala and the Spark programming API in under 24 programming hours. Note that the implementation currently still contains some inefficiencies.

The Spark MapReduce job calculates the top N trending topics in a Twitter dump over a configurable period of time X.

We define a [Trending Topic](https://en.wikipedia.org/wiki/Twitter#Trending_topics) as a word for which the [slope](https://en.wikipedia.org/wiki/Slope) of frequency of occurence in a certain period is largest. 

# Design

![Design](https://raw.githubusercontent.com/stefanvanwouw/spark-based-trending/master/doc/flow.png)

# Getting Started

Simply build using Maven :)

# Contributing

You are welcome to contribute to this hobby project.
Suggested future features:

- [ ] Add stemming and/or stopword removal in TweetExtractor.
- [ ] Create a streaming version using Spark Streaming.
- [ ] Reduce the number of stages (currently 3) to 2 by clever rewriting.
- [ ] Add unit tests for TopFilter.
- [ ] Add unit tests for SlopeCalculator.

