# flink-quickstart
An example project to get started in Flink
---

[Apache Flink](https://flink.apache.org/) is an open source framework for stream-oriented distributed processing written in Java and Scala.

## 1. Introduction to Flink DSL
We refer to the [official documentation](https://flink.apache.org/) for version 1.7.x used in this example.
For you it is enough to know the following aspects:
* **windowing** - allows discretizing the stream to collect multiple entries, Flink supports tumbling (non overlapping) windows, sliding windows (overlapping and used to smooth the overall aggregation), session windows (where each window start and end differs for each key), global windows (each key has a different window and its applied a custom function);
* **time** - ingestion time (time of input arrival), processing time (time of actual consumption in Flink) and event time (time as specified explicitly in the message or by a watermark);
* **watermarks** - used with event time to periodically measure the time progress and for instance to determine whether to close a window. This is generally emitted either by the source stream or has to be emitted explicitly at processing time right before any window operation is performed.
* **trigger** - determines when the window is ready to be processed, i.e. it is an event listener, which for instance reacts to the addition of elements and conclusion of windows;
* **evictor** - performs a pre-selection of the window elements after the trigger fires and before the computation takes place;

## 2. Example use case
This repo provides a quickstart in Flink. The use case is the processing of a log stream resulting from users downloading content from a website.
The goal is to count the number of downloads per user on a certain time span, in order to spot potentially fraudolent behavior.
Let's indeed assume a data scientist in our team has already previously explored a dataset collected in the same way, has computed the typical number of assets downloaded by users, and calculated a cumulative distribution function on that.
At that point, he gave us, data engineers the decision boundary for which we users can be considered behaving fraudolently.

The Flink topology will therefore:
* open an input stream from Kafka to retrieve event logs
* process only entries for logged users (i.e. having an `userid != -1`)
* count the number of assets downloaded by each user in a time window
* output the result to a file on a distributed file system, distinguishing those users falling below and above the decision boundary in different folders (respectively `active` and `regular`) 

