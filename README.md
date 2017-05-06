

## SimpleDynamo - Replicated Key-Value Storage

### Introduction

This is about implementing a simplified version of Dynamo. There are three main pieces implemented:
- Partitioning
- Replication
- Failure handling

The main goal is to provide both availability and linearizability at the same time. In other words, it always perform read and write operations successfully even under failures. At the same time, a read operation return the most recent value.

Partitioning and replication is done exactly the way Dynamo does.

### References

Here are two references for the Dynamo design:
- [Lecture slides](http://www.cse.buffalo.edu/~stevko/courses/cse486/spring17/lectures/26-dynamo.pdf)
- [Dynamo paper](http://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
