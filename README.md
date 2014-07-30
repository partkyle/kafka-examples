kafka-go-examples
=================

Kafka Go examples

Installation
============

Install like a normal go package. There are no 3rd party dependencies.

```
go get github.com/partkyle/kafka-examples/consumer
go get github.com/partkyle/kafka-examples/producer
```

This should install the packages into your `$GOPATH/bin` directory.

Usage
=====

Consumer:
```
[partkyle:~]$ consumer -h
Usage of consumer:
  -kafkas=[:2181]: kafka addresses to connect to
  -offset=0: offset to start reading from
  -partitions=[]: partitions to read from
  -show-offsets=false: show offset and exit
  -topic="": topic to read
```

Producer:
```
[partkyle:~]$ producer -h
Usage of producer:
  -kafka=":2181": kafka addr
  -key="": key to post message with
  -message="": message to post
  -topic="default": topic to post
```
