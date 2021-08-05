# Sharded KVS

A simple in-memory key-value store capable of sharding data across multiple storage nodes to improve scability and performance.

## Overview

## Setup

### Dependencies

* Go 1.13
* Docker

### Running

Storage nodes for sharded-kvs can be run easily inside Docker containers. Each node must have its own IP address set as an environment variable. It must also have a list of all IP addresses for nodes in the initial `view`. They must be in the same order for every node.

```
$ docker run -d -p 13801:13800 --net=sharded-kvs-subnet --ip=10.10.0.4 --name="node1" 
  -e ADDRESS="10.10.1.0:13800" -e VIEW="10.10.1.0:13800,10.10.2.0:13800" sharded-kvs:1.0
```

A [script](test/create.sh) is provided to create docker containers with this format.

### Testing

See [test/test1.sh](test/test1.sh) for an example of how to start and query the kvs.

```
cd sharded-kvs/test;
chmod +x *.sh;
./test1.sh
```

## Design

### Issues

## Acknowledgements

 The project was completed as part of coursework for [CSE 138: Distributed Systems](https://courses.soe.ucsc.edu/courses/cse138/).