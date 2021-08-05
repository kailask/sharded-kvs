#!/bin/bash
NODES=$(docker ps -a --format "{{.Names}}" --filter "ancestor=sharded-kvs:1.0")

for i in $NODES; do
  docker kill $i > /dev/null
  docker rm $i > /dev/null
done

docker network rm sharded-kvs-subnet > /dev/null
