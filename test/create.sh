#!/bin/bash

if (($# != 2)); then
    echo "Usage: ./create.sh [number of nodes] [number of nodes in initial view]"
    exit 1
fi

docker network create --subnet=10.10.0.0/16 sharded-kvs-subnet
docker build -t sharded-kvs:1.0 ..

num_nodes=$1
nodes_in_view=$2
view=""

for ((i = 1 ; i <= $num_nodes ; i++)); do
    view+="10.10.$i.0:13800"
    if (( $i < $nodes_in_view )); then
        view+=","
    fi
done

for ((i = 1 ; i <= $num_nodes ; i++)); do
    docker create --name="node$i" \
        --net=sharded-kvs-subnet \
        --ip=10.10.$i.0 \
        -p 1380$i:13800 \
        -e ADDRESS="10.10.$i.0:13800" \
        -e VIEW="${view}" \
        sharded-kvs:1.0 \
        > /dev/null
done

