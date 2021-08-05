#!/bin/bash

# ------------------------------
# Run Docker containers

# convenience variables
addr1="10.10.1.0:13800"
addr2="10.10.2.0:13800"
addr3="10.10.3.0:13800"
full_view="${addr1},${addr2},${addr3}"

# create 3 nodes with 2 in initial view
source ./create.sh 3 2
docker start node1 node2 node3 > /dev/null

# ------------------------------
# add a key

curl --request   PUT                                 \
     --header    "Content-Type: application/json"    \
     --data      '{"value": "sampleValue"}'          \
     --write-out "%{http_code}\n"                    \
     http://localhost:13802/kvs/keys/sampleKey

# get a key
curl --request GET                                   \
     --header "Content-Type: application/json"       \
     --write-out "%{http_code}\n"                    \
     http://localhost:13801/kvs/keys/sampleKey

# ------------------------------
# Now we start a new node and add it to the existing store

curl --request PUT                                   \
     --header "Content-Type: application/json"       \
     --data "{\"view\":\"${full_view}\"}"            \
     --write-out "%{http_code}\n"                    \
     http://localhost:13802/kvs/view-change

# get same key from new node

curl --request GET                                   \
     --header "Content-Type: application/json"       \
     --write-out "%{http_code}\n"                    \
     http://localhost:13803/kvs/keys/sampleKey

source ./stop.sh
