# ------------------------------
# Run Docker containers

# create 3 nodes with 2 in initial view
source ./create.sh 3 2
docker start node1 node2 node3 > /dev/null
           
sleep 5

python3 test2-helper.py

source ./stop.sh