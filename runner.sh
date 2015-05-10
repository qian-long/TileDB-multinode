#!/bin/bash
declare -a NODES=("istc2" "istc3" "istc4" "istc5" "istc6" "istc1" "istc8" "istc9" "istc10" "istc11" "istc12" "istc13")
NUM_NODES=$1
DATASET1=ais_2010_allzones
#DATASET1=test_D
FILENAME1=$DATASET1.csv
DATASET2=ais_2011_allzones
#DATASET2=test_E
FILENAME2=$DATASET2.csv
TEMP_NAME=$( date +"%m%dT%H:%M")
TRIAL=$2
RUN_NAME=n$NUM_NODES-t$TRIAL-$DATASET1-$DATASET2-$TEMP_NAME
DATA_FOLDER=Result/$RUN_NAME
mkdir -p $DATA_FOLDER

#clear the machines for running
for i in `seq 0 $(($NUM_NODES-2))`;
do
  NODE="${NODES[$i]}"
  echo setting up node $NODE
  ssh -i ~/.ssh/qlong $NODE -t "cd ~/TileDB-multinode; make multi-istc; ./setup_prod.sh; /usr/local/bin/drop_caches"
done;

#1 = datatsize, 2 = number of nodes including master, 3 = trial number
#ex: ./runner 500 8

# run the actual thing
make multi-istc
./setup_prod.sh
mpiexec.mpich2 -n $NUM_NODES -f machinefile_istc ./multinode_launcher $FILENAME1 $FILENAME2 > $DATA_FOLDER/master.txt
cat $DATA_FOLDER/master.txt > /dev/null


#get the data from each machine
if [ "$?" -eq "0" ]
then
  #get data from master
  cp /data/qlong/workspaces/workspace-0/logfile $DATA_FOLDER/master_logfile.txt
  for i in `seq 0 $(($NUM_NODES-2))`;
  do
    WORKER=i
    WS_PATH=/data/qlong/workspaces/workspace-$(($WORKER+1))
    NODE="${NODES[$i]}"

    echo grabbing files from $WS_PATH on node $NODE

    ssh -i ~/.ssh/qlong $NODE -t "du -ab $WS_PATH > $WS_PATH/sizes.txt"
    scp -i ~/.ssh/qlong $NODE:$WS_PATH/logfile $DATA_FOLDER/w$WORKER\_$NODE\_log.txt
    scp -i ~/.ssh/qlong $NODE:$WS_PATH/sizes.txt $DATA_FOLDER/w$WORKER\_$NODE\_sizes.txt
  done;
else
  echo "Test script failed"
fi


