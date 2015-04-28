#! /bin/bash
NUM_NODES=3
DATASET1=test_D
FILENAME1=$DATASET1.csv
DATASET2=test_E
FILENAME2=$DATASET2.csv
TEMP_NAME=$( date +"%m-%dT%H:%M")
TRIAL=${3:-$TEMP_NAME}
RUN_NAME=t$NUM_NODES\_$DATASET1\_$DATASET2\_$TRIAL
DATA_FOLDER=Result/$RUN_NAME
mkdir -p $DATA_FOLDER

#clear the machines for running
for i in `seq 2 $NUM_NODES`;
do
  ssh -i ~/.ssh/qlong istc$i -t "cd ~/TileDB-multinode; make multi-istc; ./setup_env.sh"
done;

#1 = datatsize, 2 = number of nodes including master, 3 = trial number
#ex: ./runner 500 8

# run the actual thing
make multi-istc
./setup_env.sh
mpiexec.mpich2 -n $NUM_NODES -f machinefile_istc ./multinode_launcher $FILENAME1 $FILENAME2 > $DATA_FOLDER/master.txt
cat $DATA_FOLDER/master.txt


#get the data from each machine
if [ "$?" -eq "0" ]
then
  #get data from master
  cp ~/TileDB-multinode/workspaces/workspace-0/logfile $DATA_FOLDER/master_logfile.txt
  for i in `seq 2 $NUM_NODES`;
  do
    scp istc$i:~/TileDB-multinode/workspaces/workspace-$(($i-1))/logfile $DATA_FOLDER/istc_machine_$i.txt
  done;
else
  echo "Test script failed"
fi


