#! /bin/bash

TEMP_NAME=$( date +"%d-%H_%M")
TRIAL=${3:-$TEMP_NAME}
RUN_NAME=$2_$1_$TRIAL
DATA_FOLDER=Result/$RUN_NAME
mkdir -p $DATA_FOLDER

#clear the machines for running
for i in `seq 1 $2`;
do
  ssh qian-test$i -t "cd TileDB-multinode; make mpi-prod"
done;

#1 = datatsize, 2 = number of nodes including master, 3 = trial number
#ex: ./runner 500 8

#run the actual thing
#cp host thing
sudo cp hosts /etc/hosts
make mpi-prod 
mpiexec -n $(($2+1)) -f machinefile_prod ./mpi_main $1 > $DATA_FOLDER/master.txt
cat $DATA_FOLDER/master.txt


#get the data from each machine
if [ "$?" -eq "0" ]
then
  #get data from master
  cp ~/TileDB-multinode/workspaces/workspace-0/logfile $DATA_FOLDER/master_logfile.txt
  for i in `seq 1 $2`;
  do
    scp qian-test$i:~/TileDB-multinode/workspaces/workspace-$i/logfile $DATA_FOLDER/machine_$i.txt
  done;
else
  echo "Test script failed"
fi


