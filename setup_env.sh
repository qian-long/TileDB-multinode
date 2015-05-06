#!/bin/bash

mkdir -p workspaces
for i in `seq 0 12`;
do

mkdir -p workspaces/workspace-$i/data

rm -rf workspaces/workspace-$i/Loader \
  workspaces/workspace-$i/StorageManager \
  workspaces/workspace-$i/*.csv \
  workspaces/workspace-$i/Consolidator \
  workspaces/workspace-$i/Executor \
  workspaces/workspace-$i/QueryProcessor \
  workspaces/workspace-$i/MetaData \
  workspaces/workspace-$i/data/HASH_*.csv

touch workspaces/workspace-$i/logfile

cat /dev/null > workspaces/workspace-$i/logfile
done;

