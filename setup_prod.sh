#!/bin/bash
#!/bin/bash

mkdir -p /data/qlong/workspaces

for NODE in `seq 0 12`
do
  mkdir -p /data/qlong/workspaces/workspace-$NODE/data
  touch /data/qlong/workspaces/workspace-$NODE/logfile
  cat /dev/null > /data/qlong/workspaces/workspace-$NODE/logfile

  rm -rf /data/qlong/workspaces/workspace-$NODE/Loader \
  /data/qlong/workspaces/workspace-$NODE/StorageManager \
  /data/qlong/workspaces/workspace-$NODE/*.csv \
  /data/qlong/workspaces/workspace-$NODE/Consolidator \
  /data/qlong/workspaces/workspace-$NODE/Executor \
  /data/qlong/workspaces/workspace-$NODE/QueryProcessor \
  /data/qlong/workspaces/workspace-$NODE/MetaData \
  /data/qlong/workspaces/workspace-$NODE/data/HASH_*.csv

done;


