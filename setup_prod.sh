#!/bin/bash
#!/bin/bash

mkdir -p /data/qlong/workspaces

for NODE in `seq 0 8`
do
  mkdir -p /data/qlong/workspaces/workspace-$NODE/data
  touch /data/qlong/workspaces/workspace-$NODE/logfile
  cat /dev/null > workspaces/workspace-$NODE/logfile
done;

rm -rf /data/qlong/workspaces/workspace-[0-9]/Loader \
  /data/qlong/workspaces/workspace-[0-9]/StorageManager \
  /data/qlong/workspaces/workspace-[0-9]/*.csv \
  /data/qlong/workspaces/workspace-[0-9]/Consolidator \
  /data/qlong/workspaces/workspace-[0-9]/Executor \
  /data/qlong/workspaces/workspace-[0-9]/QueryProcessor \
  /data/qlong/workspaces/workspace-[0-9]/MetaData \
  /data/qlong/workspaces/workspace-[0-9]/data/HASH_*.csv

