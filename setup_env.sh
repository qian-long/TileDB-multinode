#!/bin/bash

mkdir -p workspaces
rm -rf workspaces/workspace-[0-9]/Loader workspaces/workspace-[0-9]/StorageManager
sync
sudo cp hosts /etc/hosts
