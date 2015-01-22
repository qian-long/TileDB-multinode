#!/bin/bash

mkdir -p workspaces
mkdir -p workspaces/workspace-0
mkdir -p workspaces/workspace-1
mkdir -p workspaces/workspace-2
mkdir -p workspaces/workspace-3
mkdir -p workspaces/workspace-4
mkdir -p workspaces/workspace-5
mkdir -p workspaces/workspace-6
mkdir -p workspaces/workspace-7
mkdir -p workspaces/workspace-8
rm -rf workspaces/workspace-[0-9]/Loader workspaces/workspace-[0-9]/StorageManager workspaces/workspace-[0-9]/*.tmp
touch workspaces/workspace-0/logfile
touch workspaces/workspace-1/logfile
touch workspaces/workspace-2/logfile
touch workspaces/workspace-3/logfile
touch workspaces/workspace-4/logfile
touch workspaces/workspace-5/logfile
touch workspaces/workspace-6/logfile
touch workspaces/workspace-7/logfile
touch workspaces/workspace-8/logfile
cat /dev/null > workspaces/workspace-0/logfile
cat /dev/null > workspaces/workspace-1/logfile
cat /dev/null > workspaces/workspace-2/logfile
cat /dev/null > workspaces/workspace-3/logfile
cat /dev/null > workspaces/workspace-4/logfile
cat /dev/null > workspaces/workspace-5/logfile
cat /dev/null > workspaces/workspace-6/logfile
cat /dev/null > workspaces/workspace-7/logfile
cat /dev/null > workspaces/workspace-8/logfile
