TileDB
======

The TileDB array database system


## How to run multinode TileDB

For development:
```
make multi-debug
make multi-run-local
```

For prod:
```
make multi
make multi-run
```

## Production setup
0. Set up instances (openstack, ec2, etc), ssh keys, copy IP address and keys
   into config and hosts, clone repo to all instances
1. Copy config to ~/.ssh/config
2. Workaround for csail puppet (if needed): 
```
sudo cp hosts /etc/hosts
```

3. run one of the bash scripts 

## Dependencies
```
sudo apt-get install mpich2
```

## Repo Structure

## Resources
* https://help.ubuntu.com/community/MpichCluster
* http://mpitutorial.com/mpi-hello-world/


