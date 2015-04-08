./setup_env.sh
mpiexec.mpich2 -n 3 -f machinefile_local ./multinode_launcher test_C.csv
