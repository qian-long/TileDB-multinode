./setup_env.sh
mpiexec.mpich2 -n 5 -f machinefile_local ./multinode_launcher ais_2009_01_allzones.csv 
