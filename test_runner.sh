./setup_env.sh
mpiexec.mpich2 -n 3 -f machinefile_local ./multinode_launcher test_D.csv test_E.csv
#mpiexec.mpich2 -n 3 -f machinefile_local ./multinode_launcher ais_2009_01_allzones.csv ais_2009_09_allzones.csv
