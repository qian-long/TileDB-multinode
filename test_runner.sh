./setup_env.sh
./setup_prod.sh
mpiexec.mpich2 -n 13 -f machinefile_istc ./multinode_launcher test_D.csv test_E.csv
#mpiexec.mpich2 -n 13 -f machinefile_local ./multinode_launcher ais_2009_01_allzones.csv ais_2009_09_allzones.csv
