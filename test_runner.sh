./setup_env.sh
./setup_prod.sh
mpiexec.mpich2 -n 13 -f machinefile_local ./multinode_launcher test_D.csv test_E.csv
#mpiexec.mpich2 -n 13 -f machinefile_local ./multinode_launcher ais_2009_01_to_06_allzones.csv ais_2009_07_to_12_allzones.csv
