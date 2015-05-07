#run all the 8 node tests

for num_nodes in 13;
do
  for trial in 1;
  do
    ./runner.sh $num_nodes $trial
  done;
done;
