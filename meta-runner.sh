#run all the 8 node tests

for num_nodes in 9 5 3;
do
  for trial in 1 2;
  do
    ./runner.sh $num_nodes $trial
  done;
done;
