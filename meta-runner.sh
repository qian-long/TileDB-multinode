#run all the 8 node tests

for trial in 1;
do
  for num_nodes in 13 11 9 7 5;
  do
    ./runner.sh $num_nodes $trial
  done;
done;
