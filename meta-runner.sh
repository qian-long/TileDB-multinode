#run all the 8 node tests

for num in 8 4 2 1;
do
  for dataset in 500 1 2;
  do
    for trial in 1 2 3;
    do 
      ./runner $dataset $num $trial
    done;
  done;
done;
