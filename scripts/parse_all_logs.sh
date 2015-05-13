#!/bin/bash

OUTPUT=results_50GB_breakdowns_raw.csv
cat /dev/null > $OUTPUT
ls -d ../Result/n*-t*-ais_2010* |
while read i;
do
  echo $i
  ./parse_logs.py $i >> $OUTPUT
done;

