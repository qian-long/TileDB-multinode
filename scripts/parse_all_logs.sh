#!/bin/bash

OUTPUT=results_4GB_breakdowns_raw.csv
cat /dev/null > $OUTPUT
ls -d ../Result/n*-t*-ais_2009_01_allzones* |
while read i;
do
  echo $i
  ./parse_logs.py $i >> $OUTPUT
done;

