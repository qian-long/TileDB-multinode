#!/bin/bash

echo 'Executing load for IREG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A IREG -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv

echo 'Executing load for REG...'
~/stavrospapadopoulos/TileDB/tiledb/bin/tiledb \
 -q load \
 -w ~/stavrospapadopoulos/TileDB/data/example/ \
 -A REG -f ~/stavrospapadopoulos/TileDB/data/test_A_0.csv
