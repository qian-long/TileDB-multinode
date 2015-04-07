#ifndef CONSTANTS_H
#define CONSTANTS_H
#include <cstdlib>

#define MASTER 0 // coordinator node id
#define MPI_BUFFER_LENGTH 1000 // num bytes, per worker buffer

// How data is partitioned across nodes
enum PartitionType {
  ORDERED_PARTITION,
  HASH_PARTITION
};

#endif
