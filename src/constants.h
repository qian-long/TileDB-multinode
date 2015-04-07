#ifndef CONSTANTS_H
#define CONSTANTS_H
#include <cstdlib>

#define MASTER 0 // coordinator node id
#define MPI_BUFFER_LENGTH 100000 // max num bytes in a single mpi_send or receive
#define MH_TOTAL_BUF_SIZE 10000000 // total amount of memory for mpi handler

// How data is partitioned across nodes
enum PartitionType {
  ORDERED_PARTITION,
  HASH_PARTITION
};

#endif
