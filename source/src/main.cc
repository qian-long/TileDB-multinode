#include <stdio.h>
#include <mpi.h>
#include "debug.h"

#define MASTER 0

// This is the user
int main(int argc, char** argv) {
  int myrank, nprocs;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

  if (myrank == MASTER) {
    DEBUG_MSG("I am the master node");
  } else {
    DEBUG_MSG("I am a worker node");
  }
  MPI_Finalize();
  return 0;
}

