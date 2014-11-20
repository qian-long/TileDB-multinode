#include <stdio.h>
#include <mpi.h>
#include "debug.h"
#include "coordinator_node.h"
#include "worker_node.h"
#include "constants.h"


// This is the user
int main(int argc, char** argv) {
  int myrank, nprocs;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

  if (myrank == MASTER) {
    CoordinatorNode * coordinator = new CoordinatorNode(myrank, nprocs);
    coordinator->run();
  } else {
    WorkerNode * worker = new WorkerNode(myrank, nprocs);
    worker->run();
  }

  MPI_Finalize();
  return 0;
}

