#include <stdio.h>
#include <mpi.h>

#define MASTER 0

int main(int argc, char** argv) {
  int myrank, nprocs;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

  if (myrank == MASTER) {
    printf("Hello from processor %d of %d. I am the master node\n", myrank, nprocs);
  } else {
    printf("Hello from processor %d of %d. I am a worker node\n", myrank, nprocs);
  }
  MPI_Finalize();
  return 0;
}

