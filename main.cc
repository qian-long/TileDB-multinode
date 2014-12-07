#include <stdio.h>
#include <mpi.h>
#include "debug.h"
#include "coordinator_node.h"
#include "worker_node.h"
#include "constants.h"
#include <stdio.h>
#include <string.h>
#include <sys/time.h>


const char* get_filename(int dataset_num, int numprocs) {
  numprocs--;
  switch(dataset_num) {
    case 1:
      switch(numprocs) {
        case 1:
          return "500MB_500MB.csv";
        case 2:
          return "250MB_500MB.csv";
        case 4:
          return "125MB_500MB.csv";
        case 8:
          return "62.5MB_500MB.csv";
      }
    case 2:
      switch(numprocs) {
        case 1:
          return "500MB_1GB.csv";
        case 2:
          return "250MB_1GB.csv";
        case 4:
          return "125MB_1GB.csv";
        case 8:
          return "62.5MB_1GB.csv";
      }
    case 3:
      switch(numprocs) {
        case 1:
          return "500MB_2GB.csv";
        case 2:
          return "250MB_2GB.csv";
        case 4:
          return "125MB_2GB.csv";
        case 8:
          return "62.5MB_2GB.csv";
      }

  }
}


// This is the user
int main(int argc, char** argv) {
  int myrank, nprocs;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

  if (myrank == MASTER) {
    CoordinatorNode * coordinator = new CoordinatorNode(myrank, nprocs);
    //coordinator->run();
    struct timeval tim;  
    gettimeofday(&tim, NULL);  
    double t1=tim.tv_sec+(tim.tv_usec/1000000.0);  
    if (strncmp(argv[1], "test",4) == 0) {
        coordinator->run();
    }else if (strncmp(argv[1], "500",3) == 0) {
        std::string filename(get_filename(1, nprocs));
        coordinator->test_load(filename);
    }else if (strncmp(argv[1], "1",1) == 0) {
        std::string filename(get_filename(2, nprocs));
        coordinator->test_load(filename);
    }else if (strncmp(argv[1], "2",1) == 0) {
        std::string filename(get_filename(3, nprocs));
        coordinator->test_load(filename);
    }else {
        DEBUG_MSG("not a valid total data size");
    }
    gettimeofday(&tim, NULL);  
    double t2=tim.tv_sec+(tim.tv_usec/1000000.0);  
    printf("%.6lf seconds elapsed running dataset %s\n", t2-t1, argv[1]);  

    coordinator->quit_all();
  } else {
    WorkerNode * worker = new WorkerNode(myrank, nprocs);
    worker->run();
  }

  MPI_Finalize();
  return 0;
}


