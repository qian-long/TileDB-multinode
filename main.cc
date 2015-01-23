#include <stdio.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <cstdlib>
#include "debug.h"
#include "coordinator_node.h"
#include "worker_node.h"
#include "constants.h"
#include "logger.h"


const char* get_filename(int dataset_num, int numprocs) {
  numprocs--;
  switch(dataset_num) {
    case 1:
      switch(numprocs) {
        case 1:
          return "500MB_500MB";
        case 2:
          return "250MB_500MB";
        case 4:
          return "125MB_500MB";
        case 8:
          return "62.5MB_500MB";
        default:
          DEBUG_MSG("not a valid number of machines"); 
          std::exit(-1);
      }
    case 2:
      switch(numprocs) {
        case 1:
          return "1GB_1GB";
        case 2:
          return "500MB_1GB";
        case 4:
          return "250MB_1GB";
        case 8:
          return "125MB_1GB";
        default:
          DEBUG_MSG("not a valid number of machines"); 
          std::exit(-1);
      }
    case 3:
      switch(numprocs) {
        case 1:
          return "2GB_2GB";
        case 2:
          return "1GB_2GB";
        case 4:
          return "500MB_2GB";
        case 8:
          return "250MB_2GB";
        default:
          DEBUG_MSG("not a valid number of machines"); 
          std::exit(-1);
      }
    default:
      DEBUG_MSG("not a valid data set number"); 
      std::exit(-1);
  }
}


void run_test_suite(CoordinatorNode * coordinator, std::string array_name) {
  struct timeval tim;  
  gettimeofday(&tim, NULL);  
  double t1 = tim.tv_sec+(tim.tv_usec/1000000.0);  
  char buffer[100];
  int len;

  // LOAD TEST
  coordinator->test_load(array_name);

  gettimeofday(&tim, NULL);  
  double t2 = tim.tv_sec+(tim.tv_usec/1000000.0);  

  len = snprintf(buffer, 100, "Load %s wall time: %.6lf secs\n", array_name.c_str(), t2 - t1);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);

  // FILTER TEST
  coordinator->test_filter(array_name);
  gettimeofday(&tim, NULL);  
  double t3 = tim.tv_sec+(tim.tv_usec/1000000.0);  

  len = snprintf(buffer, 100, "Filter %s wall time: %.6lf secs\n", array_name.c_str(), t3 - t2);
  coordinator->logger()->log(LOG_INFO,std::string(buffer, len));
  printf("%s", buffer);

  // AGGREGATE TEST
  coordinator->test_aggregate(array_name);

  gettimeofday(&tim, NULL);  
  double t4 = tim.tv_sec+(tim.tv_usec/1000000.0);  

  len = snprintf(buffer, 100, "Aggregate %s wall time: %.6lf secs\n", array_name.c_str(), t4 - t3);
  coordinator->logger()->log(LOG_INFO,std::string(buffer, len));
  printf("%s", buffer);


  len = snprintf(buffer, 100, "%.6lf seconds elapsed running dataset %s\n", t4-t1, array_name.c_str());  
  coordinator->logger()->log(LOG_INFO,std::string(buffer, len));
  printf("%s", buffer);

  // SUBARRAY TEST
  //coordinator->test_subarray(array_name);

}

// This is the user
int main(int argc, char** argv) {
  int myrank, nprocs;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

  if (myrank == MASTER) {
    CoordinatorNode * coordinator = new CoordinatorNode(myrank, nprocs);

    std::string filename;

    if (argc <= 1 || strncmp(argv[1], "test",4) == 0) {
        filename = "test_A.csv";
    }else if (strncmp(argv[1], "500",3) == 0) {
        filename = std::string(get_filename(1, nprocs));
    }else if (strncmp(argv[1], "1",1) == 0) {
        filename = std::string(get_filename(2, nprocs));
    }else if (strncmp(argv[1], "2",1) == 0) {
        filename = std::string(get_filename(3, nprocs));
    }else {
        DEBUG_MSG("not a valid total data size");
    }

    //run_test_suite(coordinator, filename);
    
    coordinator->run();
    coordinator->quit_all();
  } else {
    WorkerNode * worker = new WorkerNode(myrank, nprocs);
    worker->run();
  }

  MPI_Finalize();
  return 0;
}


