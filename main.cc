#include <stdio.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <cstdlib>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
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


void run_test_suite(CoordinatorNode * coordinator, std::string array_name_base, std::string filename) {
  struct timeval tim;  
  gettimeofday(&tim, NULL);  
  double t1 = tim.tv_sec+(tim.tv_usec/1000000.0);  
  char buffer[100];
  int len;
  std::string array_name;
  int num_samples = 1000;
  double tstart;
  double tend;

  /*
  // PARALLEL HASH LOAD TEST
  array_name = array_name_base + "_phash";
  coordinator->test_parallel_load(array_name, filename, HASH_PARTITION);

  gettimeofday(&tim, NULL);  
  double t2 = tim.tv_sec+(tim.tv_usec/1000000.0);  

  len = snprintf(buffer, 100, "Hash Partition Load %s wall time: %.6lf secs\n", array_name.c_str(), t2 - t1);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);
  */

  // PARALLEL ORDERED LOAD TEST
  array_name = array_name_base + "_pordered";
  gettimeofday(&tim, NULL);  
  tstart = tim.tv_sec+(tim.tv_usec/1000000.0);  
  coordinator->test_parallel_load(array_name, filename, ORDERED_PARTITION, num_samples);

  gettimeofday(&tim, NULL);  
  tend = tim.tv_sec+(tim.tv_usec/1000000.0);  

  len = snprintf(buffer, 100, "Ordered Partition Load %s wall time: %.6lf secs\n", array_name.c_str(), tend - tstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);

  /*
  // HASH LOAD TEST
  array_name = array_name_base + "_hash";
  coordinator->test_load(array_name, filename, HASH_PARTITION);

  gettimeofday(&tim, NULL);  
  double t4 = tim.tv_sec+(tim.tv_usec/1000000.0);  

  len = snprintf(buffer, 100, "Hash Partition Load %s wall time: %.6lf secs\n", array_name.c_str(), t4 - t3);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);


  // ORDERED LOAD TEST
  array_name = array_name_base + "_ordered";
  coordinator->test_load(array_name, filename, ORDERED_PARTITION, LoadMsg::SORT);

  gettimeofday(&tim, NULL);  
  double t5 = tim.tv_sec+(tim.tv_usec/1000000.0);  

  len = snprintf(buffer, 100, "Ordered Partition Load %s wall time: %.6lf secs\n", array_name.c_str(), t5 - t4);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);
  */


  /*
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
  */

}

// This is the user
int main(int argc, char** argv) {
  int myrank, nprocs;

  MPI_Init(&argc, &argv);
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs);
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank);

#ifdef DEBUG
  // For debugging with gdb
  pid_t pid = getpid();
  pid_t ppid = getppid();
  std::stringstream ssd;
  ssd << "Pid: " << pid << " Parent pid: " << ppid << "\n";
  std::cout << ssd.str();
  //int DebugWait = 1;
  //while (DebugWait);

#endif

  // seed srand
  srand(0);

  std::string datadir = "./data";
  if (myrank == MASTER) {

#ifdef ISTC
    datadir = "/data/qlong/ais_final";
#endif
    CoordinatorNode * coordinator = new CoordinatorNode(myrank, nprocs, datadir);

    std::string filename;
    std::string array_name;

    std::cout << "argc: " << argc << "\n";
    if (argc <= 1) {
      filename = "test_C.csv";
    } else {
      filename = argv[1];
    }

    /*
    else if (strncmp(argv[1], "500",3) == 0) {
        filename = std::string(get_filename(1, nprocs));
    }else if (strncmp(argv[1], "1",1) == 0) {
        filename = std::string(get_filename(2, nprocs));
    }else if (strncmp(argv[1], "2",1) == 0) {
        filename = std::string(get_filename(3, nprocs));
    }else {
        DEBUG_MSG("not a valid total data size");
    }
    */

#ifdef ISTC
    std::stringstream ss(filename);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, '.')) {
      elems.push_back(item);
    }
    array_name = elems[0];
    run_test_suite(coordinator, array_name, filename);
    coordinator->quit_all();
#else
    std::cout << "Running debug\n";
    coordinator->run();

    std::cout << "Finished running debug\n";
#endif

  } else {

#ifdef ISTC
    datadir = "/data/qlong/processed_ais_data";
#endif
    WorkerNode * worker = new WorkerNode(myrank, nprocs, datadir);
    worker->run();
  }

  MPI_Finalize();
  return 0;
}


