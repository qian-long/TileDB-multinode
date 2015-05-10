#include <stdio.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <cstdlib>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <cmath>
#include "debug.h"
#include "coordinator_node.h"
#include "worker_node.h"
#include "constants.h"
#include "logger.h"

// helpers
void drop_caches() {
  std::string command = "/usr/local/bin/drop_caches";
  if (system(command.c_str()) == 0) {
    std::cout << "Drop caches successful\n";
  } else {
    std::cout << "Drop caches unsuccessful\n";
  }
}

double get_wall_time(){
  struct timeval time;
  if (gettimeofday(&time,NULL)) {
    //Handle error
    return 0;
  }
  return (double)time.tv_sec + (double)time.tv_usec * .000001;
}

double get_cpu_time(){
  return (double)clock() / CLOCKS_PER_SEC;
}

int get_num_samples(double confidence, double error, double p) {
  // round up
  return (1.0 / (2*error * error)) * log(2*p/(1.0 - confidence)) + 0.5;
}

void run_load_test_suite(int num_workers, CoordinatorNode *coordinator,
    std::string array_name_base, std::string filename) {
  char buffer[1000];
  int len;
  std::string array_name;
  double tstart = 0;
  double tend = 0;
  double ctstart = 0;
  double ctend = 0;

  double confidence = .99;
  double error = .02;
  int num_samples = get_num_samples(confidence, error, num_workers - 1);
  std::cout << "num_samples: " << num_samples << "\n";


  // test centralized loads and baseline, will be super slow...
  // HASH LOAD TEST
  drop_caches();
  array_name = array_name_base + "_hash";
  std::cout << "Load Hash Test " << array_name << "\n";

  tstart = get_wall_time();
  ctstart = get_cpu_time();

  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [load hash]");
  coordinator->test_load(array_name, filename, HASH_PARTITION);

  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [load hash] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);



  // ORDERED LOAD TEST
  std::cout << "Load Ordered Test " << array_name << "\n";
  drop_caches();

  tstart = get_wall_time();
  ctstart = get_cpu_time();

  array_name = array_name_base + "_ordered";

  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [load ordered]");
  coordinator->test_load(array_name, filename, ORDERED_PARTITION, LoadMsg::SAMPLE, num_samples);
  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [load ordered] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);

}



void run_test_suite(int num_workers, CoordinatorNode * coordinator, std::string array_name_base, std::string filename, std::string array_name_base2 = "", std::string filename2 = "") {

  std::string array_name2;
  char buffer[1000];
  int len;
  std::string array_name;
  double tstart = 0;
  double tend = 0;
  double ctstart = 0;
  double ctend = 0;

  double confidence = .99;
  double error = .02;
  int num_samples = get_num_samples(confidence, error, num_workers - 1) / num_workers;
  std::cout << "num_workers: " << num_workers << " num_samples_per_worker: " << num_samples << "\n";

  // PARALLEL ORDERED LOAD TEST
  drop_caches();
  array_name = array_name_base + "_pordered";
  std::cout << "Parallel Load Ordered Test " << array_name << "\n";
  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [pload ordered]");

  tstart = get_wall_time();
  ctstart = get_cpu_time();


  coordinator->test_parallel_load(array_name, filename, ORDERED_PARTITION, num_samples);

  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [pload ordered] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);

  // ORDERED JOIN TEST
  std::cout << "ORDERED JOIN TEST\n";
  if (array_name_base2.empty() && filename2.empty()) {
    std::cout << "MIssing 2nd array name\n";
  } else {

    // order load array 2
    array_name2 = array_name_base2 + "_pordered";
    std::cout << "Parallel Loading array 2 " << array_name2 << "\n";
    drop_caches();
    coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [pload ordered]");

    tstart = get_wall_time();
    ctstart = get_cpu_time();

    coordinator->test_parallel_load(array_name2, filename2, ORDERED_PARTITION, num_samples);

    ctend = get_cpu_time();
    tend = get_wall_time();

    len = snprintf(buffer, 1000, "\n[END TEST] [pload ordered] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name2.c_str(), tend - tstart, ctend - ctstart);
    coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
    printf("%s", buffer);


    std::cout << "Start test join for ordered partition\n";
    array_name = array_name_base + "_pordered";
    drop_caches();

    coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [join ordered]");
    tstart = get_wall_time();
    ctstart = get_cpu_time();

    coordinator->test_join(array_name, array_name2, "join_" + array_name + "_" + array_name2);

    ctend = get_cpu_time();
    tend = get_wall_time();


    len = snprintf(buffer, 1000, "\n[END TEST] [join ordered] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
    coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
    printf("%s", buffer);

  }

  // SUBARRAY ORDERED SPARSE TEST
  drop_caches();
  array_name = array_name_base + "_pordered";
  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [subarrays ordered]");

  tstart = get_wall_time();
  ctstart = get_cpu_time();

  coordinator->test_subarray_sparse(array_name);

  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [subarrays ordered] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);


  // SUBARRAY ORDERED DENSE TEST
  drop_caches();
  array_name = array_name_base + "_pordered";
  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [subarrayd ordered]");

  tstart = get_wall_time();
  ctstart = get_cpu_time();

  coordinator->test_subarray_dense(array_name);

  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [subarrayd ordered] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);


  // PARALLEL HASH LOAD TEST
  array_name = array_name_base + "_phash";
  std::cout << "Parallel Load Hash Test " << array_name << "\n";

  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [pload hash]");
  tstart = get_wall_time();
  ctstart = get_cpu_time();

  coordinator->test_parallel_load(array_name, filename, HASH_PARTITION);

  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [pload hash] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);

  // HASH JOIN TEST
  std::cout << "HASH JOIN TEST\n";
  if (array_name_base2.empty() && filename2.empty()) {
    std::cout << "Missing 2nd array name\n";
  } else {
    // hash load array 2
    drop_caches();
    array_name2 = array_name_base2 + "_phash";
    std::cout << "Loading array 2 " << array_name2 << "\n";

    coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [pload hash]");

    tstart = get_wall_time();
    ctstart = get_cpu_time();

    coordinator->test_parallel_load(array_name2, filename2, HASH_PARTITION);

    ctend = get_cpu_time();
    tend = get_wall_time();

    len = snprintf(buffer, 1000, "\n[END TEST] [pload hash] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name2.c_str(), tend - tstart, ctend - ctstart);

    coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
    printf("%s", buffer);


    std::cout << "Start test join\n";
    drop_caches();
    array_name = array_name_base + "_phash";
    coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [join hash]");


    tstart = get_wall_time();
    ctstart = get_cpu_time();

    coordinator->test_join(array_name, array_name2, "join_" + array_name + "_" + array_name2);

    ctend = get_cpu_time();
    tend = get_wall_time();


    len = snprintf(buffer, 1000, "\n[END TEST] [join hash] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
    coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
    printf("%s", buffer);
  }

  // SUBARRAY HASH SPARSE TEST
  drop_caches();
  array_name = array_name_base + "_phash";
  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [subarrays hash]");

  tstart = get_wall_time();
  ctstart = get_cpu_time();

  coordinator->test_subarray_sparse(array_name);

  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [subarrays hash] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);

  // SUBARRAY HASH DENSE TEST
  drop_caches();
  array_name = array_name_base + "_phash";
  coordinator->logger()->log(LOG_INFO, "\n[RUN TEST] [subarrayd hash]");

  tstart = get_wall_time();
  ctstart = get_cpu_time();

  coordinator->test_subarray_dense(array_name);

  ctend = get_cpu_time();
  tend = get_wall_time();

  len = snprintf(buffer, 1000, "\n[END TEST] [subarrayd hash] %s total wall time: [%.6lf] secs, total cpu time: [%.6lf]\n", array_name.c_str(), tend - tstart, ctend - ctstart);
  coordinator->logger()->log(LOG_INFO, std::string(buffer, len));
  printf("%s", buffer);



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

  */

}

std::string get_array_name(std::string filename) {
  std::stringstream ss(filename);
  std::string item;
  std::vector<std::string> elems;
  while (std::getline(ss, item, '.')) {
    elems.push_back(item);
  }
  return elems[0];
}

// This is the user
int main(int argc, char** argv) {
  /*
  double c = .99;
  double e = .05;
  for (int i = 2; i <= 12; ++i) {
  std::cout << "confidence = " << c << " error = " << e << " p = " << i << " num_samples: " << get_num_samples(c, e, i) << "\n";
  }
  */

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

#endif

  // seed srand
  // 0 -> succeeds_local_B
  // 2 -> precedes_local_B
  // 4 -> precedes_local_A
  // 6 -> succeeds_local_A
  //srand(2);
  srand(time(NULL));

  std::string datadir = "./data";
  std::string workspace_base = "./workspaces";

#ifdef ISTC
  workspace_base = "/data/qlong/workspaces";
#endif

  if (myrank == MASTER) {

#ifdef ISTC
    datadir = "/data/qlong/ais_final";
#endif

    CoordinatorNode * coordinator = new CoordinatorNode(myrank, nprocs, datadir, workspace_base);

    std::string filename;
    std::string array_name;
    std::string filename2 = "";
    std::string array_name2 = "";

    std::cout << "argc: " << argc << "\n";
    if (argc <= 1) {
      filename = "test_C.csv";
    } else if (argc <= 2) {
      filename = argv[1];
    } else if (argc <= 3) {

      filename = argv[1];
      filename2 = argv[2];
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
    array_name = get_array_name(filename);

    if (filename2.size() > 0) {
      array_name2 = get_array_name(filename2);
    }

    std::cout << "array_name: " << array_name << " array_name2: " << array_name2 << "\n";
    run_test_suite(nprocs - 1, coordinator, array_name, filename, array_name2, filename2);
    coordinator->quit_all();
#else
    //std::cout << "Running debug\n";
    //coordinator->run();
    array_name = get_array_name(filename);
    run_load_test_suite(nprocs - 1, coordinator, array_name, filename);
    coordinator->quit_all();

    //std::cout << "Finished running debug\n";
#endif

  } else {

#ifdef ISTC
    datadir = "/data/qlong/processed_ais_data";
#endif
    WorkerNode * worker = new WorkerNode(myrank, nprocs, datadir, workspace_base);
    worker->run();
  }

  MPI_Finalize();
  return 0;
}


