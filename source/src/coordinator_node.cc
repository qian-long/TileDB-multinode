#include <mpi.h>
#include <string>
#include <sstream>
#include "coordinator_node.h"
#include "debug.h"
#include "csv_file.h"

CoordinatorNode::CoordinatorNode(int rank, int nprocs) {
  this->myrank_ = rank;
  this->nprocs_ = nprocs; 
}

// TODO
CoordinatorNode::~CoordinatorNode() {}

void CoordinatorNode::run() {
  DEBUG_MSG("I am the master node");
  partition_initial_file();
}

// TODO: make it better
// round robin style
// does everything in memory
void CoordinatorNode::partition_initial_file() {
  // TODO what is 25?
  CSVFile file("Data/partition_test.csv", CSVFile::READ, 25);
  CSVLine line;
  std::stringstream ssa[nprocs_];
  int counter = 0;
  try {
    while(file >> line) {
      ssa[counter] << line.str() << "\n";
      counter = (counter + 1) % (nprocs_ - 1); // number of workers
    }
  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
  }

  for (int i = 0; i < nprocs_ - 1; i++) {
    int workerid = i + 1;
    std::string content = ssa[i].str();
    MPI::COMM_WORLD.Send(content.c_str(), content.length(), MPI::CHAR, workerid, 1);
  }
}

