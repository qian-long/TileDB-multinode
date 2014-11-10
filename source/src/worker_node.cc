#include <mpi.h>
#include "worker_node.h"
#include "debug.h"
#include "csv_file.h"

WorkerNode::WorkerNode(int rank, int nprocs) {
  this->myrank_ = rank;
  this->nprocs_ = nprocs;
}

// TODO
WorkerNode::~WorkerNode() {}

void WorkerNode::run() {
  DEBUG_MSG("I am a worker node");
  int source = 0;
  MPI::Status status;
  MPI::COMM_WORLD.Probe(source, 1, status);
  int length = status.Get_count(MPI::CHAR);
  char *buf = new char[length];
  MPI::COMM_WORLD.Recv(buf, length, MPI::CHAR, source, 1, status);
  std::string content(buf, length);
  std::stringstream fn;
  fn << "./workspaces/partition_test_rnk" << myrank_ << ".csv";
  // TODO what is 25?
  CSVFile* file = new CSVFile(fn.str(), CSVFile::WRITE, 25);
  CSVLine line;
  line << content.c_str();
  *file << line; 
  DEBUG_MSG(content);
  delete [] buf;
  delete file;
}

