#include "constants.h"
#include "assert.h"
#include <cstring>
#include "mpi.h"
#include "worker_node.h"
#include "debug.h"
#include "csv_file.h"


WorkerNode::WorkerNode(int rank, int nprocs) {
  this->myrank_ = rank;
  this->nprocs_ = nprocs;
  std::stringstream workspace;
  workspace << "./workspaces/workspace-" << myrank_;
  this->my_workspace_ = workspace.str();
  this->storage_manager_ = new StorageManager(my_workspace_);
  this->loader_ = new Loader(my_workspace_, *storage_manager_);
  this->query_processor_ = new QueryProcessor(*storage_manager_);

}

// TODO
WorkerNode::~WorkerNode() {}


void WorkerNode::run() {
  DEBUG_MSG("I am a worker node");

  MPI_Status status;
  char *buf = new char[MAX_DATA];
  int length;
  bool again = true;
  while (again) {
      MPI_Recv(buf, MAX_DATA, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_CHAR, &length);
      switch (status.MPI_TAG) {
        case QUIT_TAG:
          again = 0;
          break;
        case GET_TAG:
          assert(get(std::string(buf, length)));
          break;
        default:
          std::string content(buf, length);
          DEBUG_MSG(content);
      }
  }
/*
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
*/
}

int WorkerNode::get(std::string arrayname) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  CSVFile file(ss.str(), CSVFile::READ, MAX_DATA);
  CSVLine line;

  // TODO make it better, right now everything is in one string
  std::stringstream content;
  try {
    while(file >> line) {
      content << line.str() << "\n";
    }
  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
  }

  MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, GET_TAG, MPI_COMM_WORLD);
  return 1;
}
void subarray() {

}
