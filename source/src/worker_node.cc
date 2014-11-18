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
  // TODO put in config file
  workspace << "./workspaces/workspace-" << myrank_;
  this->my_workspace_ = workspace.str();
  this->storage_manager_ = new StorageManager(my_workspace_);
  this->loader_ = new Loader(my_workspace_, *storage_manager_);
  this->query_processor_ = new QueryProcessor(*storage_manager_);

  // catalogue data structures
  this->arrayname_map_ = new std::map<std::string, std::string>();
  this->global_schema_map_ = new std::map<std::string, ArraySchema *>();
  this->local_schema_map_ = new std::map<std::string, ArraySchema *>();
}

// TODO delete things inside maps?
WorkerNode::~WorkerNode() {
  delete arrayname_map_;
  delete global_schema_map_;
  delete local_schema_map_;
}


void WorkerNode::run() {
  DEBUG_MSG("I am a worker node");

  MPI_Status status;
  char *buf = new char[MAX_DATA];
  int length;
  int loop = true;
  int result;
  while (loop) {
      MPI_Recv(buf, MAX_DATA, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_CHAR, &length);
      switch (status.MPI_TAG) {
        case QUIT_TAG:
          loop = false;
          break;
        case GET_TAG:
          result = receive_get(std::string(buf, length));
          assert(result);
          break;
        case ARRAY_SCHEMA_TAG:
          result = receive_array_schema(std::string(buf, length));
          assert(result);
          break;
        case LOAD_TAG: // TODO
          result = receive_load(std::string(buf, length));
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

int WorkerNode::receive_get(std::string arrayname) {
  //std::stringstream ss;
  //ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  CSVFile file(get_arrayname(arrayname), CSVFile::READ, MAX_DATA);
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

int WorkerNode::receive_array_schema(std::string serial_str) {
  ArraySchema * array_schema = ArraySchema::deserialize(serial_str.c_str(), serial_str.size());

  // add schema to catalogue
  (*this->global_schema_map_)[array_schema->array_name()] = array_schema;

  // debug message
  DEBUG_MSG("received array schema: \n" + array_schema->to_string());
  return 1;
}

int WorkerNode::receive_load(std::string serial_str) {
  DEBUG_MSG("received load\n");

  Loader::LoadArgs args = Loader::deserialize_load_args(serial_str.c_str(), serial_str.size());

  std::string filepath = convert_filename(args.filename);
  Loader::Order order = args.order;
  ArraySchema * schema = args.array_schema;

  //DEBUG_MSG(filepath + "\n" + schema->to_string());
  this->loader_->load(filepath, *schema, order);

  DEBUG_MSG("Finished load");
  return 1;
}



/******************************************************
 ****************** HELPER FUNCTIONS ******************
 ******************************************************/
std::string WorkerNode::get_arrayname(std::string arrayname) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  return ss.str();
}

std::string WorkerNode::convert_filename(std::string filename) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << filename.c_str() << "_rnk" << myrank_ << ".csv";
  return ss.str();
}

