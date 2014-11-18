#include <mpi.h>
#include <string>
#include "assert.h"
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
  send_all("hello", DEF_TAG);
  DEBUG_MSG("calling get");
  send_and_receive("partition_test", GET_TAG);

  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1");
  attribute_names.push_back("attr2");

  // Set attribute types
  std::vector<ArraySchema::DataType> attribute_types;
  attribute_types.push_back(ArraySchema::INT);
  attribute_types.push_back(ArraySchema::FLOAT);

  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i");
  dim_names.push_back("j");

  // Set dimension type
  ArraySchema::DataType dim_type = ArraySchema::INT64_T;

  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 40));
  dim_domains.push_back(std::pair<double,double>(0, 40));
  // Create an array with irregular tiles
  ArraySchema * array_schema = new ArraySchema("A",
    attribute_names,
    attribute_types,
    dim_domains,
    dim_names,
    dim_type);

  DEBUG_MSG("sending load instruction to all workers");

  std::string filename = "load_irreg_test";
  Loader::Order order = Loader::ROW_MAJOR;
  send_load(filename, *array_schema, order);
  quit_all();
}


void CoordinatorNode::send_all(std::string content, int tag) {
  assert( content.length() < MAX_DATA);
  // TODO make asynchronous
  for (int i = 1; i < nprocs_; i++) {
    MPI_Send(content.c_str(), content.length(), MPI::CHAR, i, tag, MPI_COMM_WORLD);
  }
}

void CoordinatorNode::send_and_receive(std::string msg, int tag) {
  send_all(msg, tag);
  switch(tag) {
    case GET_TAG:
      handle_get();
      break;
    case INIT_TAG:
      break;
    case ARRAY_SCHEMA_TAG:
      break;
    case LOAD_TAG:
      handle_load();
      break; 
    default:
      // don't do anything
      break;
  }

}

void CoordinatorNode::send_array_schema(ArraySchema & array_schema) {
  send_all(array_schema.serialize(), ARRAY_SCHEMA_TAG);
}

void CoordinatorNode::send_load(std::string filename, ArraySchema& schema, Loader::Order order) {
  send_all(Loader::serialize_load_args(filename, schema, order), LOAD_TAG);
  /*
  std::string serial = Loader::serialize_load_args(filename, schema, order);
  Loader::LoadArgs args = Loader::deserialize_load_args(serial.c_str(), serial.size());
  DEBUG_MSG(args.filename);
  DEBUG_MSG(args.order);
  DEBUG_MSG(args.array_schema->to_string());
  */
}
void CoordinatorNode::handle_load() {
  // TODO print ok message to user
}

// TODO make asynchronous
void CoordinatorNode::handle_get() {
  std::stringstream ss;
  for (int i = 0; i < nprocs_ - 1; i++) {
    MPI_Status status;
    int nodeid = i + 1;
    char *buf = new char[MAX_DATA];
    int length;
    MPI_Recv(buf, MAX_DATA, MPI_CHAR, nodeid, GET_TAG, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &length);
    ss << std::string(buf, length);
  }

  std::cout << ss.str();
}

void CoordinatorNode::quit_all() {
  send_all("quit", 0);
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

