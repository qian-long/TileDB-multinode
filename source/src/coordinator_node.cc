#include <mpi.h>
#include <string>
#include "assert.h"
#include <sstream>
#include "coordinator_node.h"
#include "debug.h"
#include "csv_file.h"
#include "messages.h"

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
  GetMsg gmsg("partition_test");
  send_and_receive(gmsg);

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
  ArraySchema array_schema = ArraySchema("A",
    attribute_names,
    attribute_types,
    dim_domains,
    dim_names,
    dim_type);

  DEBUG_MSG("sending load instruction to all workers");

  std::string filename = "load_irreg_test";
  Loader::Order order = Loader::ROW_MAJOR;
  LoadMsg lmsg = LoadMsg(filename, array_schema, order);
  send_all(lmsg);
  quit_all();
}

void CoordinatorNode::send_all(Msg& msg) {
  this->send_all(msg.serialize(), msg.msg_tag);
}

void CoordinatorNode::send_all(std::string content, int tag) {
  assert( content.length() < MAX_DATA);
  // TODO make asynchronous
  for (int i = 1; i < nprocs_; i++) {
    MPI_Send(content.c_str(), content.length(), MPI::CHAR, i, tag, MPI_COMM_WORLD);
  }
}

void CoordinatorNode::send_and_receive(Msg& msg) {
  send_all(msg);
  switch(msg.msg_tag) {
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
  //TODO fix
  assert (false);
  send_all(array_schema.serialize(), ARRAY_SCHEMA_TAG);
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
  send_all("quit", QUIT_TAG);
}

