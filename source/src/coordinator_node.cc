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

  // Set array name
  std::string array_name = "smallish";
  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1");
  attribute_names.push_back("attr2");

  // Set attribute types
  std::vector<ArraySchema::DataType> attribute_types;
  attribute_types.push_back(ArraySchema::INT);
  attribute_types.push_back(ArraySchema::INT);

  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i");
  dim_names.push_back("j");

  // Set dimension type
  ArraySchema::DataType dim_type = ArraySchema::DOUBLE;

  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 1000));
  dim_domains.push_back(std::pair<double,double>(0, 1000));
  // Create an array with irregular tiles
  ArraySchema array_schema = ArraySchema(array_name,
    attribute_names,
    attribute_types,
    dim_domains,
    dim_names,
    dim_type);

  DEBUG_MSG("sending load instruction to all workers");

  std::string filename = "test";
  Loader::Order order = Loader::ROW_MAJOR;
  LoadMsg lmsg = LoadMsg(filename, &array_schema, order);
  send_all(lmsg);


  DEBUG_MSG("sending filter instruction to all workers");
  int attr_index = 1;
  Op op = GT;
  int operand = 4;
  Predicate<int> pred(attr_index, op, operand);
  DEBUG_MSG(pred.to_string());
  FilterMsg<int> fmsg = FilterMsg<int>(array_schema.attribute_type(attr_index), array_schema, pred, "smallish_filter");

  send_all(fmsg);

  DEBUG_MSG("sending get test_filter instruction to all workers");
  GetMsg gmsg = GetMsg("smallish_filter");
  send_and_receive(gmsg);

  DEBUG_MSG("sending subarray");
  std::vector<double> vec;
  //one to five
  vec.push_back(9); vec.push_back(11);
  //30 to 40
  vec.push_back(10); vec.push_back(3);


  SubArrayMsg sbmsg("subarray", &array_schema, vec);
  send_all(sbmsg);
  DEBUG_MSG("done sending subarray messages");

  DEBUG_MSG("sending get subarray instruction to all workers");
  GetMsg gmsg1 = GetMsg("subarray");
  send_and_receive(gmsg1);


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

/*
void CoordinatorNode::send_array_schema(ArraySchema & array_schema) {
  //TODO fix
  send_all(array_schema.serialize(), ARRAY_SCHEMA_TAG);
}
*/

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
    bool keep_receiving = true;

    int count = 0;
    do {
      MPI_Recv(buf, MAX_DATA, MPI_CHAR, nodeid, GET_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_CHAR, &length);

      // check last byte
      keep_receiving = (bool) buf[length-1];

      // print all but last byte
      std::cout << std::string(buf, length - 1);
    } while (keep_receiving);

  }
}

void CoordinatorNode::quit_all() {
  send_all("quit", QUIT_TAG);
}

/******************************************************
 *************** TESTING FUNCTIONS ********************
 ******************************************************/

void CoordinatorNode::test_load(std::string array_name) {
  DEBUG_MSG("Start Load");
  DEBUG_MSG("loading array " + array_name);
  ArraySchema * array_schema = get_test_arrayschema(array_name);
  Loader::Order order = Loader::ROW_MAJOR;
  LoadMsg lmsg = LoadMsg(array_name, array_schema, order);

  send_all(lmsg);

  DEBUG_MSG("Test Load Done");

  // don't leak memory
  delete array_schema;
}

void CoordinatorNode::test_filter(std::string array_name) {
  DEBUG_MSG("Start Filter");
  ArraySchema* array_schema = get_test_arrayschema(array_name);

  int attr_index = 1;
  Op op = GT;
  int operand = 4;
  Predicate<int> pred(attr_index, op, operand);
  DEBUG_MSG(pred.to_string());
  FilterMsg<int> fmsg = FilterMsg<int>(array_schema->attribute_type(attr_index), *array_schema, pred, array_name+"_filtered");

  send_all(fmsg);
  DEBUG_MSG("Test Filter Done");

  // don't leak memory
  delete array_schema;
}

void CoordinatorNode::test_subarray(std::string array_name) {
  DEBUG_MSG("Start SubArray");
  ArraySchema* array_schema = get_test_arrayschema(array_name);
  std::vector<double> vec;
  //one to five
  vec.push_back(9); vec.push_back(11);
  //30 to 40
  vec.push_back(10); vec.push_back(3);

  SubArrayMsg sbmsg(array_name+"_subarray", array_schema, vec);
  send_all(sbmsg);
  DEBUG_MSG("Test Subarray Done");

  // don't leak memory
  delete array_schema;
}
ArraySchema* CoordinatorNode::get_test_arrayschema(std::string array_name) {

  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1");
  attribute_names.push_back("attr2");

  // Set attribute types
  std::vector<ArraySchema::DataType> attribute_types;
  attribute_types.push_back(ArraySchema::INT);
  attribute_types.push_back(ArraySchema::INT);

  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i");
  dim_names.push_back("j");

  // Set dimension type
  ArraySchema::DataType dim_type = ArraySchema::DOUBLE;

  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 1000000));
  dim_domains.push_back(std::pair<double,double>(0, 1000000));

  // Create an array with irregular tiles
  ArraySchema * array_schema = new ArraySchema(array_name,
    attribute_names,
    attribute_types,
    dim_domains,
    dim_names,
    dim_type);

  return array_schema;
}
