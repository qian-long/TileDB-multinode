#include <mpi.h>
#include <string>
#include <sstream>
#include <cstring>
#include "assert.h"
#include "coordinator_node.h"
#include "csv_file.h"

CoordinatorNode::CoordinatorNode(int rank, int nprocs) {
  myrank_ = rank;
  nprocs_ = nprocs; 
  nworkers_ = nprocs - 1;

  // TODO put in config file
  my_workspace_ = "./workspaces/workspace-0";
  logger_ = new Logger(my_workspace_ + "/logfile");

}

// TODO
CoordinatorNode::~CoordinatorNode() {
  delete logger_;
}

Logger* CoordinatorNode::logger() {
  return logger_;
}

void CoordinatorNode::run() {
  logger_->log("I am the master node");
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
  ArraySchema::DataType dim_type = ArraySchema::INT;

  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 999));
  dim_domains.push_back(std::pair<double,double>(0, 999));
  // Create an array with irregular tiles
  ArraySchema array_schema = ArraySchema(array_name,
    attribute_names,
    attribute_types,
    dim_domains,
    dim_names,
    dim_type);

  DEBUG_MSG("sending load instruction to all workers");

  Loader::Order order = Loader::COLUMN_MAJOR;
  LoadMsg lmsg = LoadMsg(array_name, &array_schema, order);
  send_and_receive(lmsg);

  DEBUG_MSG("sending filter instruction to all workers");
  int attr_index = 1;
  Op op = GT;
  int operand = 4;
  Predicate<int> pred(attr_index, op, operand);
  DEBUG_MSG(pred.to_string());
  FilterMsg<int> fmsg = FilterMsg<int>(array_schema.attribute_type(attr_index), array_schema, pred, "smallish_filter");

  send_and_receive(fmsg);

  DEBUG_MSG("sending get test_filter instruction to all workers");
  GetMsg gmsg = GetMsg("smallish_filter");
  send_and_receive(gmsg);

  /*
  DEBUG_MSG("sending subarray");
  std::vector<double> vec;
  vec.push_back(9); vec.push_back(11);
  vec.push_back(10); vec.push_back(13);

  SubArrayMsg sbmsg("subarray", &array_schema, vec);
  send_and_receive(sbmsg);
  DEBUG_MSG("done sending subarray messages");

  DEBUG_MSG("sending get subarray instruction to all workers");
  GetMsg gmsg1 = GetMsg("subarray");
  send_and_receive(gmsg1);
  */

  DEBUG_MSG("sending aggregate instruction to all workers");
  AggregateMsg amsg = AggregateMsg(array_name, 1);
  send_and_receive(amsg);


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
    case LOAD_TAG:
    case FILTER_TAG:
    case SUBARRAY_TAG:
      handle_ack();
      break;
    case AGGREGATE_TAG:
      handle_aggregate();
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

void CoordinatorNode::handle_ack() {

  for (int i = 0; i < nworkers_; i++) {
    MPI_Status status;
    int nodeid = i + 1;
    char *buf = new char[MAX_DATA];
    int length;

    MPI_Recv(buf, MAX_DATA, MPI_CHAR, nodeid, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    assert((status.MPI_TAG == DONE_TAG) || (status.MPI_TAG == ERROR_TAG));
    MPI_Get_count(&status, MPI_CHAR, &length);

    logger_->log("Received ack " + std::string(buf, length) + " from worker: " + std::to_string(nodeid));

  }

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

// TODO other types
void CoordinatorNode::handle_aggregate() {

  int aggregate_max = -10000000;
  int worker_max = -10000000;
  for (int i = 0; i < nworkers_; i++) {
    MPI_Status status;
    int nodeid = i + 1;
    char *buf = new char[MAX_DATA];
    int length;

    MPI_Recv(buf, MAX_DATA, MPI_CHAR, nodeid, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    assert((status.MPI_TAG == AGGREGATE_TAG) || (status.MPI_TAG == ERROR_TAG));
    MPI_Get_count(&status, MPI_CHAR, &length);

    if (status.MPI_TAG == ERROR_TAG) { // Error
      logger_->log("Received aggregate error from worker: " + std::to_string(nodeid));

    } else { // Success
      memcpy(&worker_max, buf, sizeof(int));
      logger_->log("Received max from Worker " + std::to_string(nodeid) + ": " + std::to_string(worker_max));
      if (worker_max > aggregate_max) {
        aggregate_max = worker_max;
      }

      MPI_Recv(buf, MAX_DATA, MPI_CHAR, nodeid, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      assert(status.MPI_TAG == DONE_TAG);
      MPI_Get_count(&status, MPI_CHAR, &length);

      logger_->log("Received ack " + std::string(buf, length) + " from worker: " + std::to_string(nodeid));
    }

  }

  std::stringstream ss;
  ss << "Max: " << aggregate_max;
  logger_->log(ss.str());
  std::cout << ss.str() << "\n";
}

void CoordinatorNode::quit_all() {
  send_all("quit", QUIT_TAG);
}

/******************************************************
 *************** TESTING FUNCTIONS ********************
 ******************************************************/

void CoordinatorNode::test_load(std::string array_name) {
  logger_->log("Start Load");
  logger_->log("loading array " + array_name);
  ArraySchema * array_schema = get_test_arrayschema(array_name);
  Loader::Order order = Loader::ROW_MAJOR;
  LoadMsg lmsg = LoadMsg(array_name, array_schema, order);

  send_and_receive(lmsg);

  logger_->log("Test Load Done");

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_filter(std::string array_name) {
  logger_->log("Start Filter");
  ArraySchema* array_schema = get_test_arrayschema(array_name);

  // .5 selectivity
  int attr_index = 1;
  Op op = GE;
  int operand = 500000;
  Predicate<int> pred(attr_index, op, operand);
  logger_->log(pred.to_string());
  FilterMsg<int> fmsg = FilterMsg<int>(array_schema->attribute_type(attr_index), *array_schema, pred, array_name+"_filtered");

  send_and_receive(fmsg);
  logger_->log("Test Filter Done");

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_subarray(std::string array_name) {
  logger_->log("Start SubArray");
  ArraySchema* array_schema = get_test_arrayschema(array_name);
  std::vector<double> vec;

  // .5 selectivity
  vec.push_back(0); vec.push_back(1000000);
  vec.push_back(0); vec.push_back(500000);

  SubArrayMsg sbmsg(array_name+"_subarray", array_schema, vec);
  send_and_receive(sbmsg);
  logger_->log("Test Subarray Done");

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_aggregate(std::string array_name) {
  logger_->log("Start Aggregate test");

  int attr_index = 1;
  AggregateMsg amsg(array_name, 1);
  send_and_receive(amsg);
  logger_->log("Test Aggregate Done");

  // don't leak memory
  //delete array_schema;
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
