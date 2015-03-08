#include <mpi.h>
#include <string>
#include <sstream>
#include <cstring>
#include <ostream>
#include <iostream>
#include <istream>
#include <fstream>
#include <algorithm>
#include <cstdio>
#include "assert.h"
#include "coordinator_node.h"
#include "csv_file.h"
#include "debug.h"

CoordinatorNode::CoordinatorNode(int rank, int nprocs) {
  myrank_ = rank;
  nprocs_ = nprocs; 
  nworkers_ = nprocs - 1;

  // TODO put in config file
  my_workspace_ = "./workspaces/workspace-0";
  logger_ = new Logger(my_workspace_ + "/logfile");

  storage_manager_ = new StorageManager(my_workspace_);
  loader_ = new Loader(my_workspace_, *storage_manager_);
  mpi_handler_ = new MPIHandler();
}

// TODO
CoordinatorNode::~CoordinatorNode() {
  delete logger_;
}

Logger* CoordinatorNode::logger() {
  return logger_;
}

void CoordinatorNode::run() {
  logger_->log(LOG_INFO, "I am the master node");
  send_all("hello", DEF_TAG);

  // Set array name
  std::string array_name = "test_A";
  std::string filename = array_name + ".csv";

  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1");
  attribute_names.push_back("attr2");

  // Set attribute types
  std::vector<const std::type_info*> types;
  types.push_back(&typeid(int));
  types.push_back(&typeid(int));


  // Set dimension type
  types.push_back(&typeid(int));

  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i");
  dim_names.push_back("j");

  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 999));
  dim_domains.push_back(std::pair<double,double>(0, 999));

  // Create an array with irregular tiles
  ArraySchema array_schema = ArraySchema(array_name,
      attribute_names,
      dim_names,
      dim_domains,
      types,
      ArraySchema::HILBERT);

  DEBUG_MSG("sending parallel load instructions to all workers");
  ParallelLoadMsg pmsg = ParallelLoadMsg(filename, ParallelLoadMsg::NAIVE, array_schema);
  send_and_receive(pmsg);

  /*
  DEBUG_MSG("sending load instruction to all workers");
  ArraySchema::Order order = ArraySchema::COLUMN_MAJOR;
  LoadMsg lmsg = LoadMsg(array_name, array_schema);
  send_and_receive(lmsg);

  DEBUG_MSG("sending get test instruction to all workers");
  GetMsg gmsg = GetMsg("test_A");
  send_and_receive(gmsg);
  */


  /*
  DEBUG_MSG("sending filter instruction to all workers");
  int attr_index = 1;
  Op op = GT;
  int operand = 4;
  Predicate<int> pred(attr_index, op, operand);
  DEBUG_MSG(pred.to_string());
  FilterMsg<int> fmsg = FilterMsg<int>(array_schema.celltype(attr_index), array_schema, pred, "smallish_filter");

  send_and_receive(fmsg);

  DEBUG_MSG("sending subarray");
  std::vector<double> vec;
  vec.push_back(9); vec.push_back(11);
  vec.push_back(10); vec.push_back(13);

  SubarrayMsg sbmsg("subarray", array_schema, vec);
  send_and_receive(sbmsg);
  DEBUG_MSG("done sending subarray messages");

  DEBUG_MSG("sending get subarray instruction to all workers");
  GetMsg gmsg1 = GetMsg("subarray");
  send_and_receive(gmsg1);
  */

  /*
  DEBUG_MSG("sending aggregate instruction to all workers");
  AggregateMsg amsg = AggregateMsg(array_name, 1);
  send_and_receive(amsg);
  */


  quit_all();
}

void CoordinatorNode::send_all(Msg& msg) {
  DEBUG_MSG("send_all: " + msg.msg_tag);
  std::pair<char*, int> serial_pair = msg.serialize();
  this->send_all(serial_pair.first, serial_pair.second, msg.msg_tag);
}

void CoordinatorNode::send_all(std::string serial_str, int tag) {
  this->send_all(serial_str.c_str(), serial_str.length(), tag);
}
void CoordinatorNode::send_all(const char* buffer, int buffer_size, int tag) {
  assert(buffer_size < MPI_BUFFER_LENGTH);
  // TODO make asynchronous
  for (int i = 1; i < nprocs_; i++) {
    MPI_Send(buffer, buffer_size, MPI::CHAR, i, tag, MPI_COMM_WORLD);
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
    case PARALLEL_LOAD_TAG:
      handle_parallel_load((dynamic_cast<ParallelLoadMsg&>(msg)));
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

void CoordinatorNode::handle_ack() {

  for (int i = 0; i < nworkers_; i++) {
    MPI_Status status;
    int nodeid = i + 1;
    char *buf = new char[MPI_BUFFER_LENGTH];
    int length;

    MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, nodeid, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
    assert((status.MPI_TAG == DONE_TAG) || (status.MPI_TAG == ERROR_TAG));
    MPI_Get_count(&status, MPI_CHAR, &length);

    logger_->log(LOG_INFO, "Received ack " + std::string(buf, length) + " from worker: " + std::to_string(nodeid));

  }

}

void CoordinatorNode::handle_get() {
  std::stringstream ss;

  // TODO refactor to use mpi handler
  char *buf = new char[MPI_BUFFER_LENGTH];
  for (int i = 0; i < nprocs_ - 1; i++) {
    MPI_Status status;
    int nodeid = i + 1;
    int length;
    bool keep_receiving = true;

    int count = 0;
    do {
      MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, nodeid, GET_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_CHAR, &length);

      // check last byte
      keep_receiving = (bool) buf[length-1];

      // print all but last byte
      std::cout << std::string(buf, length - 1);
    } while (keep_receiving);

  }

  delete [] buf;
}

// TODO other types
void CoordinatorNode::handle_aggregate() {

  int aggregate_max = -10000000;
  int worker_max = -10000000;
  for (int i = 0; i < nworkers_; i++) {
    MPI_Status status;
    int nodeid = i + 1;
    char *buf = new char[MPI_BUFFER_LENGTH];
    int length;

    MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, nodeid, MPI_ANY_TAG, MPI_COMM_WORLD, &status);

    assert((status.MPI_TAG == AGGREGATE_TAG) || (status.MPI_TAG == ERROR_TAG));
    MPI_Get_count(&status, MPI_CHAR, &length);

    if (status.MPI_TAG == ERROR_TAG) { // Error
      logger_->log(LOG_INFO, "Received aggregate error from worker: " + std::to_string(nodeid));

    } else { // Success
      memcpy(&worker_max, buf, sizeof(int));
      logger_->log(LOG_INFO, "Received max from Worker " + std::to_string(nodeid) + ": " + std::to_string(worker_max));
      if (worker_max > aggregate_max) {
        aggregate_max = worker_max;
      }

      MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, nodeid, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      assert(status.MPI_TAG == DONE_TAG);
      MPI_Get_count(&status, MPI_CHAR, &length);

      logger_->log(LOG_INFO, "Received ack " + std::string(buf, length) + " from worker: " + std::to_string(nodeid));
    }

    delete[] buf;

  }

  std::stringstream ss;
  ss << "Max: " << aggregate_max;
  logger_->log(LOG_INFO, ss.str());
  std::cout << ss.str() << "\n";
}

// TODO make asynchronous?
void CoordinatorNode::handle_parallel_load(ParallelLoadMsg& pmsg) {
  logger_->log(LOG_INFO, "In handle_parallel_load");

  switch (pmsg.load_type()) {

    case ParallelLoadMsg::NAIVE:
      handle_parallel_load_naive(pmsg);
      break;
      
    case ParallelLoadMsg::HASH_PARTITION:
      break;
    case ParallelLoadMsg::SAMPLING:
      break;
    case ParallelLoadMsg::MERGE_SORT:
      break;
    default:
      // TODO return error?
      break;
  }
}

void CoordinatorNode::handle_parallel_load_naive(ParallelLoadMsg& pmsg) {
  std::stringstream ss;

  std::string filename = pmsg.filename();
  char *buf = new char[MPI_BUFFER_LENGTH];
  std::string tmp_filepath = my_workspace_ + "/" + filename + ".tmp";
  std::ofstream tmpfile;
  tmpfile.open(tmp_filepath);

  for (int nodeid = 1; nodeid < nprocs_; ++nodeid) {
    DEBUG_MSG("Waiting for content from worker " + std::to_string(nodeid));
    mpi_handler_->receive_file(tmpfile, nodeid, PARALLEL_LOAD_TAG);
  }

  tmpfile.flush();
  tmpfile.close();

  // sort
  std::string sorted_filepath = my_workspace_ + "/sorted_" + filename + ".tmp";

  logger_->log(LOG_INFO, "Sorting csv file tmp_filepath: " + tmp_filepath);
  loader_->sort_csv_file(tmp_filepath, sorted_filepath, pmsg.array_schema());
  logger_->log(LOG_INFO, "Finished sorting csv file");

  // TODO send partitions back to worker nodes
  logger_->log(LOG_INFO, "Counting num_lines");
  std::ifstream sorted_file;
  sorted_file.open(sorted_filepath);
  // using cpp count algo function
  int num_lines = std::count(std::istreambuf_iterator<char>(sorted_file), 
                             std::istreambuf_iterator<char>(), '\n');

  logger_->log(LOG_INFO, "num_lines: " + std::to_string(num_lines));

  logger_->log(LOG_INFO, "Splitting and sending sorted content to workers");
  int lines_per_worker = num_lines / (nprocs_ - 1);
  // if not evenly split
  int remainder = num_lines % (nprocs_ - 1);

  int pos = 0;
  int total = lines_per_worker;

  if (remainder > 0) {
    total++;
  }

  sorted_file.seekg(0, std::ios::beg);
  for (int nodeid = 1; nodeid < nprocs_; ++nodeid) {
    std::string line;
    std::stringstream content;
    int end = pos + lines_per_worker;
    if (remainder > 0) {
      end++;
    }

    logger_->log(LOG_INFO, "Sending sorted file part to nodeid" + std::to_string(nodeid));
    bool keep_receiving = true;
    // TODO refactor out?
    for(; pos < end; ++pos) {
      if (content.str().length() + line.length() >= MPI_BUFFER_LENGTH) {
        // encode "there is more coming" in the last byte
        //content.write((char *) &keep_receiving, sizeof(bool));

        // send content to nodeid
        MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, nodeid, PARALLEL_LOAD_TAG, MPI_COMM_WORLD);

        mpi_handler_->send_keep_receiving(true, nodeid);
        content.str(std::string()); // clear buffer

      }
      // TODO use stavros's csvfile?
      std::getline(sorted_file, line);
      std::cout << line;
      content << line << "\n";
    }

    // final send
    //keep_receiving = false;
    //content.write((char *) &keep_receiving, sizeof(bool));
    MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, nodeid, PARALLEL_LOAD_TAG, MPI_COMM_WORLD);
    mpi_handler_->send_keep_receiving(false, nodeid);
    --remainder;
  }


  sorted_file.close();
  // TODO Cleanup
  delete [] buf;
}

void CoordinatorNode::quit_all() {
  send_all("quit", QUIT_TAG);
}


/******************************************************
 *************** TESTING FUNCTIONS ********************
 ******************************************************/

void CoordinatorNode::test_load(std::string array_name) {
  logger_->log(LOG_INFO, "Start Load");
  logger_->log(LOG_INFO, "loading array " + array_name);
  ArraySchema * array_schema = get_test_arrayschema(array_name);
  ArraySchema::Order order = ArraySchema::ROW_MAJOR;
  LoadMsg lmsg = LoadMsg(array_name, *array_schema);

  send_and_receive(lmsg);

  logger_->log(LOG_INFO, "Test Load Done");

  // TODO don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_filter(std::string array_name) {
  logger_->log(LOG_INFO, "Start Filter");
  ArraySchema* array_schema = get_test_arrayschema(array_name);

  // .5 selectivity
  int attr_index = 1;
  Op op = GE;
  int operand = 500000;
  Predicate<int> pred(attr_index, op, operand);
  logger_->log(LOG_INFO, pred.to_string());
  FilterMsg<int> fmsg = FilterMsg<int>(array_schema->celltype(attr_index), *array_schema, pred, array_name+"_filtered");

  send_and_receive(fmsg);
  logger_->log(LOG_INFO, "Test Filter Done");

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_subarray(std::string array_name) {
  logger_->log(LOG_INFO, "Start SubArray");
  ArraySchema* array_schema = get_test_arrayschema(array_name);
  std::vector<double> vec;

  // .5 selectivity
  vec.push_back(0); vec.push_back(1000000);
  vec.push_back(0); vec.push_back(500000);

  SubarrayMsg sbmsg(array_name+"_subarray", *array_schema, vec);
  send_and_receive(sbmsg);
  logger_->log(LOG_INFO, "Test Subarray Done");

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_aggregate(std::string array_name) {
  logger_->log(LOG_INFO, "Start Aggregate test");

  int attr_index = 1;
  AggregateMsg amsg(array_name, 1);
  send_and_receive(amsg);
  logger_->log(LOG_INFO, "Test Aggregate Done");

  // don't leak memory
  //delete array_schema;
}

ArraySchema* CoordinatorNode::get_test_arrayschema(std::string array_name) {

  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("attr1");
  attribute_names.push_back("attr2");

  // Set attribute types
  std::vector<const std::type_info*> attribute_types;
  attribute_types.push_back(&typeid(int));
  attribute_types.push_back(&typeid(int));


  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("i");
  dim_names.push_back("j");

  // Set dimension type
  ArraySchema::CellType dim_type = ArraySchema::DOUBLE;

  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 1000000));
  dim_domains.push_back(std::pair<double,double>(0, 1000000));

  // Create an array with irregular tiles
  ArraySchema * array_schema = new ArraySchema(array_name,
    attribute_names,
    dim_names,
    dim_domains,
    attribute_types,
    ArraySchema::ROW_MAJOR);

  return array_schema;
}
