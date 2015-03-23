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
#include <functional>
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
  executor_ = new Executor(my_workspace_);

  std::vector<int> workers;
  for (int i = 1; i < nprocs; ++i) {
    workers.push_back(i);
  }

  mpi_handler_ = new MPIHandler(0, workers);
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
  std::string array_name = "test_C";
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
  dim_domains.push_back(std::pair<double,double>(0, 1000000));
  dim_domains.push_back(std::pair<double,double>(0, 1000000));

  // Create an array with irregular tiles
  ArraySchema array_schema = ArraySchema(array_name,
      attribute_names,
      dim_names,
      dim_domains,
      types,
      ArraySchema::HILBERT);

  DEBUG_MSG("Sending DEFINE ARRAY to all workers for array test_A");
  DefineArrayMsg damsg = DefineArrayMsg(array_schema);
  send_and_receive(damsg);

  /*
  DEBUG_MSG("Sending parallel hash partition load instructions to all workers");
  ParallelLoadMsg pmsg2 = ParallelLoadMsg(filename, ParallelLoadMsg::HASH_PARTITION, array_schema);
  send_and_receive(pmsg2);
  */

  DEBUG_MSG("Sending parallel ordered partition load instructions to all workers");
  ParallelLoadMsg pmsg2 = ParallelLoadMsg(filename, ParallelLoadMsg::ORDERED_PARTITION, array_schema);
  send_and_receive(pmsg2);

  DEBUG_MSG("sending subarray");
  std::vector<double> vec;
  vec.push_back(0); vec.push_back(500000);
  vec.push_back(0); vec.push_back(500000);

  SubarrayMsg sbmsg("subarray", array_schema, vec);
  send_and_receive(sbmsg);
  DEBUG_MSG("done sending subarray messages");


  array_name = "subarray";
  DEBUG_MSG("Sending GET " + array_name + " to all workers");
  GetMsg gmsg1(array_name);
  send_and_receive(gmsg1);


  /*
  std::string array_name2 = "test_A";
  DEBUG_MSG("Sending DEFINE ARRAY to all workers for array test_load_hash");
  ArraySchema array_schema2 = array_schema.clone(array_name2);
  DefineArrayMsg damsg2 = DefineArrayMsg(array_schema2);
  send_and_receive(damsg2);



  DEBUG_MSG("Sending HASH_PARTITION load instructions to all workers");
  LoadMsg lmsg = LoadMsg(filename, array_schema2, LoadMsg::HASH);
  send_and_receive(lmsg);

  DEBUG_MSG("Sending GET " + array_name2 + " to all workers");
  GetMsg gmsg2 = GetMsg(array_name2);
  send_and_receive(gmsg2);
  */


  /*
  DEBUG_MSG("Sending ORDERED_PARTITION load instructions to all workers");
  LoadMsg lmsg = LoadMsg(filename, array_schema, LoadMsg::ORDERED);
  send_and_receive(lmsg);


  DEBUG_MSG("Sending GET test_A to all workers");
  GetMsg gmsg1 = GetMsg(array_name);
  send_and_receive(gmsg1);
  */



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
  logger_->log(LOG_INFO, "send_all");
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

// dispatch to correct handler
void CoordinatorNode::send_and_receive(Msg& msg) {
  send_all(msg);
  switch(msg.msg_tag) {
    case GET_TAG:
      handle_get(dynamic_cast<GetMsg&>(msg));
      break;
    case LOAD_TAG:
      handle_load(dynamic_cast<LoadMsg&>(msg));
    case PARALLEL_LOAD_TAG:
      handle_parallel_load(dynamic_cast<ParallelLoadMsg&>(msg));
    case DEFINE_ARRAY_TAG:
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

void CoordinatorNode::handle_load(LoadMsg& lmsg) {
  switch (lmsg.load_type()) {
    case LoadMsg::ORDERED:
      handle_load_ordered(lmsg);
      break;
    case LoadMsg::HASH:
      handle_load_hash(lmsg);
      break;
    default:
      // TODO return error?
      break;
  }
}

void CoordinatorNode::handle_get(GetMsg& gmsg) {
  std::string outpath = my_workspace_ + "/GET_" + gmsg.array_name() + ".csv";
  std::ofstream outfile;
  outfile.open(outpath);
  for (int nodeid = 1; nodeid < nprocs_; ++nodeid) {
    mpi_handler_->receive_content(outfile, nodeid, GET_TAG);
  }
  outfile.close();
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

// TODO make asynchronous
void CoordinatorNode::handle_parallel_load(ParallelLoadMsg& pmsg) {
  logger_->log(LOG_INFO, "In handle_parallel_load");

  switch (pmsg.load_type()) {
    case ParallelLoadMsg::ORDERED_PARTITION:
      handle_parallel_load_ordered(pmsg);
      break;
    case ParallelLoadMsg::HASH_PARTITION:
      handle_parallel_load_hash(pmsg);
      break;
    default:
      // TODO return error?
      break;
  }
}

void CoordinatorNode::handle_load_ordered(LoadMsg& lmsg) {
  std::stringstream ss;

  std::string filepath = "./data/" + lmsg.filename();

  // TODO check that filename exists in workspace, error if doesn't
  // TODO save array schema?

  // TODO open file and append tile-id/hilbert order
  // inject ids if regular or hilbert order
  ArraySchema& array_schema = lmsg.array_schema();
  bool regular = array_schema.has_regular_tiles();
  ArraySchema::Order order = array_schema.order();
  std::string injected_filepath = filepath;
  std::string frag_name = "0_0";

  if (regular || order == ArraySchema::HILBERT) {
    injected_filepath = executor_->loader()->workspace() + "/injected_" +
                        array_schema.array_name() + "_" + frag_name + ".csv";
    try {
      logger_->log(LOG_INFO, "Injecting ids into " + filepath + ", outputting to " + injected_filepath);
      executor_->loader()->inject_ids_to_csv_file(filepath, injected_filepath, array_schema);
    } catch(LoaderException& le) {
      logger_->log(LOG_INFO, "Caught loader exception " + le.what());
      //remove(injected_filepath.c_str());
      executor_->storage_manager()->delete_array(array_schema.array_name());
      throw LoaderException("[WorkerNode] Cannot inject ids to file\n" + le.what());
    }
  }


  // local sort
  std::string sorted_filepath = executor_->loader()->workspace() + "/sorted_" + array_schema.array_name() + "_" + frag_name + ".csv";

  logger_->log(LOG_INFO, "Sorting csv file " + injected_filepath + " into " + sorted_filepath);

  executor_->loader()->sort_csv_file(injected_filepath, sorted_filepath, array_schema);
  logger_->log(LOG_INFO, "Finished sorting csv file");



  // send partitions back to worker nodes
  logger_->log(LOG_INFO, "Counting num_lines");
  std::ifstream sorted_file;
  sorted_file.open(sorted_filepath);
  // using cpp count algo function
  int num_lines = std::count(std::istreambuf_iterator<char>(sorted_file), 
                             std::istreambuf_iterator<char>(), '\n');

  logger_->log(LOG_INFO, "Splitting and sending sorted content to workers, num_lines: " + std::to_string(num_lines));

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

    logger_->log(LOG_INFO, "Sending sorted file part to nodeid " + std::to_string(nodeid));
    for(; pos < end; ++pos) {
      if (content.str().length() + line.length() >= MPI_BUFFER_LENGTH) {
        // send content to nodeid
        MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, nodeid, LOAD_TAG, MPI_COMM_WORLD);

        mpi_handler_->send_keep_receiving(true, nodeid);
        content.str(std::string()); // clear buffer

      }
      // TODO use stavros's csvfile?
      std::getline(sorted_file, line);
      //std::cout << line;
      content << line << "\n";
    }

    // final send
    MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, nodeid, LOAD_TAG, MPI_COMM_WORLD);
    mpi_handler_->send_keep_receiving(false, nodeid);
    --remainder;
  }


  sorted_file.close();
}

void CoordinatorNode::handle_load_hash(LoadMsg& pmsg) {
  logger_->log(LOG_INFO, "Start Handle Load Hash Partiion");
  std::stringstream ss;
  
  // TODO check that filename exists in workspace, error if doesn't
  ArraySchema array_schema = pmsg.array_schema();

  std::string filepath = "./data/" + pmsg.filename();
  logger_->log(LOG_INFO, "Sending data to workers based on hash value from " + filepath);
  // scan input file, compute hash on cell coords, send to worker
  CSVFile csv_in(filepath, CSVFile::READ);
  CSVLine csv_line;
  std::hash<std::string> hash_fn;
  while (csv_in >> csv_line) {
    // TODO look into other hash fns, using default for strings right now
    std::string coord_id = csv_line.values()[0];
    for (int i = 1; i < array_schema.dim_num(); ++i) {
      coord_id += ",";
      coord_id += csv_line.values()[i];
    }
    std::size_t cell_id_hash = hash_fn(coord_id);
    int receiver = (cell_id_hash % nworkers_) + 1;
    std::string csv_line_str = csv_line.str() + "\n";
    mpi_handler_->send_content(csv_line_str.c_str(), csv_line_str.length(), receiver, LOAD_TAG);
  }

  logger_->log(LOG_INFO, "Flushing all sends");
  mpi_handler_->flush_all_sends(LOAD_TAG);
}

// participates in all to all mpi exchange
void CoordinatorNode::handle_parallel_load_hash(ParallelLoadMsg& pmsg) {
  logger_->log(LOG_INFO, "Participating in all to all communication");
  std::ofstream tmp;
  // TODO clean this up
  tmp.open("tmp");
  mpi_handler_->finish_recv_a2a(tmp);
  tmp.close();

}

// TODO
void CoordinatorNode::handle_parallel_load_ordered(ParallelLoadMsg& pmsg) {
  logger_->log(LOG_INFO, "In handle parallel load ordered");

  // receive samples from all workers
  logger_->log(LOG_INFO, "Receiving samples from workers");
  std::vector<int64_t> samples;
  std::stringstream ss;
  for (int worker = 1; worker <= nworkers_; ++worker) {

    logger_->log(LOG_INFO, "Waiting for samples from worker " + std::to_string(worker));

    SamplesMsg* smsg = mpi_handler_->receive_samples_msg(worker);
    
    for (int i = 0; i < smsg->samples().size(); ++i) {
      samples.push_back(smsg->samples()[i]);
    }
    // TODO cleanup
  }




  // pick nworkers - 1 samples for the n - 1 "stumps"
  //
  logger_->log(LOG_INFO, "Getting partitions");
  std::vector<int64_t> partitions = get_partitions(samples, nworkers_ - 1);
 
  logger_->log(LOG_INFO, "sending partitions back to all workers");
  // send partition infor back to all workers
  SamplesMsg msg(partitions);
  for (int worker = 1; worker <= nworkers_ ; worker++) {
    mpi_handler_->send_samples_msg(&msg, worker);
  }

  logger_->log(LOG_INFO, "Participating in all to all communication");
  std::ofstream tmp;
  // TODO clean this up
  tmp.open("tmp");
  mpi_handler_->finish_recv_a2a(tmp);
  tmp.close();

  // cleanup
}

void CoordinatorNode::quit_all() {
  send_all("quit", QUIT_TAG);
}

/******************************************************
 **************** HELPER FUNCTIONS ********************
 ******************************************************/
std::vector<int64_t> CoordinatorNode::get_partitions(std::vector<int64_t> samples, int k) {
  std::default_random_engine generator;
  std::uniform_int_distribution<int64_t> distribution(0, samples.size()-1);
  auto dice = std::bind(distribution, generator);
  std::vector<int64_t> partitions;
  for (int i = 0; i < k; ++i) {
    partitions.push_back(samples[dice()]);
  }
  std::sort(partitions.begin(), partitions.end());
  return partitions;
}

/******************************************************
 *************** TESTING FUNCTIONS ********************
 ******************************************************/

void CoordinatorNode::test_load(std::string array_name) {
  logger_->log(LOG_INFO, "Start Load");
  logger_->log(LOG_INFO, "loading array " + array_name);
  ArraySchema * array_schema = get_test_arrayschema(array_name);
  ArraySchema::Order order = ArraySchema::ROW_MAJOR;
  LoadMsg lmsg = LoadMsg(array_name, *array_schema, LoadMsg::ORDERED);

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
