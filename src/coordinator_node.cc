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
#include "hash_functions.h"
#include "constants.h"
#include "util.h"

CoordinatorNode::CoordinatorNode(int rank, int nprocs,
    std::string datadir, std::string workspace_base) {
  myrank_ = rank;
  nprocs_ = nprocs;
  nworkers_ = nprocs - 1;

  // TODO put in config file
  //my_workspace_ = "./workspaces/workspace-0";
  my_workspace_ = workspace_base + "/workspace-0";
  logger_ = new Logger(my_workspace_ + "/logfile");
  executor_ = new Executor(my_workspace_);

  std::vector<int> workers;
  for (int i = 1; i < nprocs; ++i) {
    workers.push_back(i);
  }

  mpi_handler_ = new MPIHandler(0, workers, logger_);
  datadir_ = datadir;
  md_manager_ = new MetaDataManager(my_workspace_);
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
  std::string array_name = "test_F";
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

  
  ArraySchema array_schema_C = array_schema.clone("test_F_ordered");
  std::string filename_C = "test_F.csv";
  ArraySchema array_schema_D = array_schema.clone("test_G_ordered");
  std::string filename_D = "test_G.csv";


  DEBUG_MSG("Sending DEFINE ARRAY to all workers for " + array_schema_C.array_name());
  DefineArrayMsg damsg_C = DefineArrayMsg(array_schema_C);
  send_and_receive(damsg_C);

  DEBUG_MSG("Sending ordered partition load instructions to all workers");
  LoadMsg lmsg_C = LoadMsg(filename_C, array_schema_C, ORDERED_PARTITION);
  send_and_receive(lmsg_C);

  DEBUG_MSG("Sending DEFINE ARRAY to all workers for array " + array_schema_D.array_name());
  DefineArrayMsg damsg_D = DefineArrayMsg(array_schema_D);
  send_and_receive(damsg_D);

  DEBUG_MSG("Sending ordered partition load instructions to all workers");
  LoadMsg lmsg_D = LoadMsg(filename_D, array_schema_D, ORDERED_PARTITION);
  send_and_receive(lmsg_D);


  DEBUG_MSG("Sending join");
  std::string join_ordered_result = "join_ordered_" + array_schema_C.array_name() + "_" + array_schema_D.array_name();
  JoinMsg jmsg2 = JoinMsg(array_schema_C.array_name(), array_schema_D.array_name(), join_ordered_result);
  send_and_receive(jmsg2);

  DEBUG_MSG("Sending GET result to all workers");
  GetMsg gmsg2 = GetMsg(join_ordered_result);
  send_and_receive(gmsg2);

  logger_->log(LOG_INFO, "Sending DEFINE ARRAY to all workers for array " + array_schema.array_name());
  DefineArrayMsg damsg = DefineArrayMsg(array_schema);
  send_and_receive(damsg);

  logger_->log(LOG_INFO, "Sending ordered partition load instructions to all workers");
  LoadMsg lmsg = LoadMsg(filename, array_schema, HASH_PARTITION);
  send_and_receive(lmsg);

  ArraySchema array_schema_B = array_schema.clone("test_G");
  std::string filename_B = array_schema_B.array_name() + ".csv";


  logger_->log(LOG_INFO, "Sending DEFINE ARRAY to all workers for array " + array_schema_B.array_name());
  DefineArrayMsg damsg2 = DefineArrayMsg(array_schema_B);
  send_and_receive(damsg2);

  logger_->log(LOG_INFO, "Sending ordered partition load instructions to all workers");
  LoadMsg lmsg2 = LoadMsg(filename_B, array_schema_B, HASH_PARTITION);
  send_and_receive(lmsg2);

  logger_->log(LOG_INFO, "Sending join hash");
  std::string join_result = "join_hash_" + array_schema.array_name() + "_" + array_schema_B.array_name();
  JoinMsg jmsg = JoinMsg("test_F", "test_G", join_result);
  send_and_receive(jmsg);

  logger_->log(LOG_INFO, "Sending GET result to all workers");
  GetMsg gmsg1 = GetMsg(join_result);
  send_and_receive(gmsg1);


  /*
  DEBUG_MSG("sending subarray");
  std::vector<double> vec;
  vec.push_back(0); vec.push_back(500000);
  vec.push_back(0); vec.push_back(500000);
  SubarrayMsg sbmsg("subarray", array_schema, vec);
  send_and_receive(sbmsg);
  DEBUG_MSG("done sending subarray messages");


  std::string sarray_name = "subarray";
  DEBUG_MSG("Sending GET " + sarray_name + " to all workers");
  GetMsg gmsg1(sarray_name);
  send_and_receive(gmsg1);
  */


  /*
  DEBUG_MSG("sending filter instruction to all workers");
  std::string expr = "attr1"; // expression hard coded in executor.cc
  std::string result = "filter";
  FilterMsg fmsg(array_name, expr, result);
  send_and_receive(fmsg);

  std::string farray_name = "filter";
  DEBUG_MSG("Sending GET " + farray_name + " to all workers");
  GetMsg gmsg2(farray_name);
  send_and_receive(gmsg2);
  */


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
  for (int i = 1; i < nprocs_; i++) {
    MPI_Send((char *)buffer, buffer_size, MPI::CHAR, i, tag, MPI_COMM_WORLD);
  }
}

// dispatch to correct handler
void CoordinatorNode::send_and_receive(Msg& msg) {
  send_all(msg);
  switch(msg.msg_tag) {
    case GET_TAG:
      handle_get(dynamic_cast<GetMsg&>(msg));
      handle_acks();
      break;
    case LOAD_TAG:
      handle_load(dynamic_cast<LoadMsg&>(msg));
      handle_acks();
      break;
    case PARALLEL_LOAD_TAG:
      handle_parallel_load(dynamic_cast<ParallelLoadMsg&>(msg));
      handle_acks();
      break;
    case DEFINE_ARRAY_TAG:
    case FILTER_TAG:
    case SUBARRAY_TAG:
      handle_acks();
      break;
    case AGGREGATE_TAG:
      //handle_aggregate();
      break;
    case JOIN_TAG:
      handle_join(dynamic_cast<JoinMsg&>(msg));
      // acks handled internally
      break;
    default:
      // don't do anything
      break;
  }

}

int CoordinatorNode::handle_acks() {

  bool all_success = true;
  for (int nodeid = 1; nodeid <= nworkers_; nodeid++) {
    logger_->log(LOG_INFO, "Waiting for ack from worker " + util::to_string(nodeid));
    AckMsg* ack = mpi_handler_->receive_ack(nodeid);

    if (ack->result() == AckMsg::ERROR) {
      all_success = false;
    }

    logger_->log(LOG_INFO, "Received ack " + ack->to_string() + " from worker: " + util::to_string(nodeid));

    // cleanup
    delete ack;
  }

  return all_success ? 0 : -1;
}

/******************************************************
 **                  HANDLE GET                      **
 ******************************************************/
void CoordinatorNode::handle_get(GetMsg& gmsg) {
  logger_->log(LOG_INFO, "In handle_get");
  std::string outpath = my_workspace_ + "/GET_" + gmsg.array_name() + ".csv";
  std::ofstream outfile;
  outfile.open(outpath.c_str());
  bool keep_receiving = true;
  for (int nodeid = 1; nodeid < nprocs_; ++nodeid) {
    // error handling if file is not found on sender
    keep_receiving = mpi_handler_->receive_keep_receiving(nodeid);
    logger_->log(LOG_INFO, "Sender: " + util::to_string(nodeid) + " Keep receiving: " + util::to_string(keep_receiving));
    if (keep_receiving == true) {
      logger_->log(LOG_INFO, "Waiting for sender " + util::to_string(nodeid));
      mpi_handler_->receive_content(outfile, nodeid, GET_TAG);
    }
  }
  outfile.close();
}

/******************************************************
 **                  HANDLE LOAD                     **
 ******************************************************/
void CoordinatorNode::handle_load(LoadMsg& lmsg) {
  MetaData metadata;
  switch (lmsg.partition_type()) {
    case ORDERED_PARTITION:
      switch(lmsg.load_method()) {
        case LoadMsg::SORT: 
          handle_load_ordered_sort(lmsg);
          break;
        case LoadMsg::SAMPLE:
          handle_load_ordered_sample(lmsg);
          break;
        default:
          // shouldn't get here
          break;
      }
      break;
    case HASH_PARTITION:
      handle_load_hash(lmsg);

      logger_->log_start(LOG_INFO, "Writing metadata to disk");
      metadata = MetaData(HASH_PARTITION);
      md_manager_->store_metadata(lmsg.array_schema().array_name(), metadata);
      logger_->log_end(LOG_INFO);

      break;
    default:
      // TODO return error?
      break;
  }
}


void CoordinatorNode::handle_load_ordered_sort(LoadMsg& lmsg) {
  std::stringstream ss;

  std::string filepath = datadir_ + "/" + lmsg.filename();

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
      logger_->log_start(LOG_INFO, "Injecting ids into " + filepath + ", outputting to " + injected_filepath);
      executor_->loader()->inject_ids_to_csv_file(filepath, injected_filepath, array_schema);
    } catch(LoaderException& le) {
      logger_->log(LOG_INFO, "Caught loader exception " + le.what());
#ifdef NDEBUG
      remove(injected_filepath.c_str());
      executor_->storage_manager()->delete_array(array_schema.array_name());
#endif
      throw LoaderException("[WorkerNode] Cannot inject ids to file\n" + le.what());
    }
  }
  logger_->log_end(LOG_INFO);


  // local sort
  std::string sorted_filepath = executor_->loader()->workspace() + "/sorted_" + array_schema.array_name() + "_" + frag_name + ".csv";

  logger_->log_start(LOG_INFO, "Sorting csv file " + injected_filepath + " into " + sorted_filepath);

  executor_->loader()->sort_csv_file(injected_filepath, sorted_filepath, array_schema);
  logger_->log_end(LOG_INFO);



  // send partitions back to worker nodes
  logger_->log(LOG_INFO, "Counting num_lines");
  std::ifstream sorted_file;
  sorted_file.open(sorted_filepath.c_str());
  // using cpp count algo function
  int num_lines = std::count(std::istreambuf_iterator<char>(sorted_file), 
                             std::istreambuf_iterator<char>(), '\n');


  ss.str(std::string());
  ss << "Splitting and sending sorted content to workers, num_lines: " << num_lines;
  logger_->log(LOG_INFO, ss.str());

  int lines_per_worker = num_lines / nworkers_;
  // if not evenly split
  int remainder = num_lines % nworkers_;

  int pos = 0;
  int total = lines_per_worker;

  if (remainder > 0) {
    total++;
  }

  sorted_file.close();
  //sorted_file.seekg(0, std::ios::beg);
  CSVFile csv(sorted_filepath, CSVFile::READ);
  CSVLine csv_line;
  std::vector<uint64_t> partitions; // nworkers - 1 partitions
  for (int nodeid = 1; nodeid < nprocs_; ++nodeid) {
    std::string line;
    std::stringstream content;
    int end = pos + lines_per_worker;
    if (remainder > 0) {
      end++;
    }

    logger_->log(LOG_INFO, "Sending sorted file part to nodeid " + util::to_string(nodeid) + " with " + util::to_string(end - pos) +  " lines");

    for(; pos < end - 1; ++pos) {

      csv >> csv_line;
      line = csv_line.str() + "\n";
      mpi_handler_->send_content(line.c_str(), line.size(), nodeid, LOAD_TAG);
    }

    // need nworkers - 1 "stumps" for partition ranges
    assert(pos == end - 1); 
    csv >> csv_line;
    line = csv_line.str() + "\n";
    mpi_handler_->send_content(line.c_str(), line.size(), nodeid, LOAD_TAG);
    if (nodeid != nprocs_ - 1) { // not last node
      uint64_t sample = std::strtoull(csv_line.values()[0].c_str(), NULL, 10);
      partitions.push_back(sample);
    }

    // final send
    mpi_handler_->flush_send(nodeid, LOAD_TAG);
    assert(mpi_handler_->all_buffers_empty() == true);

    --remainder;
  }


  assert(partitions.size() == nworkers_ - 1);

  logger_->log(LOG_INFO, "Sending partition samples to all workers");
  SamplesMsg msg(partitions);
  for (int worker = 1; worker <= nworkers_ ; worker++) {
    logger_->log(LOG_INFO, "Sending partitions to worker " + util::to_string(worker));
    mpi_handler_->send_samples_msg(&msg, worker);
  }

  // store metadata to disk
  logger_->log_start(LOG_INFO, "Writing metadata to disk");
  MetaData metadata(ORDERED_PARTITION,
      std::pair<uint64_t, uint64_t>(partitions[0], partitions[partitions.size()-1]),
      partitions);
  md_manager_->store_metadata(array_schema.array_name(), metadata);
  logger_->log_end(LOG_INFO);

  logger_->log(LOG_INFO, "Finished handle load ordered sort");
}

void CoordinatorNode::handle_load_ordered_sample(LoadMsg& msg) {
  logger_->log(LOG_INFO, "Start Handle Load Ordered Partiion using Sampling");

  std::stringstream ss;

  std::string filepath = datadir_ + "/" + msg.filename();

  // inject cell ids and sample
  CSVFile csv_in(filepath, CSVFile::READ);
  CSVLine line_in, line_out;
  ArraySchema& array_schema = msg.array_schema();
  ArraySchema::Order order = array_schema.order();
  std::string frag_name = "0_0";


  std::string injected_filepath = filepath;

  // TODO support other orderings later
  assert(array_schema.has_regular_tiles() || 
         array_schema.order() == ArraySchema::HILBERT);


  injected_filepath = executor_->loader()->workspace() + "/injected_" +
                      array_schema.array_name() + "_" + frag_name + ".csv";

  CSVFile *csv_out = new CSVFile(injected_filepath, CSVFile::WRITE);

  uint64_t cell_id;
  std::vector<double> coordinates;
  unsigned int dim_num = array_schema.dim_num();
  coordinates.resize(dim_num);
  double coordinate;
  int resevoir_count = 0;
  int num_samples = msg.num_samples();
  std::vector<uint64_t> samples;

  logger_->log_start(LOG_INFO, "Inject cell ids to " + injected_filepath + " and while resevoir sampling");
  while (csv_in >> line_in) {
    // Retrieve coordinates from the input line
    for(unsigned int i=0; i < array_schema.dim_num(); i++) {
      if(!(line_in >> coordinate))
        throw LoaderException("Cannot read coordinate value from CSV file."); 
      coordinates[i] = coordinate;
    }

    // Put the id at the beginning of the output line
    if(array_schema.has_regular_tiles()) { // Regular tiles
      if(order == ArraySchema::HILBERT)
        cell_id = array_schema.tile_id_hilbert(coordinates);
      else if(order == ArraySchema::ROW_MAJOR)
        cell_id = array_schema.tile_id_row_major(coordinates);
      else if(order == ArraySchema::COLUMN_MAJOR)
        cell_id = array_schema.tile_id_column_major(coordinates);
    } else { // Irregular tiles + Hilbert cell order
        cell_id = array_schema.cell_id_hilbert(coordinates);
    }
    line_out = cell_id;
    // Append the input line to the output line, 
    // and then into the output CSV file
    line_out << line_in;
    (*csv_out) << line_out;

    // do resevoir sampling
    if (resevoir_count < num_samples) {
      samples.push_back(cell_id);
    } else {
        // replace elements with gradually decreasing probability
        // TODO double check off by one error?
        int r = (rand() % resevoir_count) + 1; // 0 to counter inclusive
        if (r < num_samples) {
          samples[r] = cell_id;
        }
    }

    resevoir_count++;
  }

  logger_->log_end(LOG_INFO);

  // call destructor to force flush
  delete csv_out;

  // sample and get partitions
  std::vector<uint64_t> partitions = get_partitions(samples, nworkers_ - 1);

  // scan input and send partitions to workers
  logger_->log_start(LOG_INFO, "Send partitions to workers, reading " + injected_filepath);
  CSVFile csv_injected(injected_filepath, CSVFile::READ);
  CSVLine csv_line;
  while (csv_injected >> csv_line) {
    int64_t cell_id = std::strtoll(csv_line.values()[0].c_str(), NULL, 10);
    int receiver = get_receiver(partitions, cell_id);
    std::string csv_line_str = csv_line.str() + "\n";
    mpi_handler_->send_content(csv_line_str.c_str(),
        csv_line_str.length(), receiver, LOAD_TAG);
  }


  // flush all sends
  logger_->log(LOG_INFO, "Flushing all sends");
  mpi_handler_->flush_all_sends(LOAD_TAG);

  logger_->log_end(LOG_INFO);

  // send partition boundaries back to workers to record
  logger_->log(LOG_INFO, "Sending partition samples to all workers: " +
      util::to_string(partitions));

  SamplesMsg smsg(partitions);
  for (int worker = 1; worker <= nworkers_ ; worker++) {
    mpi_handler_->send_samples_msg(&smsg, worker);
  }

  // store metadata to disk
  logger_->log_start(LOG_INFO, "Writing metadata to disk");
  MetaData metadata(ORDERED_PARTITION,
      std::pair<uint64_t, uint64_t>(partitions[0], partitions[partitions.size()-1]),
      partitions);
  md_manager_->store_metadata(array_schema.array_name(), metadata);


  logger_->log_end(LOG_INFO);

  logger_->log(LOG_INFO, "Finished handle load ordered sampling");
}


void CoordinatorNode::handle_load_hash(LoadMsg& pmsg) {
  logger_->log(LOG_INFO, "Start Handle Load Hash Partiion");
  std::stringstream ss;

  // TODO check that filename exists in workspace, error if doesn't
  ArraySchema array_schema = pmsg.array_schema();

  std::string filepath = datadir_ + "/" + pmsg.filename();
  logger_->log(LOG_INFO, "Sending data to workers based on hash value from " + filepath);
  // scan input file, compute hash on cell coords, send to worker
  CSVFile csv_in(filepath, CSVFile::READ);
  CSVLine csv_line;
  while (csv_in >> csv_line) {
    // TODO look into other hash fns, see hash_functions.h for borrowed ones
    std::string coord_id = csv_line.values()[0];
    for (int i = 1; i < array_schema.dim_num(); ++i) {
      coord_id += ",";
      coord_id += csv_line.values()[i];
    }
    std::size_t cell_id_hash = DEKHash(coord_id);
    int receiver = (cell_id_hash % nworkers_) + 1;
    std::string csv_line_str = csv_line.str() + "\n";
    mpi_handler_->send_content(csv_line_str.c_str(), csv_line_str.length(), receiver, LOAD_TAG);
  }

  logger_->log(LOG_INFO, "Flushing all sends");
  mpi_handler_->flush_all_sends(LOAD_TAG);
}


/******************************************************
 **             HANDLE PARALLEL LOAD                 **
 ******************************************************/
void CoordinatorNode::handle_parallel_load(ParallelLoadMsg& pmsg) {
  logger_->log(LOG_INFO, "In handle_parallel_load");

  MetaData metadata;
  switch (pmsg.partition_type()) {
    case ORDERED_PARTITION:
      handle_parallel_load_ordered(pmsg);
      break;
    case HASH_PARTITION:
      handle_parallel_load_hash(pmsg);

      logger_->log_start(LOG_INFO, "write metadata");
      metadata = MetaData(HASH_PARTITION);
      md_manager_->store_metadata(pmsg.array_schema().array_name(), metadata);
      logger_->log_end(LOG_INFO);

      break;
    default:
      // TODO return error?
      break;
  }
}

// participates in all to all mpi exchange
void CoordinatorNode::handle_parallel_load_hash(ParallelLoadMsg& pmsg) {
  logger_->log(LOG_INFO, "{Query Start: pload hash}");
  mpi_handler_->finish_recv_a2a();
}

void CoordinatorNode::handle_parallel_load_ordered(ParallelLoadMsg& pmsg) {
  logger_->log(LOG_INFO, "{Query Start: pload ordered}");

  // receive samples from all workers
  logger_->log_start(LOG_INFO, "Receive samples");
  std::vector<uint64_t> samples;
  std::stringstream ss;
  for (int worker = 1; worker <= nworkers_; ++worker) {

    logger_->log(LOG_INFO, "Waiting for samples from worker " + util::to_string(worker));

    SamplesMsg* smsg = mpi_handler_->receive_samples_msg(worker);

    for (int i = 0; i < smsg->samples().size(); ++i) {
      samples.push_back(smsg->samples()[i]);
    }
    // TODO cleanup
  }
  logger_->log_end(LOG_INFO);

  // pick nworkers - 1 samples for the n - 1 "stumps"
  logger_->log_start(LOG_INFO, "compute partitions");
  std::vector<uint64_t> partitions = get_partitions(samples, nworkers_ - 1);
  logger_->log_end(LOG_INFO);

  logger_->log_start(LOG_INFO, "send partitions");
  // send partition infor back to all workers
  logger_->log(LOG_INFO, "Partitions: " + util::to_string(partitions));
  SamplesMsg msg(partitions);
  for (int worker = 1; worker <= nworkers_ ; worker++) {
    mpi_handler_->send_samples_msg(&msg, worker);
  }
  logger_->log_end(LOG_INFO);

  logger_->log_start(LOG_INFO, "N to N Shuffle");
  mpi_handler_->finish_recv_a2a();
  logger_->log_end(LOG_INFO);

  // store metadata to disk
  logger_->log_start(LOG_INFO, "Write metadata");
  MetaData metadata(ORDERED_PARTITION,
      std::pair<uint64_t, uint64_t>(partitions[0], partitions[partitions.size()-1]),
      partitions);
  md_manager_->store_metadata(pmsg.array_schema().array_name(), metadata);
  logger_->log_end(LOG_INFO);


  // cleanup
}


/******************************************************
 **                  HANDLE JOIN                     **
 ******************************************************/
void CoordinatorNode::handle_join(JoinMsg& msg) {
  MetaData *md_A = md_manager_->retrieve_metadata(msg.array_name_A());
  MetaData *md_B = md_manager_->retrieve_metadata(msg.array_name_B());

  assert(md_A->partition_type() == md_B->partition_type());

  MetaData md_C;
  int r;
  switch (md_A->partition_type()) {
    case ORDERED_PARTITION:
      handle_join_ordered(msg);
      break;
    case HASH_PARTITION:
     break;
    default:
      // shouldn't get here
     break;
  }

  r = handle_acks();
  if (r == 0) {
    logger_->log_start(LOG_INFO, "Query successful, writing new metadata");
    md_C = MetaData(md_A->partition_type());
    md_manager_->store_metadata(msg.result_array_name(), md_C);
    logger_->log_end(LOG_INFO);
  } else {
    logger_->log(LOG_INFO, "Query failed, no new array created");
  }

  // cleanup
  delete md_A;
  delete md_B;
}

// TODO
void CoordinatorNode::handle_join_ordered(JoinMsg& msg) {

  logger_->log_start(LOG_INFO, "N to N shuffle array A bounding coords");
  mpi_handler_->finish_recv_a2a();
  logger_->log_end(LOG_INFO);

  logger_->log_start(LOG_INFO, "N to N shuffle array B bounding coords");
  mpi_handler_->finish_recv_a2a();
  logger_->log_end(LOG_INFO);

  logger_->log_start(LOG_INFO, "N to N shuffle array A join fragments");
  mpi_handler_->finish_recv_a2a();
  logger_->log_end(LOG_INFO);

  logger_->log_start(LOG_INFO, "N to N shuffle array B join fragments");
  mpi_handler_->finish_recv_a2a();
  logger_->log_end(LOG_INFO);

}


void CoordinatorNode::quit_all() {
  logger_->log(LOG_INFO, "Sending quit all");
  send_all("quit", QUIT_TAG);
}

/******************************************************
 **************** HELPER FUNCTIONS ********************
 ******************************************************/
std::vector<uint64_t> CoordinatorNode::get_partitions(std::vector<uint64_t> samples, int k) {
  // We are picking k partition boundaries that will divide the
  // entire distributed dataset into k + 1 somewhat equal (hopefully) partitions
  std::sort(samples.begin(), samples.end());
  std::vector<uint64_t> partitions;

  int num_partitions = k + 1;
  int num_samples_per_part = samples.size() / num_partitions;
  int remainder = samples.size() % num_partitions;
  if (remainder > 0) {
    num_samples_per_part++;
  }

  for (int i = 1; i <= k; ++i) {
    partitions.push_back(samples[i*num_samples_per_part]);
  }
  return partitions;
}

// TODO optimize to use binary search if needed
// TODO move to util class
inline int CoordinatorNode::get_receiver(std::vector<uint64_t> partitions, uint64_t cell_id) {
  int recv = 1;
  for (std::vector<uint64_t>::iterator it = partitions.begin(); it != partitions.end(); ++it) {
    if (cell_id <= *it) {
      return recv;
    }
    recv++;
  }
  return recv;
}
/******************************************************
 *************** TESTING FUNCTIONS ********************
 ******************************************************/

void CoordinatorNode::test_load(std::string array_name, 
    std::string filename, 
    PartitionType partition_type,
    LoadMsg::LoadMethod method) {

  logger_->log(LOG_INFO, "TEST START load array_name: " + array_name + " filename: " + filename);
  logger_->log(LOG_INFO, "Sending DEFINE ARRAY to all workers for array " + array_name);

  ArraySchema * array_schema = get_test_arrayschema(array_name);
  DefineArrayMsg damsg = DefineArrayMsg(*array_schema);
  send_and_receive(damsg);


  logger_->log(LOG_INFO, "Sending LOAD ARRAY to all workers for array " + array_name);
  LoadMsg lmsg = LoadMsg(filename, *array_schema, partition_type, method);

  send_and_receive(lmsg);

  // TODO don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_parallel_load(std::string array_name,
    std::string filename,
    PartitionType partition_type, int num_samples) {

    logger_->log(LOG_INFO, "TEST START pload array_name: " + array_name + " filename: " + filename);

    logger_->log(LOG_INFO, "Sending DEFINE ARRAY to all workers for array " + array_name);

  ArraySchema * array_schema = get_test_arrayschema(array_name);
  DefineArrayMsg damsg = DefineArrayMsg(*array_schema);
  send_and_receive(damsg);


  logger_->log(LOG_INFO, "Sending PARALLEL LOAD ARRAY to all workers for array " + array_name);
  ParallelLoadMsg msg = ParallelLoadMsg(filename, partition_type, *array_schema, num_samples);

  send_and_receive(msg);

  //delete array_schema;
}

void CoordinatorNode::test_join(std::string array_name_A,
    std::string array_name_B, std::string result_array_name) {
  logger_->log(LOG_INFO, "TEST START join Start on array_name_A: " + array_name_A + " array_name_B: " + array_name_B + " result_array_name: " + result_array_name);

  logger_->log(LOG_INFO, "Sending JOIN MSG to all workers");
  JoinMsg msg = JoinMsg(array_name_A, array_name_B, result_array_name); 
  send_and_receive(msg);

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
  /*
  FilterMsg<int> fmsg = FilterMsg<int>(array_schema->celltype(attr_index), *array_schema, pred, array_name+"_filtered");

  send_and_receive(fmsg);
  */
  logger_->log(LOG_INFO, "Test Filter Done");

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_subarray_sparse(std::string array_name) {
  logger_->log(LOG_INFO, "TEST START subarray sparse array_name: " + array_name);
  ArraySchema* array_schema = get_test_arrayschema(array_name);
  std::vector<double> vec;


  if (array_name.compare(0, 4, "test") == 0) {
    // .5 selectivity [0, 10000]
    vec.push_back(0); vec.push_back(1000);
    vec.push_back(0); vec.push_back(1000);
  } else {
    vec.push_back(40000000); vec.push_back(51500000);
    vec.push_back(125000000); vec.push_back(136500000);
  }

  SubarrayMsg sbmsg("subarrays_" + array_name, *array_schema, vec);
  send_and_receive(sbmsg);

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_subarray_dense(std::string array_name) {
  logger_->log(LOG_INFO, "TEST START subarray sparse array_name: " + array_name);
  ArraySchema* array_schema = get_test_arrayschema(array_name);
  std::vector<double> vec;


  if (array_name.compare(0, 4, "test") == 0) {
    // .5 selectivity [0, 10000]
    vec.push_back(0); vec.push_back(1000);
    vec.push_back(0); vec.push_back(1000);
  } else {
    vec.push_back(106000000); vec.push_back(106050000);
    vec.push_back(130700000); vec.push_back(130750000);
  }

  SubarrayMsg sbmsg("subarrayd_" + array_name, *array_schema, vec);
  send_and_receive(sbmsg);

  // don't leak memory
  //delete array_schema;
}

void CoordinatorNode::test_aggregate(std::string array_name) {
  logger_->log(LOG_INFO, "Start Aggregate3 test");

  int attr_index = 1;
  AggregateMsg amsg(array_name, 1);
  send_and_receive(amsg);
  logger_->log(LOG_INFO, "Test Aggregate Done");

  // don't leak memory
  //delete array_schema;
}

ArraySchema* CoordinatorNode::get_test_arrayschema(std::string array_name) {

  // array schema for test_[X}.csv 
  if (array_name.compare(0, 4, "test") == 0) {
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
    ArraySchema *array_schema = new ArraySchema(array_name,
        attribute_names,
        dim_names,
        dim_domains,
        types,
        ArraySchema::HILBERT);
    return array_schema;
  }

  // array schema for ais data
  // Set attribute names
  std::vector<std::string> attribute_names;
  attribute_names.push_back("sog"); // float
  attribute_names.push_back("cog"); // float
  attribute_names.push_back("heading"); // float
  attribute_names.push_back("rot"); // float
  attribute_names.push_back("status"); // int
  attribute_names.push_back("voyageid"); // int64_t
  attribute_names.push_back("mmsi"); // int64_t


  // Set attribute types
  std::vector<const std::type_info*> types;
  types.push_back(&typeid(float));
  types.push_back(&typeid(float));
  types.push_back(&typeid(float));
  types.push_back(&typeid(float));
  types.push_back(&typeid(int));
  types.push_back(&typeid(int64_t));
  types.push_back(&typeid(int64_t));


  // Set dimension type
  types.push_back(&typeid(int64_t));

  // Set dimension names
  std::vector<std::string> dim_names;
  dim_names.push_back("coordX");
  dim_names.push_back("coordY");

  // Set dimension domains
  std::vector<std::pair<double,double> > dim_domains;
  dim_domains.push_back(std::pair<double,double>(0, 360000000));
  dim_domains.push_back(std::pair<double,double>(0, 180000000));

  // Create an array with irregular tiles
  ArraySchema *array_schema = new ArraySchema(array_name,
      attribute_names,
      dim_names,
      dim_domains,
      types,
      ArraySchema::HILBERT);
  return array_schema;

}
