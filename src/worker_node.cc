#include <ostream>
#include <iostream>
#include <istream>
#include <fstream>
#include <cstring>
#include <sys/time.h>
#include <fcntl.h> // O_RDONLY
#include <mpi.h>
#include "constants.h"
#include "assert.h"
#include "worker_node.h"
#include "debug.h"
#include "csv_file.h"
#include "hash_functions.h"
#include "util.h"

WorkerNode::WorkerNode(int rank,
    int nprocs,
    std::string datadir,
    std::string workspace_base,
    int64_t mpi_buffer_length,
    int64_t mpi_handler_total_buf_size) {

  myrank_ = rank;
  nprocs_ = nprocs;
  nworkers_ = nprocs - 1;
  mpi_buffer_length_ = mpi_buffer_length;
  mpi_handler_total_buf_size_ = mpi_handler_total_buf_size;

  std::stringstream workspace;
  // TODO put in config file
  workspace << workspace_base << "/workspace-" << myrank_;
  my_workspace_ = workspace.str();
  datadir_ = datadir;
  executor_ = new Executor(my_workspace_);
  logger_ = new Logger(my_workspace_ + "/logfile");
  md_manager_ = new MetaDataManager(my_workspace_);


  std::vector<int> other_nodes; // everyone except self
  for (int i = 0; i < nprocs; ++i) {
    if (i != myrank_) {
      other_nodes.push_back(i);
    }
  }

  mpi_handler_ = new MPIHandler(myrank_, 
      other_nodes, 
      logger_,
      mpi_buffer_length_,
      mpi_handler_total_buf_size_);

  // catalogue data structures
  arrayname_map_ = new std::map<std::string, std::string>();
  global_schema_map_ = new std::map<std::string, ArraySchema *>();
  local_schema_map_ = new std::map<std::string, ArraySchema *>();
}

// TODO delete things inside maps?
WorkerNode::~WorkerNode() {
  delete arrayname_map_;
  delete global_schema_map_;
  delete local_schema_map_;

  delete executor_;
  delete logger_;
  delete md_manager_;
  delete mpi_handler_;
}


void WorkerNode::run() {
  logger_->log(LOG_INFO, "I am a worker node");

  MPI_Status status;
  char *buf = new char[MPI_BUFFER_LENGTH];
  int length;
  int loop = true;
  int result;
  Msg* msg;

  // TIMING VARIABLES
  struct timeval tim;  
  double tstart;
  double tend;
  while (loop) {
    try {
      MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_CHAR, &length);
      switch (status.MPI_TAG) {
        case QUIT_TAG:
          logger_->log(LOG_INFO, "Quitting...");
          loop = false;
          break;
        case GET_TAG:
        case DEFINE_ARRAY_TAG:
        case LOAD_TAG:
        case SUBARRAY_TAG:
        case FILTER_TAG:
        case AGGREGATE_TAG:
        case PARALLEL_LOAD_TAG:
        case JOIN_TAG:
          gettimeofday(&tim, NULL);
          tstart = tim.tv_sec+(tim.tv_usec/1000000.0);

          msg = deserialize_msg(status.MPI_TAG, buf, length);
          result = handle_msg(msg->msg_tag, msg);

          gettimeofday(&tim, NULL);
          tend = tim.tv_sec+(tim.tv_usec/1000000.0);

          respond_ack(result, status.MPI_TAG, tend - tstart);
          break;

        default:
          std::string content(buf, length);

          logger_->log(LOG_INFO, "Msg Tag not defined " + content);
          break;
      }

    // TODO delete stuff to avoid memory leak
    } catch(LoaderException& le) {
      logger_->log(LOG_INFO, "LoaderException: ");
      logger_->log(LOG_INFO, le.what());
      respond_ack(-1, status.MPI_TAG, -1);
    } catch (ExecutorException& ee) {
      logger_->log(LOG_INFO, "ExecutorException: ");
      logger_->log(LOG_INFO, ee.what());

      respond_ack(-1, status.MPI_TAG, -1);
    }

  }
}

void WorkerNode::respond_ack(int r, int tag, double time) {

  logger_->log(LOG_INFO, "Responding ack: " + util::to_string(r));

  AckMsg::Result result = (r < 0) ? AckMsg::ERROR : AckMsg::DONE;
  AckMsg ack(result, tag, time); 

  logger_->log(LOG_INFO, "Sending ack: " + ack.to_string());
  mpi_handler_->send_ack(&ack, MASTER);

}

/******************************************************
 ****************** MESSAGE HANDLERS ******************
 ******************************************************/

/******************************************************
 **                   HANDLE GET                     **
 ******************************************************/
int WorkerNode::handle(GetMsg* msg) {
  logger_->log(LOG_INFO, "Received Get Msg: " + msg->array_name());

  std::string result_filename = arrayname_to_csv_filename(msg->array_name());
  logger_->log(LOG_INFO, "Result filename: " + result_filename);


  logger_->log(LOG_INFO, "exporting to CSV");
  executor_->export_to_csv(msg->array_name(), result_filename);

  int fd = open(result_filename.c_str(), O_RDONLY);
  if (fd == -1) {
    logger_->log(LOG_INFO, "csv " + result_filename + " not found, telling master to stop receiving from me");
    mpi_handler_->send_keep_receiving(false, MASTER);
  } else {
    logger_->log(LOG_INFO, "Found file, sending to master");
    mpi_handler_->send_keep_receiving(true, MASTER);
    mpi_handler_->send_file(result_filename, MASTER, GET_TAG);
  }

  return 0;
}

/******************************************************
 **               HANDLE Define Array                **
 ******************************************************/
int WorkerNode::handle(DefineArrayMsg* msg) {
  logger_->log(LOG_INFO, "Received define array msg");

  executor_->define_array(msg->array_schema());

  logger_->log(LOG_INFO, "Finished defining array");
  return 0;
}

/******************************************************
 **               HANDLE SubarrayMsg                 **
 ******************************************************/
int WorkerNode::handle(SubarrayMsg* msg) {
  logger_->log(LOG_INFO, "Received subarray \n");

  executor_->subarray(msg->array_schema().array_name(), msg->ranges(), msg->result_array_name());

  logger_->log(LOG_INFO, "Finished subarray ");

  return 0;
}

/******************************************************
 **               HANDLE Filter Msg                  **
 ******************************************************/
int WorkerNode::handle(FilterMsg* msg) {
  logger_->log(LOG_INFO, "Received filter\n");

  executor_->filter(msg->array_name(), msg->expression(), msg->result_array_name());

  logger_->log(LOG_INFO, "Finished filter");

  return 0;
}

/*************** HANDLE AggregateMsg **********************/
int WorkerNode::handle(AggregateMsg* msg) {
  /*
  logger_->log(LOG_INFO, "Received aggregate");
  logger_->log(LOG_INFO, "arrayname: " + msg->array_name());
  std::string global_schema_name = msg->array_name();
  // check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log(LOG_INFO, "Aggregate did not find schema!");
    // TODO move to while loop in run somehow
    //respond_ack(-1, ERROR_TAG, -1); 
    return -1;
  }

  // TODO add back
  //int max = query_processor_->aggregate(*(search->second), msg->attr_index_);
  int max = 0;

  logger_->log(LOG_INFO, "Sending my computed MAX aggregate: " + std::to_string(max));

  std::stringstream content;
  content.write((char *) &max, sizeof(int));
  MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, AGGREGATE_TAG, MPI_COMM_WORLD);

  */
  return 0;
}


/******************************************************
 **                 HANDLE LOAD                      **
 ******************************************************/
int WorkerNode::handle(LoadMsg* msg) {
  logger_->log(LOG_INFO, "Received load");

  MetaData metadata;
  switch (msg->partition_type()) {

    case ORDERED_PARTITION:

      switch(msg->load_method()) {
        case LoadMsg::SORT:
          handle_load_ordered_sort(msg->filename(), msg->array_schema());
          break;
        case LoadMsg::SAMPLE:
          handle_load_ordered_sample(msg->filename(), msg->array_schema());
          break;
        default:
          // shouldn't get here
          break;
      }

      logger_->log(LOG_INFO, "Update Fragment Info");
      executor_->update_fragment_info(msg->array_schema().array_name());
      logger_->log(LOG_INFO, "Finished load");
      break;

    case HASH_PARTITION:
      handle_load_hash(msg->filename(), msg->array_schema());
      logger_->log(LOG_INFO, "Finished load, creating metadata");
      metadata = MetaData(HASH_PARTITION);
      logger_->log(LOG_INFO, "Writing metadata to disk");
      md_manager_->store_metadata(msg->array_schema().array_name(), metadata);
      logger_->log(LOG_INFO, "Finished writing metadata to disk");
      break;
    default:
      // TODO send error
      break;
  }

  logger_->log(LOG_INFO, "Finished handle load");
  return 0;
}

int WorkerNode::handle_load_ordered_sort(std::string filename, ArraySchema& array_schema) {

  logger_->log(LOG_INFO, "In Handle Load Ordered");

  // Wait for sorted file from coordinator
  logger_->log_start(LOG_INFO, "Receiving file and samples from coord");
  std::string frag_name = "0_0";
  std::ofstream sorted_file;

  std::string sorted_filepath = executor_->loader()->workspace() + "/sorted_" + array_schema.array_name() + "_" + frag_name + ".csv";

  sorted_file.open(sorted_filepath.c_str());

  logger_->log(LOG_INFO, "Receiving sorted file from master to " + sorted_filepath);
  mpi_handler_->receive_content(sorted_file, MASTER, LOAD_TAG);
  sorted_file.close();

  logger_->log(LOG_INFO, "Finished receiving sorted file from master to " + sorted_filepath);


  // receive samples from coord
  logger_->log(LOG_INFO, "Receiving partitions from coordinator");
  SamplesMsg* smsg = mpi_handler_->receive_samples_msg(MASTER);
  std::vector<uint64_t> partitions = smsg->samples();
  logger_->log_end(LOG_INFO);

  // store metadata
  logger_->log(LOG_INFO, "Storing metadata");
  MetaData metadata(ORDERED_PARTITION,
      std::pair<uint64_t, uint64_t>(partitions[myrank_-1], partitions[myrank_]),
      partitions);
  md_manager_->store_metadata(array_schema.array_name(), metadata);

  logger_->log(LOG_INFO, "Starting make tiles on " + sorted_filepath);
  StorageManager::FragmentDescriptor* fd =
    executor_->storage_manager()->open_fragment(&array_schema, frag_name,
                                                StorageManager::CREATE);

  logger_->log_start(LOG_INFO, "Make tiles");
  // Make tiles
  try {
    if(array_schema.has_regular_tiles())
      executor_->loader()->make_tiles_regular(sorted_filepath, fd);
    else
      executor_->loader()->make_tiles_irregular(sorted_filepath, fd);
  } catch(LoaderException& le) {

#ifdef NDEBUG
      remove(sorted_filepath.c_str());
      executor_->storage_manager()->delete_fragment(array_schema.array_name(), frag_name);
#endif

    throw LoaderException("Error invoking local load" + filename +
                          "'.\n " + le.what());
  }

  logger_->log(LOG_INFO, "Finished make tiles");
  executor_->storage_manager()->close_fragment(fd);

  logger_->log(LOG_INFO, "Closed fragment");
  logger_->log_end(LOG_INFO);
  // TODO cleanup
  return 0;
}

int WorkerNode::handle_load_ordered_sample(std::string filename, ArraySchema& array_schema) {

  logger_->log(LOG_INFO, "Start Worker Handle Load Ordered Partiion using Sampling");
  
  std::string outpath = executor_->loader()->workspace() + "/ORDERED_SAMPLE_" + filename;

  // receive data
  logger_->log_start(LOG_INFO, "Receiving file partition from master to " + outpath);
  std::ofstream outfile;

  outfile.open(outpath.c_str());
  mpi_handler_->receive_content(outfile, MASTER, LOAD_TAG);
  outfile.close();
  logger_->log_end(LOG_INFO);

  // receive samples from coordinator
  logger_->log_start(LOG_INFO, "Receiving partitions from coordinator");
  SamplesMsg* smsg = mpi_handler_->receive_samples_msg(MASTER);
  std::vector<uint64_t> partitions = smsg->samples();
  logger_->log_end(LOG_INFO);

  // store metadata
  logger_->log(LOG_INFO, "Storing metadata");
  MetaData metadata(ORDERED_PARTITION,
      std::pair<uint64_t, uint64_t>(partitions[myrank_-1], partitions[myrank_]),
      partitions);
  md_manager_->store_metadata(array_schema.array_name(), metadata);

  // sort
  std::string frag_name = "0_0";
  std::string sorted_filepath = executor_->loader()->workspace() + "/ORDERED_SAMPLE_sorted_" + array_schema.array_name() + "_" + frag_name + ".csv";

  logger_->log_start(LOG_INFO, "Sorting csv file " + outpath + " into " + sorted_filepath);
  executor_->loader()->sort_csv_file(outpath, sorted_filepath, array_schema);
  logger_->log_end(LOG_INFO);


  // make tiles
  logger_->log_start(LOG_INFO, "Starting make tiles on " + sorted_filepath);

  StorageManager::FragmentDescriptor* fd =
    executor_->storage_manager()->open_fragment(&array_schema, frag_name,
                                                StorageManager::CREATE);

  try {
    if(array_schema.has_regular_tiles())
      executor_->loader()->make_tiles_regular(sorted_filepath, fd);
    else
      executor_->loader()->make_tiles_irregular(sorted_filepath, fd);
  } catch(LoaderException& le) {

#ifdef NDEBUG
    remove(sorted_filepath.c_str());
#endif

    executor_->storage_manager()->delete_fragment(array_schema.array_name(), frag_name);
    throw LoaderException("Error invoking local load" + filename +
                          "'.\n " + le.what());
  }

  logger_->log_end(LOG_INFO);
  executor_->storage_manager()->close_fragment(fd);

  // cleanup
  delete smsg;
  return 0;
}

int WorkerNode::handle_load_hash(std::string filename, ArraySchema& array_schema) {

  std::ofstream output;
  std::string filepath = executor_->loader()->workspace() + "/HASH_" + filename;

  logger_->log(LOG_INFO, "Receiving hash partitioned data from master, writing to filepath " + filepath);
  logger_->log_start(LOG_INFO, "Receiving data from coord");

  output.open(filepath.c_str());
  // Blocking
  mpi_handler_->receive_content(output, MASTER, LOAD_TAG);
  output.close();
  logger_->log_end(LOG_INFO);

  logger_->log(LOG_INFO, "Invoking local executor load");
  logger_->log_start(LOG_INFO, "Local load");
  executor_->load(filepath, array_schema.array_name());
  logger_->log_end(LOG_INFO);

  // TODO cleanup
  return 0;

}

/******************************************************
 **             HANDLE PARALLEL LOAD                 **
 ******************************************************/
int WorkerNode::handle(ParallelLoadMsg* msg) {
  logger_->log(LOG_INFO, "Received Parallel Load Message");
  logger_->log(LOG_INFO, "Filename: " + msg->filename());

  MetaData metadata;
  switch (msg->partition_type()) {
    case ORDERED_PARTITION:
      handle_parallel_load_ordered(msg->filename(), msg->array_schema(), msg->num_samples());
      logger_->log(LOG_INFO, "Update Fragment Info");
      executor_->update_fragment_info(msg->array_schema().array_name());
      logger_->log(LOG_INFO, "Finished load");
      break;
    case HASH_PARTITION:
      handle_parallel_load_hash(msg->filename(), msg->array_schema());
      logger_->log(LOG_INFO, "Finished load, creating metadata");
      metadata = MetaData(HASH_PARTITION);
      logger_->log(LOG_INFO, "Writing metadata to disk");
      md_manager_->store_metadata(msg->array_schema().array_name(), metadata);
      break;
    default:
      // TODO send error
      break;
  }

  // TODO cleanup
  return 0;
}

int WorkerNode::handle_parallel_load_ordered(std::string filename, ArraySchema& array_schema, int num_samples) {
  logger_->log(LOG_INFO, "Handle parallel load ordered");

  std::string filepath = get_data_path(filename);

  // inject cell_ids
  logger_->log_start(LOG_INFO, "Inject cell ids and resevoir sample");
  CSVFile csv_in(filepath, CSVFile::READ);
  CSVLine line_in, line_out;
  ArraySchema::Order order = array_schema.order();

  std::string frag_name = "0_0";

  // TODO support other orderings later
  assert(array_schema.has_regular_tiles() || 
         array_schema.order() == ArraySchema::HILBERT);

  std::string injected_filepath = executor_->loader()->workspace() + "/injected_" + array_schema.array_name() + "_" + frag_name + ".csv";

  CSVFile *csv_out = new CSVFile(injected_filepath, CSVFile::WRITE);

  uint64_t cell_id;
  std::vector<double> coordinates;
  unsigned int dim_num = array_schema.dim_num();
  coordinates.resize(dim_num);
  double coordinate;
  int resevoir_count = 0;
  std::vector<uint64_t> samples;

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
        int r = (rand() % resevoir_count) + 1; // 0 to counter inclusive // TODO double check
        if (r < num_samples) {
          samples[r] = cell_id;
        }
    }

    resevoir_count++;
  }

  delete csv_out;

  logger_->log_end(LOG_INFO);

  // send samples to coordinator
  logger_->log_start(LOG_INFO, "Send samples to coordinator");
  SamplesMsg msg(samples);
  mpi_handler_->send_samples_msg(&msg, MASTER);
  logger_->log_end(LOG_INFO);

  // receive partitions from coordinator
  logger_->log_start(LOG_INFO, "Receiving partitions from coordinator");
  SamplesMsg* smsg = mpi_handler_->receive_samples_msg(MASTER);
  std::vector<uint64_t> partitions = smsg->samples();
  logger_->log_end(LOG_INFO);

  logger_->log_start(LOG_INFO, "N to N data shuffle");
  std::string outpath = executor_->loader()->workspace() + "/PORDERED_" + filename;
  std::ofstream outfile;
  outfile.open(outpath.c_str());
  CSVFile csv_injected(injected_filepath, CSVFile::READ);
  CSVLine csv_line;
  while (csv_injected >> csv_line) {
    uint64_t cell_id = std::strtoull(csv_line.values()[0].c_str(), NULL, 10);
    int receiver = get_receiver(partitions, cell_id);
    std::string csv_line_str = csv_line.str() + "\n";
    if (receiver == myrank_) {
      outfile << csv_line_str;
    } else {
      mpi_handler_->send_and_receive_a2a(csv_line_str.c_str(), csv_line_str.size(), receiver, outfile);
    }
  }

  if (!mpi_handler_->all_buffers_empty()) {
    logger_->log(LOG_INFO, "Have non empty buffers, flush_send_and_recv_a2a");
    mpi_handler_->flush_send_and_recv_a2a(outfile);
  } else {
    logger_->log(LOG_INFO, "All buffers empty, no flush");
  }

  // keep receiving until everyone finishes sending
  logger_->log(LOG_INFO, "Keep receiving from everyone");
  mpi_handler_->finish_recv_a2a(outfile);
  outfile.close();
  logger_->log(LOG_INFO, "Finished receiving all necessary data");
  logger_->log_end(LOG_INFO);

  // sort and make tiles
  std::string sorted_filepath = executor_->loader()->workspace() + "/PORDERED_sorted_" + array_schema.array_name() + "_" + frag_name + ".csv";

  logger_->log_start(LOG_INFO, "Sorting csv file " + outpath + " into " + sorted_filepath);
  executor_->loader()->sort_csv_file(outpath, sorted_filepath, array_schema);
  logger_->log_end(LOG_INFO);

  // Make tiles
  logger_->log_start(LOG_INFO, "Starting make tiles on " + sorted_filepath);
  StorageManager::FragmentDescriptor* fd =
    executor_->storage_manager()->open_fragment(&array_schema, frag_name,
                                                StorageManager::CREATE);

  try {
    if(array_schema.has_regular_tiles())
      executor_->loader()->make_tiles_regular(sorted_filepath, fd);
    else
      executor_->loader()->make_tiles_irregular(sorted_filepath, fd);
  } catch(LoaderException& le) {

#ifdef NDEBUG
    remove(sorted_filepath.c_str());
#endif

    executor_->storage_manager()->delete_fragment(array_schema.array_name(), frag_name);
    throw LoaderException("Error invoking local load" + filename +
                          "'.\n " + le.what());
  }

  logger_->log_end(LOG_INFO);
  executor_->storage_manager()->close_fragment(fd);

  // set metadata
  logger_->log_start(LOG_INFO, "Storing metadata");
  MetaData metadata(ORDERED_PARTITION,
      std::pair<uint64_t, uint64_t>(partitions[myrank_-1], partitions[myrank_]),
      partitions);
  md_manager_->store_metadata(array_schema.array_name(), metadata);
  logger_->log_end(LOG_INFO);

  // cleanup
  delete smsg;

  return 0;
}

int WorkerNode::handle_parallel_load_hash(std::string filename, ArraySchema& array_schema) {

  logger_->log(LOG_INFO, "Handle parallel load hash array : " + array_schema.array_name());
  logger_->log_start(LOG_INFO, "Hashing + Data Shuffle");
  std::string filepath = get_data_path(filename);
  std::string outpath = executor_->loader()->workspace() + "/PHASH_" + filename;

  std::ofstream outfile;
  outfile.open(outpath.c_str());

  // scan csv file, compute hash, asynchronous send to receivers
  CSVFile csv_in(filepath, CSVFile::READ);
  CSVLine csv_line;

  int nworkers = nprocs_ - 1;
  std::map<int, std::string> chunk_map;
  // scan data to compute meta data (how many chunks to send to each node)
  std::stringstream ss;
  while (csv_in >> csv_line) {
    // TODO look into other hash fns, using default for strings right now
    std::string coord_id = csv_line.values()[0];
    for (int i = 1; i < array_schema.dim_num(); ++i) {
      coord_id += ",";
      coord_id += csv_line.values()[i];
    }
    std::size_t cell_id_hash = DEKHash(coord_id);
    int receiver = (cell_id_hash % nworkers) + 1;
    std::string csv_line_str = csv_line.str() + "\n";

    if (receiver == myrank_) {
      outfile << csv_line_str;
    } else {
      mpi_handler_->send_and_receive_a2a(csv_line_str.c_str(), csv_line_str.size(), receiver, outfile);
    }

  }

  logger_->log(LOG_INFO, "Flushing All to All send and receive");
  mpi_handler_->flush_send_and_recv_a2a(outfile);

  // keep receiving until everyone finishes sending
  logger_->log(LOG_INFO, "Finish receiving from everyone");
  mpi_handler_->finish_recv_a2a(outfile);
  outfile.close();


  logger_->log_end(LOG_INFO);
  // invoke local load on received data
  logger_->log(LOG_INFO, "Invoking local executor load filename: " + outpath + " array_name: " + array_schema.array_name());

  logger_->log(LOG_INFO, "Local Load");
  executor_->load(outpath, array_schema.array_name());

  logger_->log_end(LOG_INFO);
}

/******************************************************
 **                  HANDLE JOIN                     **
 ******************************************************/
int WorkerNode::handle(JoinMsg* msg) {
  logger_->log(LOG_INFO, "Received join query");

  MetaData *md_A = md_manager_->retrieve_metadata(msg->array_name_A());
  MetaData *md_B = md_manager_->retrieve_metadata(msg->array_name_B());

  assert(md_A->partition_type() == md_B->partition_type());

  MetaData metadata;
  switch (md_A->partition_type()) {
    case ORDERED_PARTITION:
      handle_join_ordered(msg->array_name_A(),
          msg->array_name_B(),
          msg->result_array_name());

      break;
    case HASH_PARTITION:
      logger_->log_start(LOG_INFO, "Join hash partition on " + msg->array_name_A() + " and " + msg->array_name_B()); 
      executor_->join(msg->array_name_A(), msg->array_name_B(), msg->result_array_name());
      logger_->log_end(LOG_INFO);

      logger_->log_start(LOG_INFO, "Write metadata to disk");
      metadata = MetaData(HASH_PARTITION);
      md_manager_->store_metadata(msg->result_array_name(), metadata);
      logger_->log_end(LOG_INFO);
      break;
    default:
      // shouldn't get here
      break;
  }

  // cleanup
  delete md_A;
  delete md_B;


}

int WorkerNode::handle_join_ordered(std::string array_name_A,
    std::string array_name_B,
    std::string result_array_name) {
  logger_->log(LOG_INFO, "In handle join ordered, joining " + array_name_A + " and " + array_name_B + " into " + result_array_name);

  // Check that arrays A and B exist but result doesn't
  assert(executor_->storage_manager()->array_defined(array_name_A));
  assert(executor_->storage_manager()->array_defined(array_name_B));
  assert(!executor_->storage_manager()->array_defined(result_array_name));


  // Get fragment names
  // TODO only single fragment supported now
  std::vector<std::string> frag_names_A;
  frag_names_A.push_back("0_0");
  std::vector<std::string> frag_names_B;
  frag_names_B.push_back("0_0");

  // Open arrays
  StorageManager::ArrayDescriptor* ad_A =
      executor_->storage_manager()->open_array(array_name_A, 
                                   frag_names_A,
                                   StorageManager::READ);

  StorageManager::ArrayDescriptor* ad_B =
      executor_->storage_manager()->open_array(array_name_B, 
                                   frag_names_B,
                                   StorageManager::READ);
  
  // Get fragments for array A and array B
  // TODO add error handling
  assert(frag_names_A.size() == 1 && frag_names_B.size() == 1);
  const StorageManager::FragmentDescriptor* fd_A = ad_A->fd()[0]; 
  const StorageManager::FragmentDescriptor* fd_B = ad_B->fd()[0]; 
  const ArraySchema& array_schema_A = *(fd_A->fragment_info()->array_schema_);
  const ArraySchema& array_schema_B = *(fd_B->fragment_info()->array_schema_);


  // assumes all bounding coordinates fit in memory
  // N to N sending bounding coordinates for array A
  BoundingCoordsMsg bcm_A(fd_A->fragment_info()->bounding_coordinates_);
  
#ifdef DEBUG
  logger_->log(LOG_INFO, "My bounding coords A: " + util::to_string(fd_A->fragment_info()->bounding_coordinates_));
#endif
  logger_->log_start(LOG_INFO, "Sending and receiving bounding coords of " + array_name_A + " to everyone");
  std::vector<std::ostream *> rstreams;
  for (int i = 0; i < nprocs_; ++i) {
    std::stringstream *ss = new std::stringstream();
    rstreams.push_back(ss);
  }

  mpi_handler_->send_and_receive_a2a(bcm_A, rstreams);
  mpi_handler_->flush_send_and_recv_a2a(rstreams);
  mpi_handler_->finish_recv_a2a(rstreams);
  logger_->log_end(LOG_INFO);


  std::map<int, BoundingCoordsMsg *> bc_msgs_A;
  for (int i = 1; i < rstreams.size(); ++i) {
    // skip myself
    if (i == myrank_) {
      continue;
    }

    logger_->log(LOG_INFO, "assembling bcmsg for node " + util::to_string(i));
    std::string s = ((std::stringstream *)rstreams[i])->str();
    BoundingCoordsMsg *bcm = BoundingCoordsMsg::deserialize((char *)s.c_str(), s.size());

    logger_->log(LOG_INFO, "Created boundingCoordsMsg for node " + util::to_string(i));
    bc_msgs_A[i] = bcm;
  }

  // N to N sending bounding coordinates for array B
  BoundingCoordsMsg bcm_B(fd_B->fragment_info()->bounding_coordinates_);
#ifdef DEBUG
  logger_->log(LOG_INFO, "My bounding coords B: " + util::to_string(fd_B->fragment_info()->bounding_coordinates_));
#endif
  logger_->log_start(LOG_INFO, "Send and recv bounding coords of " + array_name_B);
  // reuse rstreams b/c memory is copied into the BCMsg
  for (int i = 0; i < nprocs_; ++i) {
    delete rstreams[i];
  }
  rstreams.clear();
  assert(rstreams.size() == 0);

  for (int i = 0; i < nprocs_; ++i) {
    std::stringstream *ss = new std::stringstream();
    rstreams.push_back(ss);
  }

  mpi_handler_->send_and_receive_a2a(bcm_B, rstreams);
  mpi_handler_->flush_send_and_recv_a2a(rstreams);
  mpi_handler_->finish_recv_a2a(rstreams);
  logger_->log_end(LOG_INFO);

  // maps worker id to bc_msg ptr
  std::map<int, BoundingCoordsMsg *> bc_msgs_B;
  for (int i = 1; i < rstreams.size(); ++i) {
    // skip myself
    if (i == myrank_) {
      continue;
    }
    logger_->log(LOG_INFO, "assembling bcmsg for node " + util::to_string(i));
    std::string s = ((std::stringstream *)rstreams[i])->str();
    BoundingCoordsMsg *bcm = BoundingCoordsMsg::deserialize((char *)s.c_str(), s.size());

    logger_->log(LOG_INFO, "Created boundingCoordsMsg for node " + util::to_string(i));
    bc_msgs_B[i] = bcm;
  }


  // TODO add multiplying by capacity later
  // TODO maybe use mbrs if there is a tie?
  // number of tiles in my A partition that overlap with the partition
  // boundaries of B
  // rows: my array A partition, col: the partition boundary i'm comparing with
  uint64_t costs_A_to_B[nworkers_][nworkers_];

  // number of tiles in my B partition that overlap with the partition
  // boundaries of A
  // rows: my array B partitions, col: the partition boundary i'm comparing with
  uint64_t costs_B_to_A[nworkers_][nworkers_];

  // my tile ranks from array B that overlap with partition boundaries in array B from other workers
  std::map<int, std::vector<uint64_t> > my_overlap_tiles_A;
  // my tile ranks from array B that overlap with partition boundaries in array A from other workers
  std::map<int, std::vector<uint64_t> > my_overlap_tiles_B;

  //logger_->log(LOG_INFO, "Initializing cost matrix");
  // initialize costs matrix
  for (int i = 0; i < nworkers_; ++i) {
    for (int j = i; j < nworkers_; ++j) {
      costs_A_to_B[i][j] = 0;
      costs_A_to_B[j][i] = 0;
      costs_B_to_A[i][j] = 0;
      costs_B_to_A[j][i] = 0;
    }
  }

  //logger_->log(LOG_INFO, "Computing data shuffle costs");
  StorageManager::BoundingCoordinates bounding_coords_A;
  StorageManager::BoundingCoordinates bounding_coords_B;

  for (int i = 0; i < nworkers_; ++i) {
    int node1 = i + 1;
    for (int j = 0; j < nworkers_; ++j) {
      int node2 = j + 1;
      // don't compare with yourself
      if (node1 == node2) {
        continue;
      }

      if (node1 == myrank_) {
        bounding_coords_A = bcm_A.bounding_coordinates();
      } else {
        bounding_coords_A = bc_msgs_A[node1]->bounding_coordinates();
      }

      if (node2 == myrank_) {
        bounding_coords_B = bcm_B.bounding_coordinates();
      } else {
        bounding_coords_B = bc_msgs_B[node2]->bounding_coordinates();
      }

      std::pair<std::vector<uint64_t> , std::vector<uint64_t> > overlap_tiles = get_overlapping_tile_ranks(
          bounding_coords_A,
          bounding_coords_B,
          *(fd_A->fragment_info()->array_schema_),
          *(fd_B->fragment_info()->array_schema_));


      // TODO other cost models, such as multiplying by capacity
      // assumes same tile capacity for now
      costs_A_to_B[i][j] = (uint64_t)overlap_tiles.first.size(); // ranks from array A

      costs_B_to_A[j][i] = (uint64_t)overlap_tiles.second.size(); // ranks from array B in node j that overlap with boundaries in node i


      if (node1 == myrank_) {
        my_overlap_tiles_A[node2] = overlap_tiles.first;
      }
      
      if (node2 == myrank_) {
        my_overlap_tiles_B[node1] = overlap_tiles.second;
      }

    }
  }

  // TODO move to helper functions, ugh
#ifdef DEBUG
  std::stringstream ssf;
  std::stringstream ssh;
  ssf << "{\n";
  ssh << "{\n";
  for (int i = 0; i < nworkers_; ++i) {
    ssf << "[";
    ssh << "[";
    ssf << costs_A_to_B[i][0];
    ssh << costs_B_to_A[i][0];
    for (int j = 1; j < nworkers_; ++j) {
      ssf << ", " << costs_A_to_B[i][j];
      ssh << ", " << costs_B_to_A[i][j];
    }
    ssf << "]\n";
    ssh << "]\n";
  }
  ssf << "}";
  ssh << "}";

  logger_->log(LOG_INFO, "Costs_A_to_B (num of tiles in array A that overlap with partition boundaries in array B): " + ssf.str());
  logger_->log(LOG_INFO, "Costs_B_to_A (num of tiles in array B that overlap with partition boundaries in array A): " + ssh.str());


  for (std::map<int, std::vector<uint64_t> >::iterator it = my_overlap_tiles_A.begin(); it != my_overlap_tiles_A.end(); ++it) {
    assert(it->first != myrank_);
    logger_->log(LOG_INFO, "Overlaps from my array A that overlap with partition boundaries of array B to node " + util::to_string(it->first) + ": " + util::to_string(it->second));
  }

  for (std::map<int, std::vector<uint64_t> >::iterator it = my_overlap_tiles_B.begin(); it != my_overlap_tiles_B.end(); ++it) {
    assert(it->first != myrank_);
    logger_->log(LOG_INFO, "Overlaps from my array B that overlap with partition boundaries of array A to node " + util::to_string(it->first) + ": " + util::to_string(it->second));
  }

#endif

  std::map<int, std::vector<uint64_t> > to_send_tile_ranks_A;
  std::map<int, std::vector<uint64_t> > to_send_tile_ranks_B;

  // compare cost matrix, lower cost sends,
  // use myrank_ as tie breaker
  // filter my_overlap_tiles_A and B to the ones that I actually have to send
  int my_ind = myrank_ - 1;
  for (int j = 0; j < nworkers_; ++j) {
    // don't send to myself
    if (my_ind == j) {
      continue;
    }
    // don't send if no overlap
    if ((costs_A_to_B[my_ind][j] == 0 &&
        costs_B_to_A[j][my_ind] == 0) &&
        (costs_A_to_B[j][my_ind] == 0 &&
        costs_B_to_A[my_ind][j] == 0)) {

      continue;
    }

    int receiver = j + 1;
    // comparing my tile A overlap with their B boundary, so if overlap, might
    // send array A over
    if (costs_A_to_B[my_ind][j] < costs_B_to_A[j][my_ind]) {
      assert(costs_B_to_A[my_ind][j] == 0);
      // I send array A join fragments to node j + 1
      to_send_tile_ranks_A[receiver] = my_overlap_tiles_A[receiver];

    } else if (costs_A_to_B[my_ind][j] > costs_B_to_A[j][my_ind]) {
      // They send me array B join fragments

    } else {
      // equal, if there is overlap and equal cost, use myrank as tiebreaker
      if (costs_A_to_B[my_ind][j] > 0) {
        assert(costs_B_to_A[my_ind][j] == 0);
        if (my_ind < j) { // I send A join fragments to node j + 1

          to_send_tile_ranks_A[receiver] = my_overlap_tiles_A[receiver];
        } else {
          // They send me array B join fragments
        }
      }

    }


    // comparing my tile B overlap with their A boundary, should be mutually
    // exclusive from the above one, so if one is positive, the other is 0
    // if overlap, send array B
    if (costs_B_to_A[my_ind][j] < costs_A_to_B[j][my_ind]) {
      // I send array B join fragments to node j + 1
      assert(costs_A_to_B[my_ind][j] == 0);
      to_send_tile_ranks_B[receiver] = my_overlap_tiles_B[receiver];
    } else if (costs_B_to_A[my_ind][j] > costs_A_to_B[j][my_ind]) {
      // They send me array A join fragments

    } else {
      // equal, if there is overlap and equal cost, use myrank as tiebreaker
      if (costs_B_to_A[my_ind][j] > 0 ) {

        assert(costs_A_to_B[my_ind][j] == 0);
        if (my_ind < j) { // I send B join fragments to node j + 1
          to_send_tile_ranks_B[receiver] = my_overlap_tiles_B[receiver];
        } else {
          // They send me array A join fragments
        }
      }
    }

  }

  logger_->log(LOG_INFO, "Finished gathering overlapping tiles for sending");

  for (std::map<int, std::vector<uint64_t> >::iterator it = to_send_tile_ranks_A.begin(); it != to_send_tile_ranks_A.end(); ++it) {
    assert(it->first != myrank_);
    logger_->log(LOG_INFO, "I'm sending these tile ranks from array A that overlap with partition boundaries of array B to node " + util::to_string(it->first) + ": " + util::to_string(it->second));
  }

  if (to_send_tile_ranks_A.size() == 0) {
    logger_->log(LOG_INFO, "No array A join fragments to send");
  }

  for (std::map<int, std::vector<uint64_t> >::iterator it = to_send_tile_ranks_B.begin(); it != to_send_tile_ranks_B.end(); ++it) {
    assert(it->first != myrank_);
    logger_->log(LOG_INFO, "I'm sending these tile ranks from array B that overlap with partition boundaries of array A to node " + util::to_string(it->first) + ": " + util::to_string(it->second));
  }

  if (to_send_tile_ranks_B.size() == 0) {
    logger_->log(LOG_INFO, "No array B join fragments to send");
  }

  // Sending array A fragments over the network
  int num_attr_A = fd_A->fragment_info()->array_schema_->attribute_num();

  // holds one logical tile when reading from local array A
  const Tile** tiles_A = new const Tile*[num_attr_A+1];

  // holds one logical tile per sender, populated by mpi_handler function
  Tile*** received_tiles_A = new Tile**[nprocs_];
  bool *init_received_tiles_A = new bool[nprocs_];

  // vector of tiles from each sender, this is necessary because the received
  // tiles need to be in sorted order at the end
  std::vector<Tile** > **arr_received_tiles_A = new std::vector<Tile **>*[nprocs_];
  for (int i = 0; i < nprocs_; ++i) {
    arr_received_tiles_A[i] = new std::vector<Tile**>();
  }

  // holds current rank of received tile
  uint64_t *start_ranks_A = new uint64_t[nprocs_];
  for (int i = 0; i < nprocs_; ++i) {
    start_ranks_A[i] = 0;
    init_received_tiles_A[i] = false;
  }

  // for each receiver
  for(std::map<int, std::vector<uint64_t> >::iterator it_rank_A = to_send_tile_ranks_A.begin(); 
      it_rank_A != to_send_tile_ranks_A.end(); ++it_rank_A) {

    int receiver = it_rank_A->first;

    logger_->log(LOG_INFO, "Sending tiles to receiver: " + util::to_string(receiver));
    // for each logical tile rank
    for (std::vector<uint64_t>::iterator it_r = it_rank_A->second.begin();
         it_r != it_rank_A->second.end(); ++it_r) {

      uint64_t rank = *it_r;

      Tile** r_tile;
      bool tile_initialized = false;

      // sending (and possibly receiving) one physical tile
      // All sending and receiving should be happening in lock step for every
      // physical tile.
      for (int i = 0; i <= num_attr_A; ++i) {
        // read one physical tile from
        tiles_A[i] = executor_->storage_manager()->get_tile_by_rank(fd_A, i, rank);

#ifdef DEBUG
        if (i == num_attr_A) {
          std::pair<uint64_t, uint64_t> fl_pair =
            std::pair<uint64_t, uint64_t>(
            array_schema_A.cell_id_hilbert(tiles_A[i]->bounding_coordinates().first),
            array_schema_A.cell_id_hilbert(tiles_A[i]->bounding_coordinates().second));
          assert(fl_pair.first <= fl_pair.second);
        }
#endif

        // construct tile msg
        logger_->log(LOG_INFO, "Sending payload of size " + util::to_string(tiles_A[i]->tile_size()) + " to receiver " + util::to_string(receiver));

        // deleted inside TileMsg destructor
        char *payload = new char[tiles_A[i]->tile_size()];
        tiles_A[i]->copy_payload(payload);
        TileMsg msg(array_name_A, i, payload, tiles_A[i]->cell_num(), tiles_A[i]->cell_size());
        std::pair<char *, uint64_t> buf_pair = msg.serialize();

        assert(msg.attr_id() == i);

        // send and flush to network
        mpi_handler_->send_and_recv_tiles_a2a(buf_pair.first, buf_pair.second,
            receiver,
            received_tiles_A,
            num_attr_A,
            executor_,
            *(fd_A->fragment_info()->array_schema_),
            start_ranks_A,
            init_received_tiles_A);

      }


      logger_->log(LOG_INFO, "Finished sending one logical tile to receiver: " + util::to_string(receiver));
      for (int sender = 1; sender < nprocs_; ++sender) {
        if (sender == myrank_) {
          continue;
        }

        if (init_received_tiles_A[sender]) {
          logger_->log(LOG_INFO, "Appending full logical tile this round from sender " + util::to_string(sender));
          arr_received_tiles_A[sender]->push_back(received_tiles_A[sender]);
          start_ranks_A[sender] = start_ranks_A[sender] + 1;
        }

      }

      // reset for next round
      for (int i = 0; i < nprocs_; ++i) {
        init_received_tiles_A[i] = false;
      }

    }

  }

  mpi_handler_->finish_recv_a2a(arr_received_tiles_A, num_attr_A, executor_,
      *(fd_A->fragment_info()->array_schema_),
      fd_A->fragment_info()->array_schema_->capacity(),
      start_ranks_A);

#ifdef DEBUG

  for (int i = 1; i < nprocs_; ++i) {
    if (i == myrank_) {
      assert(arr_received_tiles_A[i]->size() == 0);
      continue;
    }

    if (arr_received_tiles_A[i]->size() == 0) {
      logger_->log(LOG_INFO, "DID NOT Received tile payloads for array B from node " + util::to_string(i));
    } else {
      logger_->log(LOG_INFO, "DID Received tile payloads for array B from node " + util::to_string(i));
      for (std::vector<Tile** >::iterator it = (*arr_received_tiles_A[i]).begin();
          it != (*arr_received_tiles_A[i]).end(); ++it) {

        for (int i = 0; i <= num_attr_A; ++i) {
          logger_->log(LOG_INFO, "Received tile id: " + util::to_string(((*it)[i])->tile_id()) + " attr id: " + util::to_string(i) + " tile size: " + util::to_string(((*it)[i])->tile_size()) + " cell num: " + util::to_string(((*it)[i])->cell_num()));
          //logger_->log(LOG_INFO, ((*it)[i])->to_string());
        }

        logger_->log(LOG_INFO, "bounding coordinates: " + util::to_string(((*it)[num_attr_A])->bounding_coordinates().first));

      }
    }
  }
#endif


  // receive array B join fragments

  // Sending array B fragments over the network
  int num_attr_B = fd_B->fragment_info()->array_schema_->attribute_num();

  // holds one logical tile when reading from local array A
  const Tile** tiles_B = new const Tile*[num_attr_B+1];

  // holds one logical tile per sender, populated by mpi_handler function
  Tile*** received_tiles_B = new Tile**[nprocs_];
  bool *init_received_tiles_B = new bool[nprocs_];

  // vector of tiles from each sender, this is necessary because the received
  // tiles need to be in sorted order at the end
  std::vector<Tile** > **arr_received_tiles_B = new std::vector<Tile **>*[nprocs_];
  for (int i = 0; i < nprocs_; ++i) {
    arr_received_tiles_B[i] = new std::vector<Tile**>();
  }


  // holds current rank of received tile
  uint64_t *start_ranks_B = new uint64_t[nprocs_];
  for (int i = 0; i < nprocs_; ++i) {
    start_ranks_B[i] = 0;
    init_received_tiles_B[i] = false;
  }

  // for each receiver
  for(std::map<int, std::vector<uint64_t> >::iterator it_rank_B = to_send_tile_ranks_B.begin(); 
      it_rank_B != to_send_tile_ranks_B.end(); ++it_rank_B) {

    int receiver = it_rank_B->first;

    logger_->log(LOG_INFO, "Sending tiles to receiver: " + util::to_string(receiver));
    // for each logical tile rank
    for (std::vector<uint64_t>::iterator it_r = it_rank_B->second.begin();
         it_r != it_rank_B->second.end(); ++it_r) {

      uint64_t rank = *it_r;

      Tile** r_tile;
      bool tile_initialized = false;

      // sending (and possibly receiving) one physical tile
      // All sending and receiving should be happening in lock step for every
      // physical tile.
      for (int i = 0; i <= num_attr_B; ++i) {
        // read one physical tile from
        tiles_B[i] = executor_->storage_manager()->get_tile_by_rank(fd_B, i, rank);


#ifdef DEBUG
        if (i == num_attr_B) {
          std::pair<uint64_t, uint64_t> fl_pair =
            std::pair<uint64_t, uint64_t>(
            array_schema_B.cell_id_hilbert(tiles_B[i]->bounding_coordinates().first),
            array_schema_B.cell_id_hilbert(tiles_B[i]->bounding_coordinates().second));
          assert(fl_pair.first <= fl_pair.second);
        }
#endif

        // construct tile msg
        char *payload = new char[tiles_B[i]->tile_size()];
        tiles_B[i]->copy_payload(payload);
        TileMsg msg(array_name_B, i, payload, tiles_B[i]->cell_num(), tiles_B[i]->cell_size());
        std::pair<char *, uint64_t> buf_pair = msg.serialize();

        assert(msg.attr_id() == i);

        // send and flush to network
        mpi_handler_->send_and_recv_tiles_a2a(buf_pair.first, buf_pair.second,
            receiver,
            received_tiles_B,
            num_attr_B,
            executor_,
            *(fd_B->fragment_info()->array_schema_),
            start_ranks_B,
            init_received_tiles_B);

      }

#ifdef DEBUG
      logger_->log(LOG_INFO, "Finished sending one logical tile to receiver: " + util::to_string(receiver));
#endif
      // push receivied tiles into all_received_tiles_B
      for (int sender = 1; sender < nprocs_; ++sender) {
        if (sender == myrank_) {
          continue;
        }

        if (init_received_tiles_B[sender]) {
#ifdef DEBUG
          logger_->log(LOG_INFO, "Appending full logical tile this round from sender " + util::to_string(sender));
#endif
          arr_received_tiles_B[sender]->push_back(received_tiles_B[sender]);
          start_ranks_B[sender] = start_ranks_B[sender] + 1;
        }

      }

      // reset for next round
      for (int i = 0; i < nprocs_; ++i) {
        init_received_tiles_B[i] = false;
      }

    }

  }

  logger_->log(LOG_INFO, "Finish receiving tiles");
  mpi_handler_->finish_recv_a2a(arr_received_tiles_B, num_attr_B, executor_,
      *(fd_B->fragment_info()->array_schema_),
      fd_B->fragment_info()->array_schema_->capacity(),
      start_ranks_B);

#ifdef DEBUG

  for (int i = 1; i < nprocs_; ++i) {
    if (i == myrank_) {
      assert(arr_received_tiles_B[i]->size() == 0);
      continue;
    }

    if (arr_received_tiles_B[i]->size() == 0) {
      logger_->log(LOG_INFO, "DID NOT Received tile payloads for array B from node " + util::to_string(i));
    } else {
      logger_->log(LOG_INFO, "DID Received tile payloads for array B from node " + util::to_string(i));
      for (std::vector<Tile** >::iterator it = (*arr_received_tiles_B[i]).begin();
          it != (*arr_received_tiles_B[i]).end(); ++it) {

        for (int i = 0; i <= num_attr_B; ++i) {
          logger_->log(LOG_INFO, "Received tile id: " + util::to_string(((*it)[i])->tile_id()) + " attr id: " + util::to_string(i) + " tile size: " + util::to_string(((*it)[i])->tile_size()) + " cell num: " + util::to_string(((*it)[i])->cell_num()));
          //logger_->log(LOG_INFO, ((*it)[i])->to_string());
        }

        logger_->log(LOG_INFO, "bounding coordinates: " + util::to_string(((*it)[num_attr_B])->bounding_coordinates().first));

      }
    }
  }
#endif

  std::vector<Tile**> *all_precedes_local_A = new std::vector<Tile**>();
  std::vector<Tile**> *all_succeeds_local_A = new std::vector<Tile**>();
  std::vector<Tile**> *all_precedes_local_B = new std::vector<Tile**>();
  std::vector<Tile**> *all_succeeds_local_B = new std::vector<Tile**>();

  for (int i = 1; i < nprocs_; ++i) {
    if (i == myrank_) {
      assert(arr_received_tiles_B[i]->size() == 0);
      continue;
    }

    if (arr_received_tiles_A[i]->size() > 0) {
      bool precedence = get_precedence(arr_received_tiles_A[i],
          num_attr_A, bcm_A.bounding_coordinates(), array_schema_A);

      if (precedence) {
        //precedes_local_A = true;
        for (std::vector<Tile** >::iterator it = (*arr_received_tiles_A[i]).begin();
          it != (*arr_received_tiles_A[i]).end(); ++it) {

          all_precedes_local_A->push_back(*it);
        }
      } else {
        //succeeds_local_A = true;
        for (std::vector<Tile** >::iterator it = (*arr_received_tiles_A[i]).begin();
          it != (*arr_received_tiles_A[i]).end(); ++it) {
          all_succeeds_local_A->push_back(*it);
        }
      }
    }
  }


  for (int i = 1; i < nprocs_; ++i) {
    if (i == myrank_) {
      assert(arr_received_tiles_B[i]->size() == 0);
      continue;
    }

    if (arr_received_tiles_B[i]->size() > 0) {
      bool precedence = get_precedence(arr_received_tiles_B[i],
          num_attr_B,
          bcm_B.bounding_coordinates(),
          array_schema_B);
      if (precedence) {
        //precedes_local_B = true;
        for (std::vector<Tile** >::iterator it = (*arr_received_tiles_B[i]).begin();
          it != (*arr_received_tiles_B[i]).end(); ++it) {

          all_precedes_local_B->push_back(*it);

        }
      } else {
        //succeeds_local_B = true;
        for (std::vector<Tile** >::iterator it = (*arr_received_tiles_B[i]).begin();
          it != (*arr_received_tiles_B[i]).end(); ++it) {

          all_succeeds_local_B->push_back(*it);
        }
      }
    }
  }

  // check received tiles A either precede or succeed local array A partition
  // TODO add assertions

#ifdef DEBUG
  logger_->log(LOG_INFO, "num tiles precedes local A: " + util::to_string(all_precedes_local_A->size()));
  logger_->log(LOG_INFO, "num tiles succeeds_local_A: " + util::to_string(all_succeeds_local_A->size()));
  logger_->log(LOG_INFO, "num tiles precedes_local_B: " + util::to_string(all_precedes_local_B->size()));
  logger_->log(LOG_INFO, "num tiles succeeds_local_B: " + util::to_string(all_succeeds_local_A->size()));
#endif 
  

  // Define the result array
  ArraySchema result_array_schema = 
      ArraySchema::create_join_result_schema(array_schema_A, // array_schema_A
                                             array_schema_B, // array_schema_B
                                             result_array_name);

  executor_->storage_manager()->define_array(result_array_schema);

  // Create the result array
  const StorageManager::FragmentDescriptor* result_fd = 
      executor_->storage_manager()->open_fragment(&result_array_schema,  "0_0", 
                                      StorageManager::CREATE);


  logger_->log_start(LOG_INFO, "Do local join with the extra tiles");
  executor_->query_processor()->join_irregular_with_extra_tiles(
          all_precedes_local_A, all_succeeds_local_A,
          all_precedes_local_B, all_succeeds_local_B,
          fd_A, fd_B, result_fd);
  logger_->log_end(LOG_INFO);

  // Clean up
  executor_->storage_manager()->close_array(ad_A);
  executor_->storage_manager()->close_array(ad_B);
  executor_->storage_manager()->close_fragment(result_fd);

  // Update the fragment information of result array at the consolidator
  executor_->update_fragment_info(result_array_name);


  // CLEANUP
  for (int i = 0; i < nprocs_; ++i) {
    delete rstreams[i];
  }

  for (std::map<int, BoundingCoordsMsg *>::iterator it = bc_msgs_A.begin();
      it != bc_msgs_A.end(); ++it) {
    delete it->second;
  }
  for (std::map<int, BoundingCoordsMsg *>::iterator it = bc_msgs_B.begin();
      it != bc_msgs_B.end(); ++it) {
    delete it->second;
  }

  delete [] tiles_A;
  delete [] received_tiles_A;
  delete [] init_received_tiles_A;
  delete [] start_ranks_A;


  delete [] tiles_B;
  delete [] received_tiles_B;
  delete [] init_received_tiles_B;
  delete [] start_ranks_B;

  for (int i = 0; i < nprocs_; ++i) {
    // delete all tiles
    if (arr_received_tiles_A[i]->size() == 0) {
      for (std::vector<Tile** >::iterator it = (*arr_received_tiles_A[i]).begin();
          it != (*arr_received_tiles_A[i]).end(); ++it) {

          delete *it;
      }
    }
    if (arr_received_tiles_B[i]->size() == 0) {
      for (std::vector<Tile** >::iterator it = (*arr_received_tiles_B[i]).begin();
          it != (*arr_received_tiles_B[i]).end(); ++it) {

          delete *it;
      }
    }

    // delete vector holding tiles for each sender
    delete arr_received_tiles_A[i];
    delete arr_received_tiles_B[i];
  }

  delete [] arr_received_tiles_A;
  delete [] arr_received_tiles_B;

}



// TODO send_error function

/******************************************************
 ****************** HELPER FUNCTIONS ******************
 ******************************************************/
inline std::string WorkerNode::arrayname_to_csv_filename(std::string arrayname) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  return ss.str();
}

// TODO optimize to use binary search if needed
inline int WorkerNode::get_receiver(std::vector<uint64_t> partitions, uint64_t cell_id) {
  int recv = 1;
  for (std::vector<uint64_t>::iterator it = partitions.begin(); it != partitions.end(); ++it) {
    if (cell_id <= *it) {
      return recv;
    }
    recv++;
  }
  return recv;
}

// TODO move to storage manager later probably
std::pair<std::vector<uint64_t>, std::vector<uint64_t> > WorkerNode::get_overlapping_tile_ranks(
    StorageManager::BoundingCoordinates& bounding_coords_A, 
    StorageManager::BoundingCoordinates& bounding_coords_B,
    const ArraySchema& array_schema_A,
    const ArraySchema& array_schema_B) { 

  //logger_->log(LOG_INFO, "In num_overlapping_tiles");

  int num_bc_A = bounding_coords_A.size();
  int num_bc_B = bounding_coords_B.size();
  // check for empty
  if (num_bc_A == 0 || num_bc_B == 0) {
    return std::pair<int, int>(0,0);
  }

  // TODO add other cell_ids later, only supporting hilbert for now
  std::pair<uint64_t, uint64_t> first_last_pair_A = 
    std::pair<uint64_t, uint64_t>(
        array_schema_A.cell_id_hilbert(bounding_coords_A[0].first), 
        array_schema_A.cell_id_hilbert(bounding_coords_A[num_bc_A-1].second));
  assert(first_last_pair_A.first <= first_last_pair_A.second);
  //logger_->log(LOG_INFO, "first_last_pair_A: " + util::to_string(first_last_pair_A));

  std::pair<uint64_t, uint64_t> first_last_pair_B = 
    std::pair<uint64_t, uint64_t>(
        array_schema_B.cell_id_hilbert(bounding_coords_B[0].first), 
        array_schema_B.cell_id_hilbert(bounding_coords_B[num_bc_B-1].second));
  assert(first_last_pair_B.first <= first_last_pair_B.second);

  //logger_->log(LOG_INFO, "first_last_pair_B: " + util::to_string(first_last_pair_B));
  // partitions do not overlap
  if (first_last_pair_A.second < first_last_pair_B.first ||
      first_last_pair_A.first > first_last_pair_B.second) {
    //logger_->log(LOG_INFO, "No overlap");
    return std::pair<int, int>(0,0);
  }

  int tile_rank_A = 0;
  int tile_rank_B = 0;

  std::vector<uint64_t> overlap_tile_ranks_A;
  std::vector<uint64_t> overlap_tile_ranks_B;

  // See how many tiles in my partition of A overlaps with the bounding
  // partitions of B 
  for(std::vector<StorageManager::BoundingCoordinatesPair>::iterator it_A = bounding_coords_A.begin(); 
      it_A != bounding_coords_A.end(); 
      ++it_A, ++tile_rank_A) {

    if (!(array_schema_A.cell_id_hilbert(it_A->first) > first_last_pair_B.second || array_schema_A.cell_id_hilbert(it_A->second) < first_last_pair_B.first)) {
      //logger_->log(LOG_INFO, "Found overlap between my bounding partition B and the bounding coords of partition A");
      overlap_tile_ranks_A.push_back(tile_rank_A);
    }

  }

  // See how many tiles in partition of B overlaps with the bounding
  // partitions of A
  for(std::vector<StorageManager::BoundingCoordinatesPair>::iterator it_B = bounding_coords_B.begin(); 
      it_B != bounding_coords_B.end(); 
      ++it_B, ++tile_rank_B) {

    if (!(array_schema_B.cell_id_hilbert(it_B->first) > first_last_pair_A.second || array_schema_B.cell_id_hilbert(it_B->second) < first_last_pair_A.first)) {
      //logger_->log(LOG_INFO, "Found overlap between my bounding partition A and the bounding coords of partition B");
      overlap_tile_ranks_B.push_back(tile_rank_B);
    }

  }

  return std::pair<std::vector<uint64_t>, std::vector<uint64_t> >(
      overlap_tile_ranks_A, overlap_tile_ranks_B);
}


bool WorkerNode::get_precedence(
    std::vector<Tile** > *extra_tiles,
    int num_attr,
    StorageManager::BoundingCoordinates local_bounding_coords, 
    const ArraySchema& array_schema) {

    assert(extra_tiles->size() > 0);

    int num_bcp = local_bounding_coords.size();
    int num_extra_tiles = extra_tiles->size();

    assert(num_bcp > 0);

    // TODO add other cell_ids later, only supporting hilbert for now
    // first_last_pair
    std::pair<uint64_t, uint64_t> fl_pair_local = 
      std::pair<uint64_t, uint64_t>(
        array_schema.cell_id_hilbert(local_bounding_coords[0].first), 
        array_schema.cell_id_hilbert(local_bounding_coords[num_bcp-1].second));

    assert(fl_pair_local.first <= fl_pair_local.second);

    std::pair<uint64_t, uint64_t> fl_pair_extra = 
      std::pair<uint64_t, uint64_t>(
        array_schema.cell_id_hilbert(
          ((extra_tiles->at(0))[num_attr])->bounding_coordinates().first), 
        array_schema.cell_id_hilbert(
          ((extra_tiles->at(num_extra_tiles-1))[num_attr])->bounding_coordinates().second));

    assert(fl_pair_extra.first <= fl_pair_extra.second);
    assert(fl_pair_local.first >= fl_pair_extra.second || fl_pair_local.second <= fl_pair_extra.first);

    if (fl_pair_local.first >= fl_pair_extra.second) {
      return true; // extra tiles precede local partition
    } else if (fl_pair_local.second <= fl_pair_extra.first) {
      return false;
    }

  // shouldn't get here
  return false;
}


//My c++ isn't great, i thought that c++ was smart enough to do this on its own
//with overloaded msg and message types
//the original plan was to only have handle(TYPEMsg*) and c++ would deduce the right 
//handle and call that, since that doesn't work this is the current workaround
int WorkerNode::handle_msg(int type, Msg* msg){
  switch(type){
    case GET_TAG:
      return handle((GetMsg*) msg);
    case DEFINE_ARRAY_TAG:
      return handle((DefineArrayMsg*) msg);
    case LOAD_TAG:
      return handle((LoadMsg*) msg);
    case SUBARRAY_TAG:
      return handle((SubarrayMsg*) msg);
    case FILTER_TAG:
      return handle((FilterMsg*) msg);
    case AGGREGATE_TAG:
      return handle((AggregateMsg*) msg);
    case PARALLEL_LOAD_TAG:
      return handle((ParallelLoadMsg*) msg);
    case JOIN_TAG:
      return handle((JoinMsg*) msg);
  }
  throw MessageException("trying to deserailze msg of unknown type");
}


/******************************************************
 ****************** PRIVATE FUNCTIONS *****************
 ******************************************************/
std::string WorkerNode::get_data_path(std::string filename) {

// directive for testing
#ifdef ISTC
  std::stringstream ss;
  ss << "t" << nprocs_ - 1 << "_s" << myrank_ - 1 << "_" << filename;
  return datadir_ + "/" + ss.str();
#else
  return datadir_ + "/" + filename;
#endif
}

