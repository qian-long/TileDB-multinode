#include <ostream>
#include <iostream>
#include <istream>
#include <fstream>
#include <cstring>
#include <stdexcept>      // std::invalid_argument
#include <sys/time.h>
#include <mpi.h>
#include "constants.h"
#include "assert.h"
#include "worker_node.h"
#include "debug.h"
#include "csv_file.h"

WorkerNode::WorkerNode(int rank, int nprocs) {
  myrank_ = rank;
  nprocs_ = nprocs;
  std::stringstream workspace;
  // TODO put in config file
  workspace << "./workspaces/workspace-" << myrank_;
  my_workspace_ = workspace.str();
  executor_ = new Executor(my_workspace_);
  logger_ = new Logger(my_workspace_ + "/logfile");


  std::vector<int> other_nodes; // everyone except self
  for (int i = 0; i < nprocs; ++i) {
    if (i != myrank_) {
      other_nodes.push_back(i);
    }
  }
  mpi_handler_ = new MPIHandler(other_nodes);

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
  char tim_buf[100];
  int tim_len;
  double tstart;
  double tend;
  while (loop) {
    try {
      MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_CHAR, &length);
      switch (status.MPI_TAG) {
        case QUIT_TAG:
          loop = false;
          break;
        case GET_TAG:
        case DEFINE_ARRAY_TAG:
        case LOAD_TAG: 
        case SUBARRAY_TAG:
        case AGGREGATE_TAG:
        case PARALLEL_LOAD_TAG: // TODO
          gettimeofday(&tim, NULL);  
          tstart = tim.tv_sec+(tim.tv_usec/1000000.0);  

          msg = deserialize_msg(status.MPI_TAG, buf, length);
          result = handle_msg(msg->msg_tag, msg);

          gettimeofday(&tim, NULL);  
          tend = tim.tv_sec+(tim.tv_usec/1000000.0);  

          respond_ack(result, status.MPI_TAG, tend - tstart);
          break;

        case FILTER_TAG: 
          {
            gettimeofday(&tim, NULL);  
            tstart = tim.tv_sec+(tim.tv_usec/1000000.0);  


            // this is really awkward with templating because the type of the
            // attribute must be known before we create the filtermsg obj...
            // // TODO look for a better way later
            ArraySchema::CellType attr_type = static_cast<ArraySchema::CellType>(buf[0]);
            switch(attr_type) {
              case ArraySchema::INT:
                {
                  FilterMsg<int> *fmsg = FilterMsg<int>::deserialize(buf, length);
                  result = handle_filter(fmsg, attr_type);
                  break; 
                }
              case ArraySchema::FLOAT:
                {
                  FilterMsg<float> *fmsg = FilterMsg<float>::deserialize(buf, length);
                  result = handle_filter(fmsg, attr_type);
                  break; 
                }
               break; 
              case ArraySchema::DOUBLE:
                {
                  FilterMsg<double> *fmsg = FilterMsg<double>::deserialize(buf, length);
                  result = handle_filter(fmsg, attr_type);
                  break; 
                }
               break; 
              default:
                // Data got corrupted
                // TODO throw exception, fix int64_t
                throw std::invalid_argument("trying to deserailze filter msg of unknown type");
                break;
            } 

            gettimeofday(&tim, NULL);  
            tend = tim.tv_sec+(tim.tv_usec/1000000.0);  
            respond_ack(result, status.MPI_TAG, tend - tstart);
          }
          break;
        default:
          std::string content(buf, length);
          logger_->log(LOG_INFO, content);
      }

   /*
      // TODO delete stuff to avoid memory leak
    } catch (StorageManagerException& sme) {
      logger_->log("StorageManagerException: ");
      logger_->log(sme.what());
      respond_ack(-1, status.MPI_TAG, -1);
    } catch(QueryProcessorException& qpe) {
      logger_->log("QueryProcessorException: ");
      logger_->log(qpe.what());
      respond_ack(-1, status.MPI_TAG, -1);
   */
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

void WorkerNode::respond_ack(int result, int tag, double time) {
  std::stringstream ss;

  switch (tag) {
    case GET_TAG:
      ss << "GET";
      break;
    case DEFINE_ARRAY_TAG:
      ss << "DEFINE_ARRAY_TAG";
      break;
    case LOAD_TAG:
      ss << "LOAD";
      break;
    case SUBARRAY_TAG:
      ss << "SUBARRAY";
      break;
    case FILTER_TAG:
      ss << "FILTER";
      break;
    case AGGREGATE_TAG:
      ss << "AGGREGATE";
      break;
    case PARALLEL_LOAD_TAG:
      ss << "PARALLEL_LOAD";
    default:
      break;
  }
  if (result == 0) {
    tag = DONE_TAG;
    ss << "[DONE]";
  } else {
    tag = ERROR_TAG;
    ss << "[ERROR]";
  }

  ss << " Time[" << time << " secs]";

  logger_->log(LOG_INFO, "Sending ack: " + ss.str());
  MPI_Send(ss.str().c_str(), ss.str().length(), MPI::CHAR, MASTER, tag, MPI_COMM_WORLD);

}

/******************************************************
 ****************** MESSAGE HANDLERS ******************
 ******************************************************/


/*************** HANDLE GET **********************/
int WorkerNode::handle(GetMsg* msg) {
  logger_->log(LOG_INFO, "ReceiveD Get Msg: " + msg->array_name());

  std::string result_filename = arrayname_to_csv_filename(msg->array_name());
  logger_->log(LOG_INFO, "Result filename: " + result_filename);


  logger_->log(LOG_INFO, "exporting to CSV");
  executor_->export_to_csv(msg->array_name(), result_filename);

  logger_->log(LOG_INFO, "sending file to master");
  mpi_handler_->send_file(result_filename, MASTER, GET_TAG);

  return 0;
}

/*************** HANDLE DEFINE ARRAY ***************/
int WorkerNode::handle(DefineArrayMsg* msg) {
  logger_->log(LOG_INFO, "Received define array msg");

  executor_->define_array(msg->array_schema());

  logger_->log(LOG_INFO, "Finished defining array");
  return 0;
}

/*************** HANDLE LOAD **********************/
// TODO move parallel load here
int WorkerNode::handle(LoadMsg* msg) {
  logger_->log(LOG_INFO, "Received load\n");

  switch (msg->load_type()) {
    case LoadMsg::ORDERED:
      handle_load_ordered(msg->filename(), msg->array_schema());
      break;
    case LoadMsg::HASH:
      handle_load_hash(msg->filename(), msg->array_schema());
      break;
    default:
      // TODO send error
      break;
  }

  logger_->log(LOG_INFO, "Update Fragment Info");
  executor_->update_fragment_info(msg->array_schema().array_name());
  logger_->log(LOG_INFO, "Finished load");
  return 0;
}

// TODO fix
/*************** HANDLE SubarrayMsg **********************/
int WorkerNode::handle(SubarrayMsg* msg) {
  logger_->log(LOG_INFO, "Received subarray \n");

  /*
  std::string global_schema_name = msg->array_schema().array_name();
  // temporary hack, create a copy of the array schema and replace the array
  // name
  // check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log(LOG_INFO, "did not find schema!");
    return -1;
  }

  ArraySchema new_schema = ((*global_schema_map_)[global_schema_name])->clone(msg->result_array_name());

  (*global_schema_map_)[msg->result_array_name()] = &new_schema;

  StorageManager::ArrayDescriptor* desc = storage_manager_->open_array(msg->array_schema().array_name());

  executor_->subarray(desc, msg->ranges(), msg->result_array_name());

  logger_->log(LOG_INFO, "Finished subarray ");
  */

  return 0;
}

/*************** HANDLE AggregateMsg **********************/
int WorkerNode::handle(AggregateMsg* msg) {
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

  return 0;
}

/*************** HANDLE PARALLEL LOAD ***************/
int WorkerNode::handle(ParallelLoadMsg* msg) {
  logger_->log(LOG_INFO, "Received Parallel Load Message");
  logger_->log(LOG_INFO, "Filename: " + msg->filename());

  //std::string filepath = "./data/" + msg->filename();
  switch (msg->load_type()) {
    case ParallelLoadMsg::NAIVE:
      //handle_parallel_load_naive(msg->filename(), msg->array_schema());
      break;
    case ParallelLoadMsg::HASH_PARTITION:
      handle_parallel_load_hash(msg->filename(), msg->array_schema());
      break;
    case ParallelLoadMsg::SAMPLING:
      break;
    case ParallelLoadMsg::MERGE_SORT:
      break;
    default:
      // TODO send error
      break;
  }

  // TODO cleanup
  return 0;
}

int WorkerNode::handle_load_ordered(std::string filename, ArraySchema& array_schema) {

  logger_->log(LOG_INFO, "In Handle Load Sort");
  // Wait for sorted file from master
  std::string frag_name = "0_0";
  std::ofstream sorted_file;

  std::string sorted_filepath = executor_->loader()->workspace() + "/sorted_" + array_schema.array_name() + "_" + frag_name + ".csv";

  sorted_file.open(sorted_filepath);

  logger_->log(LOG_INFO, "Receiving sorted file from master");
  mpi_handler_->receive_file(sorted_file, MASTER, LOAD_TAG);

  sorted_file.close();

  logger_->log(LOG_INFO, "Starting make tiles on " + sorted_filepath);
  StorageManager::FragmentDescriptor* fd =
    executor_->storage_manager()->open_fragment(&array_schema, frag_name,
                                                StorageManager::CREATE);

  // Make tiles
  try {
    if(array_schema.has_regular_tiles())
      executor_->loader()->make_tiles_regular(sorted_filepath, fd);
    else
      executor_->loader()->make_tiles_irregular(sorted_filepath, fd);
  } catch(LoaderException& le) {
    // TODO uncomment
    //remove(sorted_filepath.c_str());
    executor_->storage_manager()->delete_fragment(array_schema.array_name(), frag_name);
    throw LoaderException("Error invoking local load" + filename +
                          "'.\n " + le.what());
  }

  logger_->log(LOG_INFO, "Finished make tiles");
  executor_->storage_manager()->close_fragment(fd);

  // TODO cleanup
  return 0;
}

int WorkerNode::handle_load_hash(std::string filename, ArraySchema& array_schema) {

  std::ofstream output;
  std::string filepath = my_workspace_ + "/data/" + filename;

  logger_->log(LOG_INFO, "Receiving hash partitioned data from master, writing to filepath " + filepath);

  output.open(filepath);
  // Blocking
  mpi_handler_->receive_file(output, MASTER, PARALLEL_LOAD_TAG);
  output.close();

  // Invoke local load
  logger_->log(LOG_INFO, "Invoking local load");
  // Inject cell id
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

  // Open array in CREATE mode
  StorageManager::FragmentDescriptor* fd =
    executor_->storage_manager()->open_fragment(&array_schema, frag_name,
                                                StorageManager::CREATE);



  logger_->log(LOG_INFO, "Starting make tiles on " + sorted_filepath);
  // Make tiles
  try {
    if(array_schema.has_regular_tiles())
      executor_->loader()->make_tiles_regular(sorted_filepath, fd);
    else
      executor_->loader()->make_tiles_irregular(sorted_filepath, fd);
  } catch(LoaderException& le) {
    // TODO uncomment
    //remove(sorted_filepath.c_str());
    executor_->storage_manager()->delete_fragment(array_schema.array_name(), frag_name);
    throw LoaderException("Error invoking local load" + filename +
                          "'.\n " + le.what());
  }


  logger_->log(LOG_INFO, "Finished make tiles");
  executor_->storage_manager()->close_fragment(fd);


  // TODO cleanup
  return 0;

}

int WorkerNode::handle_parallel_load_hash(std::string filename, ArraySchema& array_schema) {

  std::string filepath = my_workspace_ + "/data/" + filename;
  std::string outpath = my_workspace_ + "/data/HASH_" + filename;

  std::ofstream outfile;
  outfile.open(outpath);

  std::vector<MPI_Request> my_requests;
  std::vector<MPI_Request> others_requests;
  // scan csv file, compute hash, asynchronous send to receivers
  CSVFile csv_in(filepath, CSVFile::READ);
  CSVLine csv_line;
  std::hash<std::string> hash_fn;


  int nworkers = nprocs_ - 1;
  int limit = 2; // after processing 6 lines, do n to n shuffle
  int count = 0;
  std::map<int, std::string> chunk_map;
  // scan data to compute meta data (how many chunks to send to each node)
  //
  std::stringstream ss;
  while (csv_in >> csv_line) {
    // TODO look into other hash fns, using default for strings right now
    std::string coord_id = csv_line.values()[0];
    for (int i = 1; i < array_schema.dim_num(); ++i) {
      coord_id += ",";
      coord_id += csv_line.values()[i];
    }
    std::size_t cell_id_hash = hash_fn(coord_id);
    int receiver = (cell_id_hash % nworkers) + 1;
    std::string csv_line_str = csv_line.str() + "\n";

    auto search = chunk_map.find(receiver);

    if (search == chunk_map.end()) {
      chunk_map.insert(std::pair<int, std::string>(receiver, csv_line_str));
    } else {
      search->second += csv_line_str;
    }
    count++;

    if (count == limit) {
      //MPI_Alltoallv(sendbuf, sendcounts[], sdispls, sendtype, recvbuf, recvcounts, rdispls, rectype, comm)
      int scounts[nprocs_];
      int rcounts[nprocs_];
      int sdispls[nprocs_];
      int rdispls[nprocs_];


      scounts[0] = 0;
      sdispls[0] = 0;

      int send_total = 0;
      for (int i = 1; i < nprocs_; ++i) {
        auto search = chunk_map.find(i);
        if (search == chunk_map.end()) {
          // send empty placeholder
          scounts[i] = 0;
          sdispls[i] = sdispls[i-1];
        } else {
          scounts[i] = search->second.size();
          sdispls[i] = sdispls[i-1] + scounts[i-1];
          ss << search->second;
        }
        send_total += scounts[i];
      }

      /* tell the other processors how much data is coming */
      logger_->log(LOG_INFO, "sending counts to other processes");
      MPI_Alltoall(&scounts, 1, MPI_INT, &rcounts, 1, MPI_INT, MPI_COMM_WORLD);

      int rec_total = 0;
      rdispls[0] = 0;
      for (int i = 0; i < nprocs_; ++i) {
        logger_->log(LOG_INFO, "Expecting " + std::to_string(rcounts[i]) + " bytes from node " + std::to_string(i));
        rec_total += rcounts[i];
        if (i > 0) {
          rdispls[i] = rcounts[i-1] + rdispls[i-1];
        }
      }


      //char *sendbuf = new char[send_total];
      char *recbuf = new char[rec_total];
      logger_->log(LOG_INFO, "rec_total: " + std::to_string(rec_total));
      MPI_Alltoallv(ss.str().c_str(), scounts, sdispls, MPI_CHAR, recbuf, rcounts, rdispls, MPI_CHAR, MPI_COMM_WORLD);
      logger_->log(LOG_INFO, std::string(recbuf, rec_total));
      // reset
      count = 0;
      ss.str(std::string());
    }

  }


  outfile.close();


  // invoke local load on received data


  // send ack back
}

/*************** HANDLE FILTER **********************/
template<class T>
int WorkerNode::handle_filter(FilterMsg<T>* msg, ArraySchema::CellType attr_type) {
  logger_->log(LOG_INFO, "Received filter");

  std::string global_schema_name = msg->array_schema().array_name();

  // temporary hack, create a copy of the array schema and replace the array
  // name
  // TODO check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log(LOG_INFO, "did not find schema!");
    return -1; // TODO need to fix b/c coordinator would hang
  }

  ArraySchema new_schema = ((*global_schema_map_)[global_schema_name])->clone(msg->result_array_name());

  (*global_schema_map_)[msg->result_array_name()] = &new_schema;

  // TODO add back
  //query_processor_->filter_irregular<T>(&msg->array_schema(), msg->predicate(), msg->result_array_name()); 
  logger_->log(LOG_INFO, "Finished filter");
  return 0;
}

// TODO send_error function

/******************************************************
 ****************** HELPER FUNCTIONS ******************
 ******************************************************/
std::string WorkerNode::arrayname_to_csv_filename(std::string arrayname) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  return ss.str();
}

std::string WorkerNode::convert_filename(std::string filename) {
  std::stringstream ss;
  // TODO move to config file
  ss << "./data/" << filename.c_str() << ".csv";
  return ss.str();
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
    case AGGREGATE_TAG:
      return handle((AggregateMsg*) msg);
    case PARALLEL_LOAD_TAG:
      return handle((ParallelLoadMsg*) msg);
  }
  throw std::invalid_argument("trying to deserailze msg of unknown type");
}
