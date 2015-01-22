#include <ostream>
#include <iostream>
#include <istream>
#include <fstream>
#include <cstring>
#include <stdexcept>      // std::invalid_argument
#include <sys/time.h>
#include "constants.h"
#include "assert.h"
#include "mpi.h"
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
  storage_manager_ = new StorageManager(my_workspace_);
  loader_ = new Loader(my_workspace_, *storage_manager_);
  query_processor_ = new QueryProcessor(my_workspace_, *storage_manager_);
  logger_ = new Logger(my_workspace_ + "/logfile");

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
  logger_->log("I am a worker node");

  MPI_Status status;
  char *buf = new char[MAX_DATA];
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
      MPI_Recv(buf, MAX_DATA, MPI_CHAR, MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      MPI_Get_count(&status, MPI_CHAR, &length);
      switch (status.MPI_TAG) {
        case QUIT_TAG:
          loop = false;
          break;
        case GET_TAG:
        case ARRAY_SCHEMA_TAG:
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
          logger_->log(content);
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
      logger_->log("LoaderException: ");
      logger_->log(le.what());
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
    case ARRAY_SCHEMA_TAG:
      ss << "ARRAY_SCHEMA_TAG";
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

  logger_->log("Sending ack: " + ss.str());
  MPI_Send(ss.str().c_str(), ss.str().length(), MPI::CHAR, MASTER, tag, MPI_COMM_WORLD);

}

/******************************************************
 ****************** MESSAGE HANDLERS ******************
 ******************************************************/


/*************** HANDLE GET **********************/
int WorkerNode::handle(GetMsg* msg) {
  logger_->log("ReceiveD Get Msg: " + msg->array_name());

  std::string result_filename = arrayname_to_csv_filename(msg->array_name());
  logger_->log("Result filename: " + result_filename);

  // check if arrayname is in worker
  auto search = (*global_schema_map_).find(msg->array_name());
  if (search == (*global_schema_map_).end()) {
    logger_->log("did not find schema!");
    return 0; // TODO need to fix b/c coordinator would hang
  }

  ArraySchema* schema = (*global_schema_map_)[msg->array_name()];

  StorageManager::ArrayDescriptor* desc = storage_manager_->open_array(schema->array_name());

  query_processor_->export_to_CSV(desc, result_filename);

  logger_->log("finished export to CSV");
  CSVFile file(result_filename, CSVFile::READ, MAX_DATA);
  CSVLine line;

  std::stringstream content;
  bool keep_receiving = true;
  int count = 0;
  // TODO figure out correct exceptions
  //try {
    while(file >> line) {
      // encode "there is more coming" in the last byte
      if (content.str().length() + line.str().length() + 1 >= MAX_DATA) {
        content.write((char *) &keep_receiving, sizeof(bool));
        MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, GET_TAG, MPI_COMM_WORLD);
        count++;
        content.str(std::string()); // clear buffer
      }
      content << line.str() << "\n";
    }

  /*
  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
    // TODO send error
    return 0;
  }
  */

  // final send, tell coordinator to stop receiving
  keep_receiving = false;
  content.write((char *) &keep_receiving, sizeof(bool));
  MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, GET_TAG, MPI_COMM_WORLD);

  count++;

  return 0;
}

/*************** HANDLE ARRAY SCHEMA ***************/
int WorkerNode::handle(ArraySchemaMsg* msg) {

  (*global_schema_map_)[msg->array_schema().array_name()] = &msg->array_schema();

  logger_->log("received array schema: \n" + msg->array_schema().to_string());
  return 0;
}

/*************** HANDLE LOAD **********************/
int WorkerNode::handle(LoadMsg* msg) {
  logger_->log("Received load\n");

  (*global_schema_map_)[msg->array_schema().array_name()] = &msg->array_schema();
  loader_->load(convert_filename(msg->filename()), msg->array_schema());

  logger_->log("Finished load");
  return 0;
}

/*************** HANDLE SubarrayMsg **********************/
int WorkerNode::handle(SubarrayMsg* msg) {
  logger_->log("Received subarray \n");

  std::string global_schema_name = msg->array_schema().array_name();
  // temporary hack, create a copy of the array schema and replace the array
  // name
  // check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log("did not find schema!");
    return -1;
  }

  ArraySchema new_schema = ((*global_schema_map_)[global_schema_name])->clone(msg->result_array_name());

  (*global_schema_map_)[msg->result_array_name()] = &new_schema;

  StorageManager::ArrayDescriptor* desc = storage_manager_->open_array(msg->array_schema().array_name());

  query_processor_->subarray(desc, msg->ranges(), msg->result_array_name());

  logger_->log("Finished subarray ");

  return 0;
}

/*************** HANDLE AggregateMsg **********************/
int WorkerNode::handle(AggregateMsg* msg) {
  logger_->log("Received aggregate");
  logger_->log("arrayname: " + msg->array_name());
  std::string global_schema_name = msg->array_name();
  // check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log("Aggregate did not find schema!");
    // TODO move to while loop in run somehow
    //respond_ack(-1, ERROR_TAG, -1); 
    return -1;
  }

  // TODO add back
  //int max = query_processor_->aggregate(*(search->second), msg->attr_index_);
  int max = 0;

  logger_->log("Sending my computed MAX aggregate: " + std::to_string(max));

  std::stringstream content;
  content.write((char *) &max, sizeof(int));
  MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, AGGREGATE_TAG, MPI_COMM_WORLD);

  return 0;
}

/*************** HANDLE PARALLEL LOAD ***************/
int WorkerNode::handle(ParallelLoadMsg* msg) {
  logger_->log("Received Parallel Load Message");
  logger_->log("Filename: " + msg->filename());

  std::string filepath = "./data/" + msg->filename();
  // TODO check that filename exists in workspace, error if doesn't
  // TODO save array schema?
  
  // TODO open file and append tile-id/hilbert order
    // inject ids if regular or hilbert order
  bool regular = msg->array_schema().has_regular_tiles();
  ArraySchema::Order order = msg->array_schema().order();
  std::string injected_filename = filepath;
  if (regular || order == ArraySchema::HILBERT) {
    injected_filename = loader_->workspace() + "/injected_" +
                        msg->array_schema().array_name() + ".csv";
    try {

      logger_->log("Injecting ids into " + filepath + ", outputting to " + injected_filename);
      loader_->inject_ids_to_csv_file(filepath, injected_filename, msg->array_schema());
    } catch(LoaderException& le) {
      logger_->log("Caught loader exception");
      remove(injected_filename.c_str());
      storage_manager_->delete_array(msg->array_schema().array_name());
      throw LoaderException("[WorkerNode] Cannot inject ids to file\n" + le.what());
    }
  }

  logger_->log("sending csv file back to master injected_filename: " + injected_filename);
  // Send csv file back to master, chunk by chunk
  // TODO refactor this out
  CSVFile file(injected_filename, CSVFile::READ, MAX_DATA);
  CSVLine line;
  std::stringstream content;
  bool keep_receiving = true;
  while(file >> line) {
    if (content.str().length() + line.str().length() + 1 >= MAX_DATA) {
      // encode "there is more coming" in the last byte
      content.write((char *) &keep_receiving, sizeof(bool));

      // send content to master
      MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, PARALLEL_LOAD_TAG, MPI_COMM_WORLD);

      content.str(std::string()); // clear buffer
    }

    content << line.str() << "\n";

  }

  // final send, tell coordinator to stop receiving
  keep_receiving = false;
  content.write((char *) &keep_receiving, sizeof(bool));
  MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, PARALLEL_LOAD_TAG, MPI_COMM_WORLD);


  // TODO wait for sorted file from master
  logger_->log("waiting for sorted file from master");
  keep_receiving = true;
  char *buf = new char[MAX_DATA];
  MPI_Status status;
  int length;
  int content_length;

  std::ofstream sorted_file;
  std::string sorted_filename = injected_filename + ".sorted";
  sorted_file.open(sorted_filename);

  do {
    MPI_Recv(buf, MAX_DATA, MPI_CHAR, MASTER, PARALLEL_LOAD_TAG, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &length);

    content_length = length - 1;

    // check last byte to see if keep receiving
    keep_receiving = (bool) buf[content_length];

    // TODO write to temp file
    sorted_file << std::string(buf, content_length);

  } while (keep_receiving);

  sorted_file.close();
  // Invoke local load
  // Open array in CREATE mode
  StorageManager::ArrayDescriptor* ad = 
      storage_manager_->open_array(msg->array_schema());

  // Make tiles
  try {
    if(msg->array_schema().has_regular_tiles())
      loader_->make_tiles_regular(sorted_filename, ad, msg->array_schema());
    else
      loader_->make_tiles_irregular(sorted_filename, ad, msg->array_schema());
  } catch(LoaderException& le) {
    //remove(sorted_filename.c_str());
    //storage_manager_.delete_array(array_schema.array_name());
    throw LoaderException("Error invoking local load" + msg->filename() + 
                          "'.\n " + le.what());
  } 


  storage_manager_->close_array(ad);

  // TODO cleanup
  return 0;
}
/*************** HANDLE FILTER **********************/
template<class T>
int WorkerNode::handle_filter(FilterMsg<T>* msg, ArraySchema::CellType attr_type) {
  logger_->log("Received filter");

  std::string global_schema_name = msg->array_schema().array_name();

  // temporary hack, create a copy of the array schema and replace the array
  // name
  // TODO check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log("did not find schema!");
    return -1; // TODO need to fix b/c coordinator would hang
  }

  ArraySchema new_schema = ((*global_schema_map_)[global_schema_name])->clone(msg->result_array_name());

  (*global_schema_map_)[msg->result_array_name()] = &new_schema;

  // TODO add back
  //query_processor_->filter_irregular<T>(&msg->array_schema(), msg->predicate(), msg->result_array_name()); 
  logger_->log("Finished filter");
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
    case ARRAY_SCHEMA_TAG:
      return handle((ArraySchemaMsg*) msg);
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
