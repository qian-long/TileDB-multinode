#include "constants.h"
#include "assert.h"
#include "mpi.h"
#include "worker_node.h"
#include "debug.h"
#include "csv_file.h"
#include <cstring>
#include <stdexcept>      // std::invalid_argument
#include <sys/time.h>

WorkerNode::WorkerNode(int rank, int nprocs) {
  myrank_ = rank;
  nprocs_ = nprocs;
  std::stringstream workspace;
  // TODO put in config file
  workspace << "./workspaces/workspace-" << myrank_;
  my_workspace_ = workspace.str();
  storage_manager_ = new StorageManager(my_workspace_);
  loader_ = new Loader(my_workspace_, *storage_manager_);
  query_processor_ = new QueryProcessor(*storage_manager_);
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
            ArraySchema::DataType attr_type = static_cast<ArraySchema::DataType>(buf[0]);
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


      // TODO delete stuff to avoid memory leak
    } catch (StorageManagerException& sme) {
      logger_->log("StorageManagerException: ");
      logger_->log(sme.what());
      respond_ack(-1, status.MPI_TAG, -1);
    } catch(QueryProcessorException& qpe) {
      logger_->log("QueryProcessorException: ");
      logger_->log(qpe.what());
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
  logger_->log("Receive get msg: " + msg->array_name);

  std::string result_filename = arrayname_to_csv_filename(msg->array_name);
  logger_->log("Result filename: " + result_filename);

  // check if arrayname is in worker
  auto search = (*global_schema_map_).find(msg->array_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log("did not find schema!");
    return 0; // TODO need to fix b/c coordinator would hang
  }

  ArraySchema * schema = (*global_schema_map_)[msg->array_name];

  query_processor_->export_to_CSV(*(*global_schema_map_)[msg->array_name], result_filename);

  logger_->log("finished export to CSV");
  CSVFile file(result_filename, CSVFile::READ, MAX_DATA);
  CSVLine line;

  std::stringstream content;
  bool keep_receiving = true;
  int count = 0;
  try {
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

  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
    // TODO send error
    return 0;
  }

  // final send, tell coordinator to stop receiving
  keep_receiving = false;
  content.write((char *) &keep_receiving, sizeof(bool));
  MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, GET_TAG, MPI_COMM_WORLD);

  count++;

  return 0;
}

/*************** HANDLE ARRAY SCHEMA ***************/
int WorkerNode::handle(ArraySchemaMsg* msg) {

  (*global_schema_map_)[msg->array_schema->array_name()] = msg->array_schema;

  logger_->log("received array schema: \n" + msg->array_schema->to_string());
  return 0;
}

/*************** HANDLE LOAD **********************/
int WorkerNode::handle(LoadMsg* msg) {
  logger_->log("Received load\n");

  (*global_schema_map_)[msg->array_schema->array_name()] = msg->array_schema;
  loader_->load(convert_filename(msg->filename), *msg->array_schema, msg->order);

  logger_->log("Finished load");
  return 0;
}

/*************** HANDLE SubarrayMsg **********************/
int WorkerNode::handle(SubArrayMsg* msg) {
  logger_->log("Received subarray \n");

  std::string global_schema_name = msg->array_schema->array_name();
  // temporary hack, create a copy of the array schema and replace the array
  // name
  // TODO check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log("did not find schema!");
    return 0; // TODO need to fix b/c coordinator would hang
  }

  ArraySchema * new_schema = ((*global_schema_map_)[global_schema_name])->deep_copy(msg->result_array_name);

  (*global_schema_map_)[msg->result_array_name] = new_schema;

  query_processor_->subarray(*(msg->array_schema), msg->ranges, msg->result_array_name);

  logger_->log("Finished subarray ");

  return 0;
}


/*************** HANDLE FILTER **********************/
template<class T>
int WorkerNode::handle_filter(FilterMsg<T>* msg, ArraySchema::DataType attr_type) {
  logger_->log("Received filter");

  std::string global_schema_name = msg->array_schema_.array_name();

  // temporary hack, create a copy of the array schema and replace the array
  // name
  // TODO check if arrayname is in worker
  auto search = (*global_schema_map_).find(global_schema_name);
  if (search == (*global_schema_map_).end()) {
    logger_->log("did not find schema!");
    return -1; // TODO need to fix b/c coordinator would hang
  }

  ArraySchema * new_schema = ((*global_schema_map_)[global_schema_name])->deep_copy(msg->result_array_name_);

  (*global_schema_map_)[msg->result_array_name_] = new_schema;

  query_processor_->filter_irregular<T>(msg->array_schema_, msg->predicate_, msg->result_array_name_); 
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

/*
std::string WorkerNode::get_arrayname(std::string arrayname) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  return ss.str();
}
*/

std::string WorkerNode::convert_filename(std::string filename) {
  std::stringstream ss;
  ss << "./Data/" << filename.c_str() << ".csv";
  //ss << my_workspace_ << "/" << filename.c_str() << "_rnk" << myrank_ << ".csv";
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
    case LOAD_TAG: // TODO
      return handle((LoadMsg*) msg);
    case SUBARRAY_TAG:
      return handle((SubArrayMsg*) msg);
  }
  throw std::invalid_argument("trying to deserailze msg of unknown type");
}
