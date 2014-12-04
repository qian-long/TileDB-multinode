#include "constants.h"
#include "assert.h"
#include <cstring>
#include "mpi.h"
#include "worker_node.h"
#include "debug.h"
#include "csv_file.h"
#include "messages.h"


WorkerNode::WorkerNode(int rank, int nprocs) {
  this->myrank_ = rank;
  this->nprocs_ = nprocs;
  std::stringstream workspace;
  // TODO put in config file
  workspace << "./workspaces/workspace-" << myrank_;
  this->my_workspace_ = workspace.str();
  this->storage_manager_ = new StorageManager(my_workspace_);
  this->loader_ = new Loader(my_workspace_, *storage_manager_);
  this->query_processor_ = new QueryProcessor(*storage_manager_);

  // catalogue data structures
  this->arrayname_map_ = new std::map<std::string, std::string>();
  this->global_schema_map_ = new std::map<std::string, ArraySchema *>();
  this->local_schema_map_ = new std::map<std::string, ArraySchema *>();
}

// TODO delete things inside maps?
WorkerNode::~WorkerNode() {
  delete arrayname_map_;
  delete global_schema_map_;
  delete local_schema_map_;
}


void WorkerNode::run() {
  DEBUG_MSG("I am a worker node");

  MPI_Status status;
  char *buf = new char[MAX_DATA];
  int length;
  int loop = true;
  int result;
  Msg* msg;
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
          msg = deserialize_msg(status.MPI_TAG, buf, length);
          result = handle_msg(msg->msg_tag, msg);
          assert(result);
          break;
        case FILTER_TAG: 
          {
            // this is really awkward with templating because the type of the
            // attribute must be known before we create the filtermsg obj...
            // // TODO look for a better way later
            ArraySchema::DataType attr_type = static_cast<ArraySchema::DataType>(buf[0]);
            switch(attr_type) {
              case ArraySchema::DataType::INT:
                {
                  FilterMsg<int> *fmsg = FilterMsg<int>::deserialize(buf, length);
                  result = handle_filter(fmsg, attr_type);
                  break; 
                }
              case ArraySchema::DataType::FLOAT:
                {
                  FilterMsg<float> *fmsg = FilterMsg<float>::deserialize(buf, length);
                  result = handle_filter(fmsg, attr_type);
                  break; 
                }
               break; 
              case ArraySchema::DataType::DOUBLE:
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

          }
          break;
        default:
          std::string content(buf, length);
          DEBUG_MSG(content);
      }

      // TODO delete stuff to avoid memory leak
    } catch (StorageManagerException& sme) {
      std::cout << "StorageManagerException:\n";
      std::cout << sme.what() << "\n";
    } catch(QueryProcessorException& qpe) {
      DEBUG_MSG("QueryProcessorException: ");
      DEBUG_MSG(qpe.what());
    }

  }
}


int WorkerNode::handle(GetMsg* msg) {
  DEBUG_MSG("Receive get msg");

  std::string result_filename = arrayname_to_csv_filename(msg->array_name);
  query_processor_->export_to_CSV(*(*global_schema_map_)[msg->array_name], result_filename);
  CSVFile file(result_filename, CSVFile::READ, MAX_DATA);
  CSVLine line;

  // TODO make it "stream", right now everything is in one string
  std::stringstream content;
  try {
    while(file >> line) {
      content << line.str() << "\n";
    }
  } catch(CSVFileException& e) {
    std::cout << e.what() << "\n";
  }

  MPI_Send(content.str().c_str(), content.str().length(), MPI::CHAR, MASTER, GET_TAG, MPI_COMM_WORLD);
  return 1;
}

int WorkerNode::handle(ArraySchemaMsg* msg) {
  (*this->global_schema_map_)[msg->array_schema->array_name()] = msg->array_schema;

  // debug message
  DEBUG_MSG("received array schema: \n" + msg->array_schema->to_string());
  return 1;
}

int WorkerNode::handle(LoadMsg* msg) {
  DEBUG_MSG("Received load\n");

  this->loader_->load(convert_filename(msg->filename), *msg->array_schema, msg->order);

  DEBUG_MSG("Finished load");
  return 1;
}

template<class T>
int WorkerNode::handle_filter(FilterMsg<T>* msg, ArraySchema::DataType attr_type) {
  DEBUG_MSG("Received filter");
  DEBUG_MSG("msg->array_schema->array_name(): " + msg->array_schema_.array_name());

  query_processor_->filter_irregular<T>(msg->array_schema_, msg->predicate_, "blsh"); 
  DEBUG_MSG("Finished filter");
  return 1;
}



/******************************************************
 ****************** HELPER FUNCTIONS ******************
 ******************************************************/
std::string WorkerNode::arrayname_to_csv_filename(std::string arrayname) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  return ss.str();
}

std::string WorkerNode::get_arrayname(std::string arrayname) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << arrayname.c_str() << "_rnk" << myrank_ << ".csv";
  return ss.str();
}

std::string WorkerNode::convert_filename(std::string filename) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << filename.c_str() << "_rnk" << myrank_ << ".csv";
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
  }
  throw std::invalid_argument("trying to deserailze msg of unknown type");
}
/*
std::string WorkerNode::convert_arrayname(std::string garray_name) {
  std::stringstream ss;
  ss << my_workspace_ << "/" << garray_name.c_str() << "_rnk" << myrank_;
  return ss.str();
}
*/
