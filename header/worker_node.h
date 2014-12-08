/**
 * @file worker_node.h
 * @section DESCRIPTION
 * This file defines class WorkerNode
 */

#ifndef WORKERNODE_H
#define WORKERNODE_H
#include "loader.h"
#include "storage_manager.h"
#include "query_processor.h"
#include "array_schema.h"
#include "messages.h"
#include "logger.h"
#include <string>
#include <map>

class WorkerNode {
  public:
    // CONSTRUCTORS
    WorkerNode(int rank, int nprocs);

    // DESTRUCTOR
    ~WorkerNode();


    /** Runs worker. Listens for instructions from Coordinator. Shouldn't return */
    void run();

    // ACTIONS TO TAKE WHEN RECEIVING A MESSAGE
    int handle_msg(int, Msg*);
    int handle(ArraySchemaMsg* msg);
    int handle(LoadMsg* msg);
    int handle(GetMsg* msg);
    int handle(SubArrayMsg* msg);
    
    template<class T>
    int handle_filter(FilterMsg<T>* msg, ArraySchema::DataType attr_type);

    void respond_ack(int result, int tag);

    // HELPER FUNCTIONS
    std::string get_arrayname(std::string);

    /** Returns filepath of matching csv file **/
    std::string convert_filename(std::string filename);
    
    /** Converts global array name to local array name **/
    std::string convert_arrayname(std::string garray_name);

    std::string arrayname_to_csv_filename(std::string arrayname);
  private:
    // PRIVATE ATTRIBUTES
    int myrank_;
    int nprocs_;
    std::string my_workspace_;
    Loader* loader_;
    StorageManager* storage_manager_;
    QueryProcessor* query_processor_;
    Logger* logger_;

    // CATALOGUE of all the arrays in the system
    // map of global array name to local array name
    std::map<std::string, std::string> * arrayname_map_;
    // map of global array name to global array schema
    std::map<std::string, ArraySchema *> * global_schema_map_;
    // map of global array name to local array schema
    std::map<std::string, ArraySchema *> * local_schema_map_;

};

#endif
