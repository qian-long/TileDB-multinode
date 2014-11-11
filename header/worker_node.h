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
#include <string>

class WorkerNode {
  public:
    // CONSTRUCTORS
    WorkerNode(int rank, int nprocs);

    // DESTRUCTOR
    ~WorkerNode();


    /** Runs worker. Listens for instructions from Coordinator. Shouldn't return */
    void run();

    int get(std::string);
    void subarray();

  private:
    // PRIVATE ATTRIBUTES
    int myrank_;
    int nprocs_;
    std::string my_workspace_;
    Loader* loader_;
    StorageManager* storage_manager_;
    QueryProcessor* query_processor_;

    // TODO other stuff like what ranges of coordinate values I hold


};

#endif
