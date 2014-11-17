/**
 * @file coordinator_node.h
 * @section DESCRIPTION
 * This file defines class CoordinatorNode
 */
#ifndef COORDINATORNODE_H
#define COORDINATORNODE_H
#include "constants.h"

class CoordinatorNode {
  public:
    // CONSTRUCTORS
    CoordinatorNode(int rank, int nprocs);

    // DESTRUCTOR
    ~CoordinatorNode();

    /** Runs coordinator. Shouldn't return */
    void run();

    void partition_initial_file();
    void quit_all();
    void handle_get();
    void handle_load();
    void send_and_receive(std::string, int);
    void send_all(std::string content, int tag);


  private:
    // PRIVATE ATTRIBUTES
    int myrank_; // should be 0
    int nprocs_;

    // TODO other stuff like what ranges of coordinate values I hold

};


#endif
