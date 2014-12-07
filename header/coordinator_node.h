/**
 * @file coordinator_node.h
 * @section DESCRIPTION
 * This file defines class CoordinatorNode
 */
#ifndef COORDINATORNODE_H
#define COORDINATORNODE_H
#include "constants.h"
#include "array_schema.h"
#include "loader.h"
#include "messages.h"

class CoordinatorNode {
  public:
    // CONSTRUCTORS
    CoordinatorNode(int rank, int nprocs);

    // DESTRUCTOR
    ~CoordinatorNode();

    /** Runs coordinator. Shouldn't return */
    void run();

    void send_and_receive(Msg&);
    void send_all(std::string, int);
    void send_all(Msg&);
    void quit_all();

    void partition_initial_file();

    void handle_get();
    void handle_load();

    /******** TESTING FUNCTIONS ********/
    // filename must be in the Data directory
    // filename is the part before .csv
    void test_load(std::string filename);

    ArraySchema* get_test_arrayschema(std::string array_name);
  private:
    // PRIVATE ATTRIBUTES
    int myrank_; // should be 0
    int nprocs_;

    // TODO other stuff like what ranges of coordinate values I hold

};


#endif
