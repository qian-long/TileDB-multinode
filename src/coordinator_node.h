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
#include "logger.h"
#include "mpi_handler.h"

class CoordinatorNode {
  public:
    // CONSTRUCTORS
    CoordinatorNode(int rank, int nprocs);

    // DESTRUCTOR
    ~CoordinatorNode();

    // GETTERS
    Logger* logger();

    /** Runs coordinator. Shouldn't return */
    void run();

    void send_and_receive(Msg&);
    void send_all(Msg&);
    void send_all(std::string serial_str, int tag);
    void send_all(const char* buffer, int buffer_size, int tag);
    void quit_all();


    void handle_get();
    void handle_load();
    void handle_aggregate();
    void handle_ack();
    void handle_parallel_load(ParallelLoadMsg& pmsg);

    // different parallel loads
    void handle_parallel_load_naive(ParallelLoadMsg& pmsg);
    void handle_parallel_load_hash();
    void handle_parallel_load_merge();
    void handle_parallel_load_sampling();
    /******** TESTING FUNCTIONS ********/
    // filename must be in the Data directory
    // filename is the part before .csv
    void test_load(std::string);
    void test_filter(std::string);
    void test_subarray(std::string);
    void test_aggregate(std::string);

    ArraySchema* get_test_arrayschema(std::string array_name);
  private:
    // PRIVATE ATTRIBUTES
    int myrank_; // should be 0
    int nprocs_;
    int nworkers_;
    std::string my_workspace_;
    Logger* logger_;
    Loader* loader_;
    StorageManager* storage_manager_;
    MPIHandler* mpi_handler_;

    // TODO other stuff like what ranges of coordinate values I hold

};


#endif
