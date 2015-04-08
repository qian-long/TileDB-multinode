/**
 * @file coordinator_node.h
 * @section DESCRIPTION
 * This file defines class CoordinatorNode
 */
#ifndef COORDINATORNODE_H
#define COORDINATORNODE_H
#include "executor.h"
#include "constants.h"
#include "array_schema.h"
#include "loader.h"
#include "messages.h"
#include "logger.h"
#include "mpi_handler.h"

class CoordinatorNode {
  public:
    // CONSTRUCTORS
    CoordinatorNode(int rank, int nprocs, std::string datadir);

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


    void handle_get(GetMsg& gmsg);
    void handle_load(LoadMsg& lmsg);
    void handle_aggregate();
    void handle_ack();
    void handle_parallel_load(ParallelLoadMsg& pmsg);

    // different parallel loads
    // assumes workers have initial csv partitions
    void handle_load_ordered(LoadMsg& pmsg);

    // assumes coordinator has initial csv
    void handle_load_hash(LoadMsg& pmsg);

    // matches same function in worker_node.h
    void handle_parallel_load_ordered(ParallelLoadMsg& pmsg);

    // matches same function in worker_node.h
    void handle_parallel_load_hash(ParallelLoadMsg& pmsg);

    /******** TESTING FUNCTIONS ********/
    // filename must be in the Data directory
    // filename is the part before .csv
    void test_load(std::string array_name, std::string filename, PartitionType partition_type);
    void test_parallel_load(std::string);
    void test_filter(std::string);
    void test_subarray(std::string);
    void test_aggregate(std::string);
    void test_join(std::string);

    ArraySchema* get_test_arrayschema(std::string array_name);

  private:
    // PRIVATE ATTRIBUTES
    int myrank_; // should be 0
    int nprocs_;
    int nworkers_;
    std::string my_workspace_;
    Logger* logger_;
    Executor* executor_;
    MPIHandler* mpi_handler_;
    std::string datadir_;

    /********* HELPER FUNCTIONS ********/
    std::vector<int64_t> get_partitions(std::vector<int64_t>, int k);


};


#endif
