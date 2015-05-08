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
#include "metadata_manager.h"

class CoordinatorNode {
  public:
    // CONSTRUCTORS
    CoordinatorNode(int rank, int nprocs, 
        std::string datadir, std::string workspace_base);

    // DESTRUCTOR
    ~CoordinatorNode();

    // GETTERS
    Logger* logger();

    /** Runs coordinator. Shouldn't return */
    void run();

    void send_and_receive(Msg&);
    void send_all(Msg&);
    void send_all(std::string serial_str, int tag);
    void send_all(const char* buffer, uint64_t buffer_size, int tag);
    void quit_all();


    void handle_get(GetMsg& gmsg);
    void handle_load(LoadMsg& lmsg);
    void handle_aggregate();
    int handle_acks();
    void handle_parallel_load(ParallelLoadMsg& pmsg);
    void handle_join(JoinMsg& msg);

    // DIFFERENT LOADING ALGORITHMS

    /**
     * This loading algorithm assumes the coordinator has the initial csv file.
     * The coordinator will inject cell ids to each line of the csv file.
     * The coordinator sorts the injected file and partitions the sorted file into
     * nworkers equal chunks to send to each worker.
     * Each worker waits for their respective partitions. When it
     * has received its share of the data, the worker will perform a local sort,
     * which involves invoking make tiles.
     *
     * The result is that the workers will end up with a globally sorted array.
     *
     * This is the baseline solution.
     */
    void handle_load_ordered_sort(LoadMsg& msg);

    /**
     * This loading algorithm assumes the coordinator has the initial csv file.
     * The coordinator will inject cell ids to each line of the csv file.
     * The coordinator then samples the injected file to determine which partitions to send to
     * each worker. Each worker waits for their respective partitions. When it
     * has received its share of the data, the worker will perform a local sort,
     * which involves sorting on cell id and invoking make tiles.
     *
     * The result is that the workers will end up with a globally sorted array.
     */
    void handle_load_ordered_sample(LoadMsg& msg);

    // assumes coordinator has initial csv
    void handle_load_hash(LoadMsg& pmsg);

    // matches same function in worker_node.h
    void handle_parallel_load_ordered(ParallelLoadMsg& pmsg);

    // matches same function in worker_node.h
    void handle_parallel_load_hash(ParallelLoadMsg& pmsg);

    // DIFFERENT JOIN ALGORITHMS
    /**
     * This is the join method called when the data is order partitioned across
     * all machines. This requires data shuffling.
     */
    void handle_join_ordered(JoinMsg& msg);

    /**
     * Join algo for when data is hash partitioned across machines.
     * This is easy and does not require any data shuffling
     */
    void handle_join_hash(JoinMsg& msg);



    /******** TESTING FUNCTIONS ********/
    // filename must be in the Data directory
    // filename is the part before .csv
    void test_load(std::string array_name, 
        std::string filename, 
        PartitionType partition_type,
        LoadMsg::LoadMethod method = LoadMsg::SORT);
    void test_parallel_load(std::string array_name,
        std::string filename, 
        PartitionType partition_type,
        int num_samples = 100);
    void test_join(std::string array_name_A, std::string array_name_B,
        std::string result_array_name);
    void test_filter(std::string);
    void test_subarray_sparse(std::string);
    void test_subarray_dense(std::string);
    void test_aggregate(std::string);

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
    MetaDataManager* md_manager_;

    /********* HELPER FUNCTIONS ********/
    std::vector<uint64_t> get_partitions(std::vector<uint64_t>, int k);
    int get_receiver(std::vector<uint64_t> partitions, uint64_t cell_id);


};


#endif
