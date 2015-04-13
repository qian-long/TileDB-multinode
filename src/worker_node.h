/**
 * @file worker_node.h
 * @section DESCRIPTION
 * This file defines class WorkerNode
 */

#ifndef WORKERNODE_H
#define WORKERNODE_H
#include <string>
#include <map>
#include "executor.h"
#include "loader.h"
#include "storage_manager.h"
#include "query_processor.h"
#include "array_schema.h"
#include "messages.h"
#include "logger.h"
#include "mpi_handler.h"
#include "metadata_manager.h"
#include "constants.h"

class WorkerNode {
  public:
    // CONSTRUCTORS
    WorkerNode(int rank, 
        int nprocs, 
        std::string datadir, 
        int64_t mpi_buffer_length = MPI_BUFFER_LENGTH, 
        int64_t mpi_handler_total_buf_size = MH_TOTAL_BUF_SIZE);

    // DESTRUCTOR
    ~WorkerNode();


    /** Runs worker. Listens for instructions from Coordinator. Shouldn't return */
    void run();

    // ACTIONS TO TAKE WHEN RECEIVING A MESSAGE
    int handle_msg(int, Msg*);
    int handle(DefineArrayMsg* msg);
    int handle(LoadMsg* msg); // centralized load (input comes from coordinator)
    int handle(GetMsg* msg);
    int handle(SubarrayMsg* msg);
    int handle(FilterMsg* msg);
    int handle(AggregateMsg* msg);
    int handle(ParallelLoadMsg* msg); // distributed load (input randomly scattered in workers)


    // See CoordinatorNode::handle_load_ordered_sort
    int handle_load_ordered_sort(std::string filename, ArraySchema& array_schema);
    // See CoordinatorNode::handle_load_ordered_sample
    int handle_load_ordered_sample(std::string filename, ArraySchema& array_schema);

    // See CoordinatorNode::handle_load_hash
    int handle_load_hash(std::string filename, ArraySchema& array_schema);

    int handle_parallel_load_ordered(
        std::string filename, 
        ArraySchema& array_schema,
        int num_samples);
    int handle_parallel_load_hash(std::string filename, ArraySchema& array_schema);
    
    void respond_ack(int result, int tag, double time);

    // HELPER FUNCTIONS
    std::string arrayname_to_csv_filename(std::string arrayname);

    /** Picks k samples at random from csvpath. Uses resevoir sampling **/
    //std::vector<int64_t> sample(std::string csvpath, int k);

    /** 
     * Given partition buckets and cell_id, determine which worker the cell
     * should go to.
     * For ordered parallel load
     */
    int get_receiver(std::vector<uint64_t> partitions, uint64_t cell_id);

    

  private:
    // PRIVATE ATTRIBUTES
    int myrank_;
    int nprocs_;
    std::string my_workspace_;
    std::string datadir_;
    Executor* executor_;
    Logger* logger_;
    MPIHandler* mpi_handler_;
    MetaDataManager* md_manager_;
    int64_t mpi_buffer_length_;
    int64_t mpi_handler_total_buf_size_;


    // CATALOGUE of all the arrays in the system
    // map of global array name to local array name
    std::map<std::string, std::string> * arrayname_map_;
    // map of global array name to global array schema
    std::map<std::string, ArraySchema *> * global_schema_map_;
    // map of global array name to local array schema
    std::map<std::string, ArraySchema *> * local_schema_map_;

    /** FOR ISTC TESTING ONLY */
    std::string get_data_path(std::string filename);

};

#endif
