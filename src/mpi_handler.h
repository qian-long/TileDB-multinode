/**
 * @file mpi_handler.h
 * @section DESCRIPTION
 * This file defines class MPIHandler.
 * MPI wrappers for shuffling data among nodes
 */

#ifndef MPI_HANDLER_H
#define MPI_HANDLER_H
#include <string>
#include <vector>
#include <map>
#include <mpi.h>
#include "messages.h"
#include "constants.h"
#include "executor.h"
#include "logger.h"

class MPIHandler {

  public:
    // CONSTRUCTORS
    /** Empty constructor. */
    MPIHandler();

    /** Constructor initializes how many buffers to maintain */
    MPIHandler(int rank,
        std::vector<int>& node_ids,
        Logger* logger,
        int64_t mpi_buffer_length = MPI_BUFFER_LENGTH,
        int64_t total_buf_size = MH_TOTAL_BUF_SIZE);

    // DESTRUCTOR
    /** Empty destructor. */
    ~MPIHandler();

    // METHODS

    /******************************************************
     *********** POINT TO POINT COMM FUNCTIONS ************
     ******************************************************/
    /**
     * Sends entire file via mpi to receiving node
     * File is expected to exist
     * Blocking call
     */
    void send_file(std::string filepath, int receiver, int tag);
    /**
     * Receive content in char buffer and write to stream (file or ss, etc)
     * Blocking call
     */
    void receive_content(std::ostream& stream, int sender, int tag);
    /**
     * Send receiver msg saying whether or not to keep receiving data chunks.
     * Should match with receive_keep_receiving on the other end.
     * Blocking call.
     */
    void send_keep_receiving(bool keep_receiving, int receiver);
    /**
     * Receives msg saying whether or not to keep receiving data chunks.
     * Should match with send_keep_receiving on the other end.
     * Blocking call.
     */
    bool receive_keep_receiving(int sender);
    /**
     * Maintain buffer for each worker, send data to worker only when buffer is full
     * Blocking Call
     */
    void send_content(const char* in_buf, int64_t length, int receiver, int tag);
    /**
     * Flush buffer for receiver
     */
    void flush_send(int receiver, int tag, bool keep_receiving = false);
    /**
     * Flush all buffers
     */
    void flush_all_sends(int tag);

    // Sending and Receiving specific messages
    // TODO maybe consolidate these?
    
    /** Sending samples to receiver Blocking */
    void send_samples_msg(SamplesMsg* smsg, int receiver);
    /** Receiving samples. Blocking */
    SamplesMsg* receive_samples_msg(int sender);
    /** Sending ack. Blocking */
    void send_ack(AckMsg* msg, int receiver);
    /** Receiving ack. Blocking */
    AckMsg* receive_ack(int sender);

    /******************************************************
     ************* ALL TO ALL COMM FUNCTIONS **************
     ******************************************************/
    /** These are all wrappers for MPI's MPI_Alltoall function */

    /**
     * Buffers data and calls flush_send_and_recv_a2a  when full.
     * Blocking call
     */
    void send_and_receive_a2a(const char* in_buf, 
        int length, 
        int receiver, 
        std::vector<std::ostream *> rstreams);

    // Wrapper for the above
    void send_and_receive_a2a(const char* in_buf, 
        int length, 
        int receiver, 
        std::ostream& rstream);


    // if receiver not specified, send to all workers 
    // data received in the rstreams
    void send_and_receive_a2a(BoundingCoordsMsg& msg, 
        std::vector<std::ostream *> rstreams);

    void send_and_receive_a2a(TileMsg& msg, 
        int receiver,
        std::ostream& rstream);

    void send_and_receive_a2a(const char* in_buf, int length, int receiver, 
      std::vector<Tile** > *rtiles, // created tiles are appended here
      int num_attr,
      Executor* executor,
      const ArraySchema& array_schema,
      uint64_t capacity,
      uint64_t start_rank);

    /**
     * One round of data shuffling among all nodes in the cluster (including
     * dummy coordinator). Relies on MPI's varies MPI_Alltoall functions.
     * Data is written to the corresponding receiving streams (rstreams).
     * Blocking call
     */
    void flush_send_and_recv_a2a(const char* in_buf,
        int length,
        int receiver,
        std::vector<std::ostream *> rstreams);

    /**
     * Wrapper for above. 
     * Data from all other nodes are written to the single specified
     * stream.
     */
    void flush_send_and_recv_a2a(const char* in_buf,
        int length,
        int receiver,
        std::ostream& stream);

    /**
     * Flush to network but have nothing to send. 
     * Wrapper
     */
    void flush_send_and_recv_a2a(std::vector<std::ostream *> rstreams);
    void flush_send_and_recv_a2a(std::ostream& file);

    // bypass the internal bufferpool
    // send and receive one physical tile in the all to all shuffle
    // Assume you receive data in order, should be taken care of by MPI
    void send_and_recv_tiles_a2a(const char* in_buf, int length, int receiver, 
      Tile*** received_tiles, // holds one logical tile (array of physical tiles) per sender, use attr_id in tilemsg to determine which attribute (or cordinate)
      int num_attr,
      Executor* executor,
      const ArraySchema& array_schema,
      uint64_t *start_ranks,
      bool *init_received_tiles);

    /**
     * Finish receiving data from other nodes when you are done sending your
     * data.
     * Blocking call.
     */
    void finish_recv_a2a(std::vector<std::ostream *> rstreams);
    void finish_recv_a2a(std::ostream& file);
    void finish_recv_a2a();

    // Same as first one, but form tiles and append to rtiles to the appropriate
    // slots
    void finish_recv_a2a(std::vector<Tile** > **arr_received_tiles,
        int num_attr,
        Executor* executor,
        const ArraySchema& array_schema,
        uint64_t capacity,
        uint64_t *start_ranks);

    // HELPER METHODS
    /** Check if buffer for nodeid is empty */
    bool buffer_empty(int nodeid);

    /** Check if all buffers are empty */
    bool all_buffers_empty();
  private:

    /** all my neighbors in the mpi world */
    std::vector<int> node_ids_;

    /** map node_id to buffer_id */
    std::map<int, int> node_to_buf_;

    /** vector of char buffers */
    std::vector<char *> buffers_;

    /** vector of char buf pos (first empty byte) */
    std::vector<int> pos_;

    /** my rank in the mpi world */
    int myrank_;

    /** max buffer size for an mpi send/receive */
    int64_t mpi_buffer_length_;

    /** total memory size allocated for all the buffers for all nodes */
    int64_t total_buf_size_;


    Logger* logger_;
};
#endif
