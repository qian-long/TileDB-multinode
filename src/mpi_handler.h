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

class MPIHandler {

  public:
    // CONSTRUCTORS
    /** Empty constructor. */
    MPIHandler();

    /** Constructor initializes how many buffers to maintain */
    MPIHandler(int rank, std::vector<int>& node_ids);

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
    /** TODO test
     * Maintain buffer for each worker, send data to worker only when buffer is full
     * Blocking Call
     */
    void send_content(const char* in_buf, int length, int receiver, int tag);
    /**
     * Flush buffer for receiver
     */
    void flush_send(int receiver, int tag, bool keep_receiving = false);
    /**
     * Flush all buffers
     */
    void flush_all_sends(int tag);

    // Blocking
    void send_samples_msg(SamplesMsg* smsg, int receiver);
    // Blocking
    SamplesMsg* receive_samples_msg(int sender);

    /******************************************************
     ************* ALL TO ALL COMM FUNCTIONS **************
     ******************************************************/
    /** These are all wrappers for MPI's MPI_Alltoall function */

    /**
     * Buffers data and calls flush_send_and_recv_a2a  when full.
     * Blocking call
     */
    void send_and_receive_a2a(const char* in_buf, int length, int receiver, std::ostream& file);
    /**
     * One round of data shuffling among all nodes in the cluster (including
     * dummy coordinator). Relies on MPI's varies MPI_Alltoall functions.
     * Blocking call
     */
    void flush_send_and_recv_a2a(const char* in_buf,
        int length,
        int receiver,
        std::ostream& file);
    void flush_send_and_recv_a2a(std::ostream& file);
    /**
     * Finish receiving data from other nodes when you are done sending your
     * data.
     * Blocking call.
     */
    void finish_recv_a2a(std::ostream& file);
    void finish_recv_a2a();

    // HELPER METHODS
    /** Check if buffer for nodeid is empty */
    bool buffer_empty(int nodeid);

    /** Check if all buffers are empty */
    bool all_buffers_empty();
  private:

    std::vector<int> node_ids_;

    // map node_id to buffer_id
    std::map<int, int> node_to_buf_;

    // vector of char buffers
    std::vector<char *> buffers_;

    // vector of char buf pos (first empty byte)
    std::vector<int> pos_;

    int myrank_;
};
#endif
