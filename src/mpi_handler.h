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
    // BLOCKING CALLS
    /** 
     * Sends entire file via mpi to receiving node
     * Blocking call
     */
    void send_file(std::string filepath, int receiver, int tag); 
    /**
     * Receive content in char buffer and write to stream (file or ss, etc)
     * Blocking call
     */
    void receive_content(std::ostream& stream, int sender, int tag);


    /**
     * Send receiver msg saying whether or not to keep receiving data chunks
     * Blocking call
     */
    void send_keep_receiving(bool keep_receiving, int receiver);

    /** TODO test
     * Maintain buffer for each worker, send data to worker only when buffer is full
     * Blocking Call
     */
    void send_content(const char* in_buf, int length, int receiver, int tag);

    void flush_send(int receiver, int tag);
    void flush_all_sends(int tag);

    // Blocking
    void send_samples_msg(SamplesMsg* smsg, int receiver);
    // Blocking
    SamplesMsg* receive_samples_msg(int sender);

    // ALL to ALL Communication
    // blocking
    void send_and_receive_a2a(const char* in_buf, int length, int receiver, std::ostream& file);
    // blocking
    void flush_send_and_recv_a2a(std::ostream& file);
    // blocking
    void flush_send_and_recv_a2a(const char* in_buf, 
        int length, 
        int receiver, 
        std::ostream& file);
    // blocking
    void finish_recv_a2a(std::ostream& file);

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
