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

class MPIHandler {

  public:
    // CONSTRUCTORS
    /** Empty constructor. */
    MPIHandler();

    /** Constructor initializes how many buffers to maintain */
    MPIHandler(std::vector<int>& node_ids);

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
     * Receive content in char buffer and store to file,
     * Blocking call
     */
    void receive_file(std::ofstream& file, int sender, int tag);


    /**
     * Send receiver msg saying whether or not to keep receiving data chunks
     * Blocking call
     */
    void send_keep_receiving(bool keep_receiving, int receiver);

    /** TODO test
     * Maintain buffer for each worker, send data to worker only when buffer is full
     * Used in hash partition parallel load
     * Blocking Call
     */
    void send_content(const char* in_buf, int length, int receiver, int tag);


    // TODO
    void flush_send(int receiver, int tag);
    void flush_all_sends(int tag);

    // NON BLOCKING CALLS
    void send_content_async(const char* in_buf, int length, int receiver, int tag);

  private:

    std::vector<int> node_ids_;

    // map node_id to buffer_id
    std::map<int, int> node_to_buf_;
    
    // vector of char buffers
    std::vector<char *> buffers_; 

    // vector of char buf pos (first empty byte)
    std::vector<int> pos_;

    //std::vector<char *> send_buffers_; // for asychronous calls

};
#endif
