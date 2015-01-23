/**
 * @file mpi_handler.h
 * @section DESCRIPTION
 * This file defines class MPIHandler.
 * MPI wrappers for shuffling data among nodes
 */

#ifndef MPI_HANDLER_H
#define MPI_HANDLER_H
#include <string>
#include <mpi.h>

class MPIHandler {

  public:
    // CONSTRUCTORS
    /** Empty constructor. */
    MPIHandler();

    // DESTRUCTOR
    /** Empty destructor. */
    ~MPIHandler();

    // METHODS
    /** 
     * Sends entire file via mpi to receiving node
     */
    void send_file(std::string filepath, int receiver, int tag); 
    
    // TODO
    void send_content(std::string content, int receiver);

    /**
     * Receive content in char buffer and store to file,
     * Blocking call
     */
    void receive_file(std::ofstream& file, int sender, int tag);
  private:
};
#endif
