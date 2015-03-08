#include <fstream>
#include <cassert>
#include <cstring>
#include <iostream>
#include "mpi.h"
#include "mpi_handler.h"
#include "csv_file.h"
#include "constants.h"
#include "messages.h"

MPIHandler::MPIHandler() {

}

MPIHandler::MPIHandler(int num_buffers, std::vector<int>& node_ids) {

  int buf_id = 0;
  for (std::vector<int>::iterator it = node_ids.begin(); it != node_ids.end(); ++it, ++buf_id) {
    // insert mapping
    node_to_buf_.insert(std::pair<int, int>(*it, buf_id));

    // initialize char buffers and char buffer pointers
    buffers_.push_back(new char[MPI_BUFFER_LENGTH]);
    pos_.push_back(0);
  }

}

MPIHandler::~MPIHandler() {

}

void MPIHandler::send_file(std::string filepath, int receiver, int tag) {
  CSVFile file(filepath, CSVFile::READ, MPI_BUFFER_LENGTH); 
  CSVLine line;
  std::stringstream content;

  // TODO throw exception?
  while(file >> line) {
    if (content.str().length() + line.str().length() >= MPI_BUFFER_LENGTH) {
      MPI_Send(content.str().c_str(), content.str().length(), 
          MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
      this->send_keep_receiving(true, receiver);
      content.str(std::string()); // clear buffer
    }
    content << line.str() << "\n";
  }

  // final send, tell coordinator to stop receiving
  MPI_Send(content.str().c_str(), content.str().length(), 
      MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
  this->send_keep_receiving(false, receiver);

}

void MPIHandler::send_keep_receiving(bool keep_receiving, int receiver) {
  std::stringstream ss;
  bool kr = keep_receiving;
  ss.write((char *) &kr, sizeof(bool));
  MPI_Send(ss.str().c_str(), ss.str().length(), MPI_CHAR, receiver, KEEP_RECEIVING_TAG, MPI_COMM_WORLD);
}

void MPIHandler::receive_file(std::ofstream& file, int sender, int tag) {
  bool keep_receiving = true;
  char *buf = new char[MPI_BUFFER_LENGTH];
  MPI_Status status;
  int length;

  do {

    MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, sender, tag, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &length);

    // write to file
    file << std::string(buf, length);

    // see if we should coninue receiving
    MPI_Recv(buf, MPI_BUFFER_LENGTH, MPI_CHAR, sender, KEEP_RECEIVING_TAG, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &length);

    keep_receiving = (bool) buf[0];

  } while (keep_receiving);


  // TODO add flushing?
  // Cleanup
  delete [] buf;
}

// TODO Maintain buffer for each worker, send data to worker only when buffer is
// full
void MPIHandler::send_content(char* in_buf, int length, int receiver, int tag) {

  auto search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_id = search->second;
  // TODO handle not found case
  int buf_length = pos_[buf_id];
  if (length + buf_length >= MPI_BUFFER_LENGTH) {
    // send data in buffer, then send data from in_buf
    //std::memmove(send_buffers_[buf_id], buffers_[buf_id], pos);

    // TODO synchronous send
    // sending data from buffer
    // blocking
    //std::memmove(&(buffers_[buf_id][pos]), &keep_receiving, sizeof(bool));
    MPI_Send(buffers_[buf_id], buf_length, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
    this->send_keep_receiving(true, receiver);


    pos_[buf_id] = 0; // resets buffer

    // TODO send data from in_buf
    int pos = 0;
    while (pos < length) {
      int send_length = std::min(MPI_BUFFER_LENGTH, length - pos);
      MPI_Send(&in_buf[pos], send_length, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);

      this->send_keep_receiving(true, receiver);
      pos += send_length;
    }

    // final msg to stop receiving
    this->send_keep_receiving(false, receiver);


  } else {
    // save data to buffer
    std::memmove(&(buffers_[buf_id][pos_[buf_id]]), in_buf, length);
    pos_[buf_id] += length;
  }


}
