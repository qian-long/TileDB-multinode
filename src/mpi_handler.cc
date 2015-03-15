#include <fstream>
#include <cassert>
#include <cstring>
#include <iostream>
#include "mpi.h"
#include "mpi_handler.h"
#include "csv_file.h"
#include "constants.h"
#include "messages.h"

MPIHandler::MPIHandler() {}

MPIHandler::MPIHandler(int myrank, std::vector<int>& node_ids) {

  int buf_id = 0;
  for (std::vector<int>::iterator it = node_ids.begin(); it != node_ids.end(); ++it, ++buf_id) {
    // insert mapping
    node_to_buf_.insert(std::pair<int, int>(*it, buf_id));

    // initialize char buffers and char buffer pointers
    buffers_.push_back(new char[MPI_BUFFER_LENGTH]);
    pos_.push_back(0);
  }

  node_ids_ = node_ids;
  //a2a_buf_len_ = 0;
  myrank_ = myrank;
}

MPIHandler::~MPIHandler() {}

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
void MPIHandler::send_content(const char* in_buf, int length, int receiver, int tag) {

  auto search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_id = search->second;
  // TODO handle not found case
  int buf_length = pos_[buf_id];
  if (length + buf_length >= MPI_BUFFER_LENGTH) {

    // synchronous send
    // sending data from buffer
    // blocking
    MPI_Send(buffers_[buf_id], buf_length, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
    this->send_keep_receiving(true, receiver);


    pos_[buf_id] = 0; // resets buffer

    // send data from in_buf
    int pos = 0;
    while (pos < length) {
      int send_length = std::min(MPI_BUFFER_LENGTH, length - pos);
      MPI_Send(&in_buf[pos], send_length, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);

      this->send_keep_receiving(true, receiver);
      pos += send_length;
    }

    // final msg to stop receiving
    //this->send_keep_receiving(false, receiver);


  } else {
    // save data to buffer
    std::memmove(&(buffers_[buf_id][pos_[buf_id]]), in_buf, length);
    pos_[buf_id] += length;
  }

}

void MPIHandler::flush_send(int receiver, int tag) {
  auto search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_id = search->second;
  // TODO handle not found case
  int buf_length = pos_[buf_id];
  if (buf_length > 0) {
    MPI_Send(buffers_[buf_id], buf_length, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
    this->send_keep_receiving(false, receiver);

    // set pos to 0
    pos_[buf_id] = 0;
  }

}

void MPIHandler::flush_all_sends(int tag) {
  for (std::vector<int>::iterator it = node_ids_.begin(); it != node_ids_.end(); ++it) {
    flush_send(*it, tag);
  }
}

void MPIHandler::send_and_receive_a2a(const char* in_buf, int length, int receiver, std::ofstream& file) {
  auto search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_ind = search->second;
  int buf_length = pos_[buf_ind];
  // TODO handle not found case
  // TODO optimize later if needed
  if (length + buf_length >= MPI_BUFFER_LENGTH) {
    flush_send_and_recv_a2a(in_buf, length, receiver, file);
  } else {
    // save data to buffer
    std::memmove(&(buffers_[buf_ind][pos_[buf_ind]]), in_buf, length);
    pos_[buf_ind] += length;
  }
}

void MPIHandler::flush_send_and_recv_a2a(const char* in_buf, int length, int receiver, std::ofstream& file) {
  
  // blocking
  int nprocs = node_ids_.size() + 1;
  int scounts[nprocs];
  int rcounts[nprocs];
  int sdispls[nprocs];
  int rdispls[nprocs];
 
  int send_total = 0;
  std::stringstream ss;

  for (int i = 0; i < nprocs; ++i) {
    scounts[i] = 0;
    sdispls[i] = 0;
    rcounts[i] = 0;
    rdispls[i] = 0;
  }


  std::string dummy("blob");
  // coordinator node has no data
  scounts[0] = dummy.size();
  ss << dummy;
  send_total = scounts[0];
  
  for (int nodeid = 1; nodeid < nprocs; ++nodeid) {
    auto search = node_to_buf_.find(nodeid);
  
    int buf_ind;
    if (search == node_to_buf_.end()) {
      assert(nodeid == myrank_);

      scounts[nodeid] = 0;
    } else {

      buf_ind = search->second;
      scounts[nodeid] = pos_[buf_ind];
      ss << std::string(buffers_[buf_ind], pos_[buf_ind]);
      if (length > 0 && nodeid == receiver && in_buf != NULL) {
        scounts[nodeid] += length;
        ss << in_buf;
      }
    }
    sdispls[nodeid] = sdispls[nodeid - 1] + scounts[nodeid - 1];
    send_total += scounts[nodeid];
  }

  /* tell the other processors how much data is coming */
  MPI_Alltoall(scounts, 1, MPI_INT, rcounts, 1, MPI_INT, MPI_COMM_WORLD);
  int recv_total = rcounts[0]; 
  rdispls[0] = 0;
  for (int i = 1; i < nprocs; ++i) {
    recv_total += rcounts[i];
    rdispls[i] = rcounts[i-1] + rdispls[i-1];
  }

  // receive content from all nodes
  char recvbuf[recv_total];

  MPI_Alltoallv(ss.str().c_str(), scounts, sdispls, MPI_CHAR, recvbuf, rcounts, rdispls, MPI_CHAR, MPI_COMM_WORLD);

  file << std::string(recvbuf, recv_total);

  // reset pos
  for (int i = 0; i < pos_.size(); ++i) {
    pos_[i] = 0;
  } 

}

void MPIHandler::flush_send_and_recv_a2a(std::ofstream& file) {
  flush_send_and_recv_a2a(NULL, 0, 0, file);
}

// keep receiving from other nodes
void MPIHandler::finish_recv_a2a(std::ofstream& file) {
  int recv_total;
  int nprocs = node_ids_.size() + 1;
  int scounts[nprocs];
  int rcounts[nprocs];
  int sdispls[nprocs];
  int rdispls[nprocs];
 
  for (int i = 0; i < nprocs; ++i) {
    scounts[i] = 0;
    sdispls[i] = 0;
    rcounts[i] = 0;
    rdispls[i] = 0;
  }

  do {

    /* tell the other processors how much data is coming */
    MPI_Alltoall(scounts, 1, MPI_INT, rcounts, 1, MPI_INT, MPI_COMM_WORLD);
    recv_total = 0;
    
    for (int i = 0; i < nprocs; ++i) {
      recv_total += rcounts[i];
    }

    // receive content from all nodes
    char recvbuf[recv_total];
    std::stringstream ss;

    MPI_Alltoallv(ss.str().c_str(), scounts, sdispls, MPI_CHAR, recvbuf, rcounts, rdispls, MPI_CHAR, MPI_COMM_WORLD);

      if (recv_total > 0 && myrank_ != 0) {
        file << std::string(recvbuf, recv_total);
      }

  } while (recv_total > 0);


}

