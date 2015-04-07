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

MPIHandler::MPIHandler(int myrank, std::vector<int>& node_ids, int64_t mpi_buffer_length, int64_t total_buf_size) {

  int buf_id = 0;
  int64_t buf_size_per_node = total_buf_size / node_ids.size();
  for (std::vector<int>::iterator it = node_ids.begin(); it != node_ids.end(); ++it, ++buf_id) {
    // insert mapping
    node_to_buf_.insert(std::pair<int, int>(*it, buf_id));

    // initialize char buffers and char buffer pointers
    buffers_.push_back(new char[buf_size_per_node]);
    pos_.push_back(0);
  }

  node_ids_ = node_ids;
  myrank_ = myrank;
  mpi_buffer_length_ = mpi_buffer_length;
  total_buf_size_ = total_buf_size;
}

MPIHandler::~MPIHandler() {}


/******************************************************
 *********** POINT TO POINT COMM FUNCTIONS ************
 ******************************************************/
void MPIHandler::send_file(std::string filepath, int receiver, int tag) {
  CSVFile file(filepath, CSVFile::READ); 
  CSVLine line;
  std::stringstream content;

  // TODO throw exception?
  while (file >> line) {
    std::string lstr = line.str() + "\n";
    this->send_content(lstr.c_str(), lstr.size(), receiver, tag);
  }

  this->flush_send(receiver, tag);

}

void MPIHandler::send_keep_receiving(bool keep_receiving, int receiver) {
  std::stringstream ss;
  bool kr = keep_receiving;
  ss.write((char *) &kr, sizeof(bool));
  MPI_Send((char *)ss.str().c_str(), ss.str().length(), MPI_CHAR, receiver, KEEP_RECEIVING_TAG, MPI_COMM_WORLD);
}

bool MPIHandler::receive_keep_receiving(int sender) {

  bool keep_receiving;
  char *buf = new char[mpi_buffer_length_];
  MPI_Status status;
  int length;

  // see if we should coninue receiving
  MPI_Recv((char *)buf, mpi_buffer_length_, MPI_CHAR, sender, KEEP_RECEIVING_TAG, MPI_COMM_WORLD, &status);
  MPI_Get_count(&status, MPI_CHAR, &length);

  //keep_receiving = (bool) buf[0];
  memcpy(&keep_receiving, &buf[0], sizeof(bool));

  delete [] buf;
  return keep_receiving;
}

void MPIHandler::receive_content(std::ostream& stream, int sender, int tag) {
  bool keep_receiving = true;
  char *buf = new char[mpi_buffer_length_];
  MPI_Status status;
  int length;

  do {

    MPI_Recv(buf, mpi_buffer_length_, MPI_CHAR, sender, tag, MPI_COMM_WORLD, &status);
    MPI_Get_count(&status, MPI_CHAR, &length);

    // write to out stream
    stream << std::string(buf, length);

    // see if we should continue receiving
    keep_receiving = this->receive_keep_receiving(sender);

  } while (keep_receiving);

  // Cleanup
  delete [] buf;
}

// Maintain buffer for each worker, send data to worker only when buffer is full
void MPIHandler::send_content(const char* in_buf, int64_t length, int receiver, int tag) {

  std::map<int, int>::iterator search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_id = search->second;
  // TODO handle not found case
  int buf_length = pos_[buf_id];
  if (length + buf_length >= mpi_buffer_length_) {

    // flush buffer to receiver via network
    this->flush_send(receiver, tag, true);

    // send data from in_buf
    assert(pos_[buf_id] == 0);
    int64_t pos = 0;
    while (pos < length) {
      int64_t send_length = std::min(mpi_buffer_length_, length);
      MPI_Send((char *)&in_buf[pos], send_length, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);

      this->send_keep_receiving(true, receiver);
      pos += send_length;
    }


  } else {
    // save data to buffer
    std::memmove(&(buffers_[buf_id][pos_[buf_id]]), in_buf, length);
    pos_[buf_id] += length;
  }

}

void MPIHandler::flush_send(int receiver, int tag, bool keep_receiving) {
  std::map<int, int>::iterator search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_id = search->second;

  // TODO handle not found case
  int buf_length = pos_[buf_id];
  if (buf_length > 0) {
    MPI_Send((char *)buffers_[buf_id], buf_length, MPI_CHAR, receiver, tag, MPI_COMM_WORLD);
    this->send_keep_receiving(keep_receiving, receiver);

    // set pos to 0
    pos_[buf_id] = 0;
  }
}

void MPIHandler::flush_all_sends(int tag) {
  for (std::vector<int>::iterator it = node_ids_.begin(); it != node_ids_.end(); ++it) {
    flush_send(*it, tag);
  }
}

// Sending and Receiving samples
void MPIHandler::send_samples_msg(SamplesMsg* smsg, int receiver) {
  std::pair<char *, int> buf_pair = smsg->serialize();
  send_content(buf_pair.first, buf_pair.second, receiver, SAMPLES_TAG);
  flush_send(receiver, SAMPLES_TAG);
}

SamplesMsg* MPIHandler::receive_samples_msg(int sender) {
  std::stringstream ss;
  receive_content(ss, sender, SAMPLES_TAG);
  return SamplesMsg::deserialize((char *)ss.str().c_str(), ss.str().length());
}


/******************************************************
 ************* ALL TO ALL COMM FUNCTIONS **************
 ******************************************************/
void MPIHandler::send_and_receive_a2a(const char* in_buf, int length, int receiver, std::ostream& file) {
  std::map<int, int>::iterator search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_ind = search->second;
  int buf_length = pos_[buf_ind];
  // TODO handle not found case
  // TODO optimize later if needed
  if (length + buf_length >= mpi_buffer_length_) {
    flush_send_and_recv_a2a(in_buf, length, receiver, file);
  } else {
    // save data to buffer
    std::memmove(&(buffers_[buf_ind][pos_[buf_ind]]), in_buf, length);
    pos_[buf_ind] += length;
  }
}

void MPIHandler::flush_send_and_recv_a2a(const char* in_buf, int length, int receiver, std::ostream& file) {
  
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
  // coordinator node has no data so send dummy data
  scounts[0] = dummy.size();
  ss << dummy;
  send_total = scounts[0];
  
  for (int nodeid = 1; nodeid < nprocs; ++nodeid) {
    std::map<int, int>::iterator search = node_to_buf_.find(nodeid);
  
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

  MPI_Alltoallv((char *)ss.str().c_str(), scounts, sdispls, MPI_CHAR, recvbuf, rcounts, rdispls, MPI_CHAR, MPI_COMM_WORLD);

  file << std::string(recvbuf, recv_total);

  // reset pos
  for (int i = 0; i < pos_.size(); ++i) {
    pos_[i] = 0;
  }

}

void MPIHandler::flush_send_and_recv_a2a(std::ostream& file) {
  flush_send_and_recv_a2a(NULL, 0, 0, file);
}

// keep receiving from other nodes
void MPIHandler::finish_recv_a2a(std::ostream& file) {
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

  std::stringstream ss;
  do {
    /* tell the other processors how much data is coming */
    MPI_Alltoall(scounts, 1, MPI_INT, rcounts, 1, MPI_INT, MPI_COMM_WORLD);
    recv_total = 0;

    for (int i = 0; i < nprocs; ++i) {
      recv_total += rcounts[i];
    }

    // receive content from all nodes
    char recvbuf[recv_total];
    std::stringstream nothing;
    MPI_Alltoallv((char *)nothing.str().c_str(), scounts, sdispls, MPI_CHAR, recvbuf, rcounts, rdispls, MPI_CHAR, MPI_COMM_WORLD);

    if (recv_total > 0 && myrank_ != 0) {
      file << std::string(recvbuf, recv_total);
    }

  } while (recv_total > 0);

}

void MPIHandler::finish_recv_a2a() {
  finish_recv_a2a(std::cout);
}


/******************************************************
 ****************** HELPER FUNCTIONS ******************
 ******************************************************/
bool MPIHandler::buffer_empty(int nodeid) {

  std::map<int, int>::iterator search = node_to_buf_.find(nodeid);

  // TODO exception?
  assert(search != node_to_buf_.end());
  int buf_ind = search->second;

  return (pos_[buf_ind] == 0);
}

bool MPIHandler::all_buffers_empty() {
  assert(pos_.size() == buffers_.size());
  for (int i = 0; i < pos_.size(); ++i) {
    if (pos_[i] > 0) {
      return false;
    }
  }
  return true;
}
