#include <fstream>
#include <cassert>
#include <cstring>
#include <iostream>
#include "mpi.h"
#include "mpi_handler.h"
#include "csv_file.h"
#include "constants.h"
#include "messages.h"
#include "util.h"

MPIHandler::MPIHandler() {}

MPIHandler::MPIHandler(int myrank, std::vector<int>& node_ids,
    Logger* logger, int64_t mpi_buffer_length, int64_t total_buf_size) {

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
  logger_ = logger;
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

// Sending and Receiving acks
void MPIHandler::send_ack(AckMsg* msg, int receiver) {
  std::pair<char *, int> buf_pair = msg->serialize();
  send_content(buf_pair.first, buf_pair.second, receiver, SAMPLES_TAG);
  flush_send(receiver, ACK_TAG);
}

AckMsg* MPIHandler::receive_ack(int sender) {
  std::stringstream ss;
  receive_content(ss, sender, ACK_TAG);
  return AckMsg::deserialize((char *)ss.str().c_str(), ss.str().length());
}

/******************************************************
 ************* ALL TO ALL COMM FUNCTIONS **************
 ******************************************************/

/******************************************************
 **              send_and_recieve_a2a                **
 ******************************************************/
void MPIHandler::send_and_receive_a2a(const char* in_buf, int length, int receiver, std::vector<std::ostream *> rstreams) {
  std::map<int, int>::iterator search = node_to_buf_.find(receiver);
  assert(search != node_to_buf_.end());
  int buf_ind = search->second;
  int buf_length = pos_[buf_ind];
  // TODO handle not found case
  // TODO optimize later if needed
  // TODO change to total_buffer_length_
  if (length + buf_length >= mpi_buffer_length_) {
    flush_send_and_recv_a2a(in_buf, length, receiver, rstreams);
  } else {
    // save data to buffer
    std::memmove(&(buffers_[buf_ind][pos_[buf_ind]]), in_buf, length);
    pos_[buf_ind] += length;
  }
}

void MPIHandler::send_and_receive_a2a(const char* in_buf, int length, int receiver, std::ostream& rstream) {
  std::vector<std::ostream *> rstreams;
  rstreams.push_back(&rstream);
  send_and_receive_a2a(in_buf, length, receiver, rstreams);
}

void MPIHandler::send_and_receive_a2a(BoundingCoordsMsg& msg, std::vector<std::ostream *> rstreams) {

  for (std::vector<int>::iterator it = node_ids_.begin();
      it != node_ids_.end(); ++it) {
    int receiver = *it;
    if (receiver != MASTER) {
      std::pair<char*, int> buf_pair = msg.serialize();
      send_and_receive_a2a(buf_pair.first, buf_pair.second, receiver, rstreams);
    }
  }

}

void MPIHandler::send_and_receive_a2a(TileMsg& msg,
    int receiver,
    std::ostream& rstream) {
  std::pair<char*, uint64_t> buf_pair = msg.serialize();
  send_and_receive_a2a(buf_pair.first, buf_pair.second, receiver, rstream);
}

/******************************************************
 **            flush_send_and_recv_a2a               **
 ******************************************************/
void MPIHandler::flush_send_and_recv_a2a(const char* in_buf, int length,
  int receiver, std::ostream& rstream) {
  std::vector<std::ostream *> rstreams;
  rstreams.push_back(&rstream);
  flush_send_and_recv_a2a(in_buf, length, receiver, rstreams);
}

void MPIHandler::flush_send_and_recv_a2a(std::vector<std::ostream *> rstreams) {
  flush_send_and_recv_a2a(NULL, 0, myrank_, rstreams);
}

void MPIHandler::flush_send_and_recv_a2a(std::ostream& file) {
  flush_send_and_recv_a2a(NULL, 0, myrank_, file);
}

// TODO fix segfault when called without send_and_receive
void MPIHandler::flush_send_and_recv_a2a(const char* in_buf, int length, int receiver, std::vector<std::ostream *> rstreams) {
  assert(rstreams.size() == 1 || rstreams.size() == node_ids_.size() + 1);

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
      ss.write(buffers_[buf_ind], pos_[buf_ind]);
      if (length > 0 && nodeid == receiver && in_buf != NULL) {
        scounts[nodeid] += length;
        ss.write(in_buf, length);
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

  // start after rcounts[0] offset
  assert(rcounts[0] == rdispls[1]);

  // ignore "blob" msg from coordinator
  // copy the data from each receiver into corresponding receiving streams
  if (rstreams.size() == 1) {
    // ignore "blob" msg from coordinator
    (*rstreams[0]) << std::string(&recvbuf[rdispls[1]], recv_total - rcounts[0]);
  } else {
    for (int i = 1; i < nprocs; ++i) {
      // ignore master and yourself
      if (i != myrank_) {
        (*rstreams[i]) << std::string(&recvbuf[rdispls[i]], rcounts[i]);
      }
    }
  }

  // reset pos
  for (int i = 0; i < pos_.size(); ++i) {
    pos_[i] = 0;
  }
}

void MPIHandler::send_and_recv_tiles_a2a(const char* in_buf,
    int length, int receiver,
    Tile*** received_tiles,
    int num_attr,
    Executor* executor,
    const ArraySchema& array_schema,
    uint64_t *start_ranks,
    bool *init_received_tiles) {


  // blocking
  int nprocs = node_ids_.size() + 1;
  int nworkers = node_ids_.size();
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

  uint64_t capacity = array_schema.capacity();

  std::string dummy("blob");
  // coordinator node has no data so send dummy data
  scounts[0] = dummy.size();
  ss << dummy;
  send_total = scounts[0];

  for (int nodeid = 1; nodeid < nprocs; ++nodeid) {
    if (length > 0 && receiver == nodeid) {
      scounts[nodeid] = length;
      ss.write(in_buf, length); // don't use <<
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

  // start after rcounts[0] offset
  assert(rcounts[0] == rdispls[1]);

  // length of actual contents (minus blob from coordinator)
  uint64_t buf_len = recv_total - rcounts[0];

  if (buf_len > 0) {
    for (int sender = 1; sender < nprocs; ++sender) {
      if (sender == myrank_) {
        assert(rcounts[sender] == 0);
        continue;
      }

      if (rcounts[sender] > 0) {
        TileMsg* new_msg = TileMsg::deserialize(&recvbuf[rdispls[sender]], rcounts[sender]);

        if (new_msg->attr_id() == 0) {
          received_tiles[sender] = new Tile*[num_attr + 1];
          init_received_tiles[sender] = true;
        }

        received_tiles[sender][new_msg->attr_id()] = executor->storage_manager()->new_tile(array_schema, new_msg->attr_id(), start_ranks[sender], capacity);
        received_tiles[sender][new_msg->attr_id()]->set_payload(new_msg->payload(), new_msg->payload_size());

        // safe b/c set_payload does memcpy
        delete new_msg;
      }

    }

  }

}



/******************************************************
 **                finish_recv_a2a                   **
 ******************************************************/
void MPIHandler::finish_recv_a2a() {
  finish_recv_a2a(std::cout);
}

void MPIHandler::finish_recv_a2a(std::ostream& file) {
  std::vector<std::ostream *> rstreams;
  rstreams.push_back(&file);
  finish_recv_a2a(rstreams);
}

// keep receiving from other nodes
void MPIHandler::finish_recv_a2a(std::vector<std::ostream *> rstreams) {
  uint64_t recv_total = 0;
  uint64_t send_total = 0;
  int nprocs = node_ids_.size() + 1;
  int scounts[nprocs];
  int rcounts[nprocs];
  int sdispls[nprocs];
  int rdispls[nprocs];

  assert (rstreams.size() == 1 || rstreams.size() == nprocs);
  bool keep_receiving = true;
  bool coordinator_last_round = false;
  do {

    // initialize every round
    std::stringstream ss;
    for (int i = 0; i < nprocs; ++i) {
      scounts[i] = 0;
      sdispls[i] = 0;
      rcounts[i] = 0;
      rdispls[i] = 0;
    }

    if (myrank_ == MASTER && !coordinator_last_round) {
      /*
       * Whenever a worker node sends data, they send a blob to the coordinator (in
       * flush_send_and_recv_a2a).
       * Here, coordinator will send "blob" out to all workers as long as at least one
       * worker is still sending data. When Coordinator sends nothing, that is a
       * signal to everyone else that they can stop receiving. Maybe this is hacky?
       */
      std::string dummy("blob");
      for (int nodeid = 1; nodeid < nprocs; ++nodeid) {
        scounts[nodeid] = dummy.size();
        ss << dummy;
        sdispls[nodeid] = sdispls[nodeid-1] + scounts[nodeid-1];
        send_total += dummy.size();
      }
    }


    /* tell the other processors how much data is coming */
    MPI_Alltoall(scounts, 1, MPI_INT, rcounts, 1, MPI_INT, MPI_COMM_WORLD);

    // how much data I am expecting from everyone else
    recv_total = rcounts[0];
    rdispls[0] = 0;
    for (int i = 1; i < nprocs; ++i) {
      recv_total += rcounts[i];
      rdispls[i] = rcounts[i-1] + rdispls[i-1];
    }

    // receive content from all nodes
    char recvbuf[recv_total];

    MPI_Alltoallv((char *)ss.str().c_str(), scounts, sdispls, MPI_CHAR, recvbuf, rcounts, rdispls, MPI_CHAR, MPI_COMM_WORLD);

    if (recv_total > 0) {
      // start after rcounts[0] offset
      assert(rcounts[0] == rdispls[1]);

      if (rstreams.size() == 1) {
        // ignore "blob" msg from coordinator
        rstreams[0]->write(&recvbuf[rdispls[1]], recv_total - rcounts[0]);
      } else {
      // copy the data from each receiver into corresponding receiving streams
        for (int i = 1; i < nprocs; ++i) {
          // ignore master and yourself
          if (i != myrank_) {
            rstreams[i]->write(&recvbuf[rdispls[i]], rcounts[i]);
          }
        }
      }

    } else {
      assert(recv_total == 0);

      if (myrank_ == MASTER) {
        if (!coordinator_last_round) {
          coordinator_last_round = true;
        } else {
          keep_receiving = false;
        }

      } else {
        keep_receiving = false;
      }
    }

  } while (keep_receiving);

}

// keep receiving from other nodes
void MPIHandler::finish_recv_a2a(std::vector<Tile** > **arr_received_tiles,
    int num_attr,
    Executor* executor,
    const ArraySchema& array_schema,
    uint64_t capacity,
    uint64_t *start_ranks) {

  uint64_t recv_total;
  int nprocs = node_ids_.size() + 1;
  int nworkers = node_ids_.size();
  int scounts[nprocs];
  int rcounts[nprocs];
  int sdispls[nprocs];
  int rdispls[nprocs];

  bool keep_receiving = true;
  bool coordinator_last_round = false;



  // holds one logical tile per sender, append to rtiles when finish receiving
  // all physical tiles of same rank from sender
  // TODO cleanup
  Tile*** received_tiles = new Tile**[nprocs];
  do {

    // initialize every round
    std::stringstream ss;
    for (int i = 0; i < nprocs; ++i) {
      scounts[i] = 0;
      sdispls[i] = 0;
      rcounts[i] = 0;
      rdispls[i] = 0;
    }

    if (myrank_ == MASTER && !coordinator_last_round) {
      /*
       * Whenever a worker node sends data, they send a blob to the coordinator (in
       * flush_send_and_recv_a2a).
       * Here, coordinator will send "blob" out to all workers as long as at least one
       * worker is still sending data. When Coordinator sends nothing, that is a
       * signal to everyone else that they can stop receiving. Maybe this is hacky?
       */
      std::string dummy("blob");
      for (int nodeid = 1; nodeid < nprocs; ++nodeid) {
        scounts[nodeid] = dummy.size();
        ss << dummy;
        sdispls[nodeid] = sdispls[nodeid-1] + scounts[nodeid-1];
      }
    }


    /* tell the other processors how much data is coming */
    MPI_Alltoall(scounts, 1, MPI_INT, rcounts, 1, MPI_INT, MPI_COMM_WORLD);

    // how much data I am expecting from everyone else
    recv_total = rcounts[0];
    rdispls[0] = 0;
    for (int i = 1; i < nprocs; ++i) {
      recv_total += rcounts[i];
      rdispls[i] = rcounts[i-1] + rdispls[i-1];
    }

    // receive content from all nodes
    char recvbuf[recv_total];

    // received one round of physical tiles (one attr or coord tile)
    MPI_Alltoallv((char *)ss.str().c_str(), scounts, sdispls, MPI_CHAR, recvbuf, rcounts, rdispls, MPI_CHAR, MPI_COMM_WORLD);

    if (recv_total > 0) {
      // start after rcounts[0] offset
      assert(rcounts[0] == rdispls[1]);

      // ignore "blob" msg from coordinator
      // TODO form tiles for each sender
      uint64_t buf_len = recv_total - rcounts[0];
      if (buf_len > 0) {
        for (int sender = 1; sender < nprocs; ++sender) {
          if (sender == myrank_) {
            assert(rcounts[sender] == 0);
            continue;
          }

          if (rcounts[sender] > 0) {

            TileMsg* new_msg = TileMsg::deserialize(&recvbuf[rdispls[sender]], rcounts[sender]);

            if (new_msg->attr_id() == 0) {
              received_tiles[sender] = new Tile*[num_attr + 1];
            }


            received_tiles[sender][new_msg->attr_id()] = executor->storage_manager()->new_tile(array_schema, new_msg->attr_id(), start_ranks[sender], capacity);
            received_tiles[sender][new_msg->attr_id()]->set_payload(new_msg->payload(), new_msg->payload_size());

            if (new_msg->attr_id() == num_attr) {
              //rtiles->push_back(received_tiles[sender]);
              arr_received_tiles[sender]->push_back(received_tiles[sender]);
              start_ranks[sender] = start_ranks[sender] + 1;
            }

            // Safe to do b/c set_payload does memcpy
            delete new_msg;
          }

        }
      }
    } else {
      assert(recv_total == 0);

      if (myrank_ == MASTER) {
        if (!coordinator_last_round) {
          coordinator_last_round = true;
        } else {
          keep_receiving = false;
        }

      } else {
        keep_receiving = false;
      }
    }

  } while (keep_receiving);

  // cleanup
  delete [] received_tiles;
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
