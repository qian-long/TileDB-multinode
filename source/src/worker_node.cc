#include <mpi.h>
#include "worker_node.h"
#include "debug.h"

WorkerNode::WorkerNode(int rank, int nprocs) {
  this->myrank_ = rank;
  this->nprocs_ = nprocs;
}

// TODO
WorkerNode::~WorkerNode() {}

void WorkerNode::run() {
  DEBUG_MSG("I am a worker node");
}

