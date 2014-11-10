/**
 * @file coordinator_node.h
 * @section DESCRIPTION
 * This file defines class CoordinatorNode
 */
#ifndef COORDINATORNODE_H
#define COORDINATORNODE_H

class CoordinatorNode {
  public:
    // CONSTRUCTORS
    CoordinatorNode(int rank, int nprocs);

    // DESTRUCTOR
    ~CoordinatorNode();

    /** Runs coordinator. Shouldn't return */
    void run();

    void partition_initial_file();


  private:
    // PRIVATE ATTRIBUTES
    int myrank_; // should be 0
    int nprocs_;

    // TODO other stuff like what ranges of coordinate values I hold

};


#endif
