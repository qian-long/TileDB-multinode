#include <iostream>
#include <sstream>
#include <mpi.h>
#ifdef DEBUG

#define DEBUG_MSG(msg) \
int myrank, nprocs; \
MPI_Comm_size(MPI_COMM_WORLD, &nprocs); \
MPI_Comm_rank(MPI_COMM_WORLD, &myrank); \
std::stringstream ss; \
ss << "[DEBUG PROCESSOR " << myrank << " of " << nprocs << "] " << __FILE__ <<  ":" << __LINE__ << ": " <<  msg << "\n"; \
std::cout << ss.str(); \

#else

#define DEBUG_MSG(msg)

#endif
