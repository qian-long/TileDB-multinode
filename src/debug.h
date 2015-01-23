#include <iostream>
#include <sstream>
#include <mpi.h>
#ifdef DEBUG

#define DEBUG_MSG(msg) do { \
  int myrank, nprocs; \
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs); \
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank); \
  std::stringstream ss; \
  ss << "[DEBUG PROCESSOR " << myrank << " of " << nprocs << "] " << __FILE__ <<  ":" << __LINE__ << ": " <<  msg << "\n"; \
  std::cout << ss.str(); \
} while (0)


#define LOGGER_DEBUG_MSG(msg, log_info) do { \
  int myrank, nprocs; \
  MPI_Comm_size(MPI_COMM_WORLD, &nprocs); \
  MPI_Comm_rank(MPI_COMM_WORLD, &myrank); \
  std::stringstream ss; \
  ss << "[DEBUG PROCESSOR " << myrank << " of " << nprocs << "] " << log_info << ": " <<  msg << "\n"; \
  std::cout << ss.str(); \
} while(0)

#else

#define DEBUG_MSG(msg)

#define LOGGER_DEBUG_MSG(msg, log_info)

#endif
