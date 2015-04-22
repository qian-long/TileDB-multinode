#ifndef UTIL_H
#define UTIL_H

#include "csv_file.h"
#include "storage_manager.h"

namespace util {
  std::vector<uint64_t> resevoir_sample(std::string csvpath, int num_samples);

  // TODO optimize to use binary search if needed
  //inline int get_partition_num(std::vector<int64_t> partitions, int64_t cell_id);

  // TO STRINGS
  // TODO template this shit
  std::string to_string(std::vector<uint64_t>);
  std::string to_string(std::vector<double>);
  std::string to_string(std::vector<int>);
  //std::string to_string(int x);
  //std::string to_string(double x);
  std::string to_string(uint64_t x);
  std::string to_string(StorageManager::BoundingCoordinates bounding_coords);
  std::string to_string(uint64_t **array, int nrows, int ncols);
  //std::string to_string(int nrows, int ncols, uint64_t array[nrows][ncols]);
  std::string to_string(std::pair<uint64_t, uint64_t> p);


};

#endif
