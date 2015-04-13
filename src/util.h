#ifndef UTIL_H

#define UTIL_H

#include "csv_file.h"

namespace util {
  std::vector<uint64_t> resevoir_sample(std::string csvpath, int num_samples);

  // TODO optimize to use binary search if needed
  //inline int get_partition_num(std::vector<int64_t> partitions, int64_t cell_id);

  // TO STRINGS
  std::string to_string(std::vector<uint64_t>);
  std::string to_string(int x);
  std::string to_string(double x);


};

#endif
