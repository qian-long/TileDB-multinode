#include <cstdlib>

#include "util.h"

namespace util {
  // http://en.wikipedia.org/wiki/Reservoir_sampling
  std::vector<uint64_t> resevoir_sample(std::string csvpath, int num_samples) {
    std::vector<uint64_t> results;
    CSVFile csv_in(csvpath, CSVFile::READ);
    CSVLine csv_line;
    int counter = 0;

    while (csv_in >> csv_line) {

      uint64_t cell_id = std::strtoull(csv_line.values()[0].c_str(), NULL, 10);
      // fill the resevoir
      if (counter < num_samples) {
        results.push_back(cell_id);
      } else {

        // replace elements with gradually decreasing probability
        int r = rand() % counter + 1; // 0 to counter inclusive // TODO double check
        if (r < num_samples) {
          results[r] = cell_id;
        }

      }
      counter++;
    }
    return results;
  }

  // TODO optimize to use binary search if needed
  /*
  inline int get_partition_num(std::vector<int64_t> partitions, int64_t cell_id) {
    int recv = 0;
    for (std::vector<int64_t>::iterator it = partitions.begin(); it != partitions.end(); ++it) {
      if (cell_id <= *it) {
        return recv;
      }
      recv++;
    }

    return recv;
  }
  */
  std::string to_string(std::vector<uint64_t> array) {

    if (array.size() == 0) {
      return "[]";
    }

    std::stringstream ss;
    std::vector<uint64_t>::iterator it = array.begin();
    ss << "[";
    ss << *(it++);
    for (; it != array.end(); ++it) {
      ss << ", " << *it;
    }
    ss << "]";

    return ss.str();
  }

  std::string to_string(std::vector<double> array) {

    if (array.size() == 0) {
      return "[]";
    }

    std::stringstream ss;
    std::vector<double>::iterator it = array.begin();
    ss << "[";
    ss << *(it++);
    for (; it != array.end(); ++it) {
      ss << ", " << *it;
    }
    ss << "]";

    return ss.str();
  }


  std::string to_string(std::vector<int> array) {
    if (array.size() == 0) {
      return "[]";
    }

    std::stringstream ss;
    std::vector<int>::iterator it = array.begin();
    ss << "[";
    ss << *(it++);
    for (; it != array.end(); ++it) {
      ss << ", " << *it;
    }
    ss << "]";

    return ss.str();
  }


  std::string to_string(int x) {
    std::stringstream ss;
    ss << x;
    return ss.str();
  }

  std::string to_string(double x) {
    std::stringstream ss;
    ss << x;
    return ss.str();
  }

  std::string to_string(uint64_t x) {
    std::stringstream ss;
    ss << x;
    return ss.str();
  }


  std::string to_string(StorageManager::BoundingCoordinates bounding_coords) {
    std::stringstream ss;
    ss << "[";
    for (int i = 0; i < bounding_coords.size(); ++i) {
      // iterating first coords in pair
      ss << "{(" << bounding_coords[i].first[0];

      for (int j = 1; j < bounding_coords[i].first.size(); ++j) {
        ss << "," << bounding_coords[i].first[j];
      }

      ss << "), (" << bounding_coords[i].second[0];
      // iteratign second coords in pair
      for (int j = 1; j < bounding_coords[i].second.size(); ++j) {
        ss << "," << bounding_coords[i].second[j];
      }

      ss << ")}\n";
    }
    ss << "]";

    return ss.str();
  }

  std::string to_string(uint64_t **array, int nrows, int ncols) {
    std::stringstream ss;
    ss << "{\n";
    for (int i = 0; i < nrows; ++i) {
      ss << "[";
      if (ncols > 0) {
        ss << array[i][0];
        for (int j = 1; j < ncols; ++j) {
          ss << ", " <<*((array + i * ncols) + j);
        }
      }
      ss << "]\n";
    }
    ss << "}";

    return ss.str();
  }

  std::string to_string(std::pair<uint64_t, uint64_t> p) {
    std::stringstream ss;
    ss << "(" << p.first << ", " << p.second << ")";
    return ss.str();
  }
}
