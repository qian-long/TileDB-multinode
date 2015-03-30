/**
 * @file array_manager.h
 * @section Manages array metadata
 * This file defines class ArrayManager
 */
#ifndef ARRAYMANAGER_H
#define ARRAYMANAGER_H
#include <string>
#include "messages.h"


#define METADATA_FILENAME "metadata.bkp"

class MetaData {

  public:

    // CONSTRUCTORS
    MetaData();
    MetaData(ParallelLoadMsg::ParallelLoadType partition_type);
    MetaData(ParallelLoadMsg::ParallelLoadType partition_type,
        std::pair<int64_t, int64_t> my_range,
        std::vector<int64_t> all_ranges);

    // DESTRUCTOR
    ~MetaData();

    // GETTERS
    ParallelLoadMsg::ParallelLoadType partition_type() { return type_; }
    std::pair<int64_t, int64_t> my_range() { return my_range_; }
    std::vector<int64_t> all_ranges() { return all_ranges_; }

    // METHODS
    std::pair<char*, int> serialize();
    void deserialize(char* buffer, int buffer_size);

  private:
    ParallelLoadMsg::ParallelLoadType type_; // data is either ordered or hash partitioned across nodes

    std::pair<int64_t, int64_t> my_range_;
    std::vector<int64_t> all_ranges_;

};

class ArrayManager {

  public:

    // CONSTRUCTORS
    ArrayManager();

    // DESTRUCTOR
    ~ArrayManager();

    // GETTERS
    // METHODS
    void store_metadata(std::string array_name, MetaData& metadata);
    MetaData* retrieve_metadata(std::string array_name);

};

#endif
