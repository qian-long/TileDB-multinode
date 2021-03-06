/**
 * @file array_manager.h
 * @section Manages array metadata
 * This file defines class ArrayManager
 */
#ifndef ARRAYMANAGER_H
#define ARRAYMANAGER_H
#include <string>
#include <vector>
#include <stdint.h>
#include "constants.h"


#define METADATA_FILENAME "metadata.bkp"
#define METADATA_DIR "MetaData"

class MetaData {

  public:

    // CONSTRUCTORS
    MetaData();
    MetaData(PartitionType partition_type);
    MetaData(PartitionType partition_type,
        std::pair<uint64_t, uint64_t> my_range,
        std::vector<uint64_t> all_ranges);

    // DESTRUCTOR
    ~MetaData();

    // GETTERS
    PartitionType partition_type() { return type_; }
    std::pair<uint64_t, uint64_t> my_range() { return my_range_; }
    std::vector<uint64_t> all_ranges() { return all_ranges_; }


    // METHODS
    std::pair<char*, int> serialize();
    void deserialize(char* buffer, int buffer_size);

  private:
    PartitionType type_; // data is either ordered or hash partitioned across nodes
    std::pair<uint64_t, uint64_t> my_range_;
    std::vector<uint64_t> all_ranges_;

};

class MetaDataManager {

  public:

    // CONSTRUCTORS
    MetaDataManager(std::string& workspace);

    // DESTRUCTOR
    ~MetaDataManager();

    // GETTERS
    std::string workspace() { return workspace_; }

    // METHODS
    void store_metadata(std::string array_name, MetaData& metadata);
    // TODO optimizations: store metadata for arrays in memory 
    MetaData* retrieve_metadata(std::string array_name);

  private:
    std::string workspace_;

    // Private methods
    void set_workspace(std::string path);
    void create_workspace();

};

/** This exception is thrown by ArrayManager. */
class MetaDataManagerException {
 public:
  // CONSTRUCTORS & DESTRUCTORS
  /** Takes as input the exception message. */
  MetaDataManagerException(const std::string& msg) 
      : msg_(msg) {}

  /** Empty destructor. */
  ~MetaDataManagerException() {}

  // ACCESSORS
  /** Returns the exception message. */
  const std::string& what() const { return msg_; }

 private:
  /** The exception message. */
  std::string msg_;
};


#endif
