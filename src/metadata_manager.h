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

class MetaDataManager {

  public:

    // CONSTRUCTORS
    MetaDataManager(std::string& workspace);

    // DESTRUCTOR
    ~MetaDataManager();

    // GETTERS
    // METHODS
    void store_metadata(std::string array_name, MetaData& metadata);
    MetaData* retrieve_metadata(std::string array_name);

  private:
    std::string workspace_;
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
