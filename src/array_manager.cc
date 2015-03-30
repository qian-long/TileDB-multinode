#include <assert.h>
#include <cstring>
#include <iostream>
#include "array_manager.h"

/******************************************************
******** METADATA CONSTRUCTORS & DESTRUCTORS **********
******************************************************/
// TODO
MetaData::MetaData() {

}

MetaData::MetaData(ParallelLoadMsg::ParallelLoadType partition_type) {
  type_ = partition_type;
}

MetaData::MetaData(ParallelLoadMsg::ParallelLoadType partition_type,
  std::pair<int64_t, int64_t> my_range,
  std::vector<int64_t> all_ranges) {

  type_ = partition_type;
  my_range_ = my_range;
  all_ranges_ = all_ranges;
}

MetaData::~MetaData() {

}

/******************************************************
****************** METADATA METHODS *******************
******************************************************/
std::pair<char*, int> MetaData::serialize() {
  assert(type_ == ParallelLoadMsg::ORDERED_PARTITION || type_ == ParallelLoadMsg::HASH_PARTITION);


  int buffer_size = 0, pos = 0;
  char* buffer;

  if (type_ == ParallelLoadMsg::ORDERED_PARTITION) {
    // calculate buffer size
    buffer_size += sizeof(ParallelLoadMsg::ParallelLoadType); // partition type
    buffer_size += 2 * sizeof(int64_t); // my_range low and high
    buffer_size += sizeof(int); // length of all_ranges
    buffer_size += all_ranges_.size() * sizeof(int64_t); // all_ranges contents
    
    // creating buffer
    buffer = new char[buffer_size];
    
    // serialize partition type
    memcpy(&buffer[pos], &type_, sizeof(ParallelLoadMsg::ParallelLoadType)); 
    pos += sizeof(ParallelLoadMsg::ParallelLoadType);

    // serialize my range
    memcpy(&buffer[pos], &my_range_.first, sizeof(int64_t)); 
    pos += sizeof(int64_t);
    memcpy(&buffer[pos], &my_range_.second, sizeof(int64_t)); 
    pos += sizeof(int64_t);

    // serialize all_ranges
    int length = all_ranges_.size();
    memcpy(&buffer[pos], &length, sizeof(int));
    pos += sizeof(int);

    for (std::vector<int64_t>::iterator it = all_ranges_.begin();
       it != all_ranges_.end(); ++it, pos += sizeof(int64_t)) {
      int64_t boundary = *it;
      memcpy(&buffer[pos], &boundary, sizeof(int64_t));
    }

    assert(pos == buffer_size);

    return std::pair<char*, int>(buffer, buffer_size);

  } else if (type_ == ParallelLoadMsg::HASH_PARTITION) {

    // calculate buffer size
    buffer_size += sizeof(ParallelLoadMsg::ParallelLoadType); // partition type
    
    // creating buffer
    buffer = new char[buffer_size];
    
    // serialize partition type
    memcpy(&buffer[pos], &type_, sizeof(ParallelLoadMsg::ParallelLoadType)); 

    assert(pos + sizeof(ParallelLoadMsg::ParallelLoadType) == buffer_size);
    
    return std::pair<char*, int>(buffer, buffer_size);
  } else {
    // shouldn't get here
  } 

}

void MetaData::deserialize(char* buffer, int buffer_size) {
  int pos = 0;

  // partition type
  memcpy(&type_, &buffer[pos], sizeof(ParallelLoadMsg::ParallelLoadType));
  pos += sizeof(ParallelLoadMsg::ParallelLoadType);

  // for ordered partition
  if (buffer_size > pos) {
    assert(type_ == ParallelLoadMsg::ORDERED_PARTITION);

    // my range
    memcpy(&my_range_.first, &buffer[pos], sizeof(int64_t));
    pos += sizeof(int64_t);
    memcpy(&my_range_.second, &buffer[pos], sizeof(int64_t));
    pos += sizeof(int64_t);

    assert(my_range_.first <= my_range_.second);
    
    // all ranges
    int num_bounds = (int) buffer[pos];
    pos += sizeof(int);
    assert((buffer_size - pos) % 8 == 0);
    for (int i = 0; i < num_bounds; ++i, pos += sizeof(int64_t)) {
      int64_t bound;
      memcpy(&bound, &buffer[pos], sizeof(int64_t));
      all_ranges_.push_back(bound);
    }

  }
  
}

/******************************************************
******* ARRAYMANAGER CONSTRUCTORS & DESTRUCTORS *******
******************************************************/
ArrayManager::ArrayManager() {}

ArrayManager::~ArrayManager() {}


/******************************************************
**************** ARRAYMANAGER METHODS *****************
******************************************************/
void ArrayManager::store_metadata(std::string array_name, MetaData& metadata) {

}

MetaData* ArrayManager::retrieve_metadata(std::string array_name) {

}

