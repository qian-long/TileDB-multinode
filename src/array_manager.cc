#include "array_manager.h"
// TODO
MetaData::MetaData() {

}

MetaData::MetaData(ParallelLoadMsg::ParallelLoadType partition_type) {

}

MetaData::MetaData(ParallelLoadMsg::ParallelLoadType partition_type,
  std::pair<int64_t, int64_t> my_range,
  std::vector<int64_t> all_ranges) {

}

MetaData::~MetaData() {

}

std::pair<char*, int> MetaData::serialize() {

}

void MetaData::deserialize(char* buffer, int buffer_size) {

}

ArrayManager::ArrayManager() {}

ArrayManager::~ArrayManager() {}

void ArrayManager::store_metadata(std::string array_name, MetaData& metadata) {

}

MetaData* ArrayManager::retrieve_metadata(std::string array_name) {

}

