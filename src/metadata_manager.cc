#include <assert.h>
#include <cstring>
#include <iostream>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h> 
#include "metadata_manager.h"

/******************************************************
******** METADATA CONSTRUCTORS & DESTRUCTORS **********
******************************************************/
// TODO
MetaData::MetaData() {

}

MetaData::MetaData(PartitionType partition_type) {
  type_ = partition_type;
}

MetaData::MetaData(PartitionType partition_type,
  std::pair<uint64_t, uint64_t> my_range,
  std::vector<uint64_t> all_ranges) {

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
  assert(type_ == ORDERED_PARTITION || type_ == HASH_PARTITION);


  int buffer_size = 0, pos = 0;
  char* buffer;

  if (type_ == ORDERED_PARTITION) {
    // calculate buffer size
    buffer_size += sizeof(PartitionType); // partition type
    buffer_size += 2 * sizeof(uint64_t); // my_range low and high
    buffer_size += sizeof(int); // length of all_ranges
    buffer_size += all_ranges_.size() * sizeof(uint64_t); // all_ranges contents
    
    // creating buffer
    buffer = new char[buffer_size];
    
    // serialize partition type
    memcpy(&buffer[pos], &type_, sizeof(PartitionType)); 
    pos += sizeof(PartitionType);

    // serialize my range
    memcpy(&buffer[pos], &my_range_.first, sizeof(uint64_t)); 
    pos += sizeof(uint64_t);
    memcpy(&buffer[pos], &my_range_.second, sizeof(uint64_t)); 
    pos += sizeof(uint64_t);

    // serialize all_ranges
    int length = all_ranges_.size();
    memcpy(&buffer[pos], &length, sizeof(int));
    pos += sizeof(int);

    for (std::vector<uint64_t>::iterator it = all_ranges_.begin();
       it != all_ranges_.end(); ++it, pos += sizeof(uint64_t)) {
      uint64_t boundary = *it;
      memcpy(&buffer[pos], &boundary, sizeof(uint64_t));
    }

    assert(pos == buffer_size);

    return std::pair<char*, int>(buffer, buffer_size);

  } else if (type_ == HASH_PARTITION) {

    // calculate buffer size
    buffer_size += sizeof(PartitionType); // partition type
    
    // creating buffer
    buffer = new char[buffer_size];
    
    // serialize partition type
    memcpy(&buffer[pos], &type_, sizeof(PartitionType)); 

    assert(pos + sizeof(PartitionType) == buffer_size);

    return std::pair<char*, int>(buffer, buffer_size);
  } else {
    // shouldn't get here
  }

}

void MetaData::deserialize(char* buffer, int buffer_size) {
  int pos = 0;

  // partition type
  memcpy(&type_, &buffer[pos], sizeof(PartitionType));
  pos += sizeof(PartitionType);

  // for ordered partition
  if (buffer_size > pos) {
    assert(type_ == ORDERED_PARTITION);

    // my range
    memcpy(&my_range_.first, &buffer[pos], sizeof(uint64_t));
    pos += sizeof(uint64_t);
    memcpy(&my_range_.second, &buffer[pos], sizeof(uint64_t));
    pos += sizeof(uint64_t);

    assert(my_range_.first <= my_range_.second);

    // all ranges
    int num_bounds = (int) buffer[pos];
    pos += sizeof(int);
    assert((buffer_size - pos) % 8 == 0);
    for (int i = 0; i < num_bounds; ++i, pos += sizeof(uint64_t)) {
      uint64_t bound;
      memcpy(&bound, &buffer[pos], sizeof(uint64_t));
      all_ranges_.push_back(bound);
    }

  }

}

/******************************************************
***** METADATAMANAGER CONSTRUCTORS & DESTRUCTORS ******
******************************************************/
MetaDataManager::MetaDataManager(std::string& workspace) {
  workspace_ = workspace;
  set_workspace(workspace);
  create_workspace();
}

MetaDataManager::~MetaDataManager() {}


/******************************************************
************** METADATAMANAGER METHODS ****************
******************************************************/
void MetaDataManager::store_metadata(std::string array_name, MetaData& metadata) {
  std::string dir_name = workspace_ + "/" + array_name + "/";  
  struct stat st;
  if(stat(dir_name.c_str(), &st) == -1) { 
    int dir_flag = mkdir(dir_name.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }

  // Open file
  std::string filename = dir_name + METADATA_FILENAME;
  int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_SYNC, S_IRWXU);
  assert(fd != -1);

  // Serialize array schema
  std::pair<char*, int> ret = metadata.serialize();
  char* buffer = ret.first;
  int buffer_size = ret.second; 

  // Store the array schema
  ssize_t r = write(fd, buffer, buffer_size); 

  if (r == -1) {
    throw MetaDataManagerException("Error writing metadata to disk");
  }

  delete [] buffer;
  close(fd);
}

// TODO
MetaData* MetaDataManager::retrieve_metadata(std::string array_name) {
  // The schema to be returned
  MetaData* metadata = new MetaData();

  // Open file
  std::string filename = workspace_ + "/" + array_name + "/" + METADATA_FILENAME;
  int fd = open(filename.c_str(), O_RDONLY);
  assert(fd != -1);

  // Initialize buffer
  struct stat st;
  fstat(fd, &st);
  uint64_t buffer_size = st.st_size;
  char* buffer = new char[buffer_size];

  // Load array schema
  read(fd, buffer, buffer_size);
  metadata->deserialize(buffer, buffer_size);

  // Clean up
  close(fd);
  delete [] buffer;

  return metadata;

}

void MetaDataManager::set_workspace(std::string path) {
  workspace_ = path;
  workspace_ += "/MetaData";
}

void MetaDataManager::create_workspace() {
  struct stat st;

  // If the workspace does not exist, create it
  if(stat(workspace_.c_str(), &st) == -1) { 
    int dir_flag = mkdir(workspace_.c_str(), S_IRWXU);
    assert(dir_flag == 0);
  }

}
