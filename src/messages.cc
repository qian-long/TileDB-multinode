#include "messages.h"
#include <assert.h>
#include <cstring>
#include <functional>
#include "debug.h"

/******************************************************
 *********************** MESSAGE **********************
 ******************************************************/
std::pair<char*, uint64_t> Msg::serialize() {
  throw MessageException("Bad function call");
}

Msg* deserialize_msg(int type, char* buf, uint64_t length){
  switch(type){
    case GET_TAG:
      return GetMsg::deserialize(buf, length);
    case DEFINE_ARRAY_TAG:
      return DefineArrayMsg::deserialize(buf, length);
    case LOAD_TAG:
      return LoadMsg::deserialize(buf, length);
    case SUBARRAY_TAG:
      return SubarrayMsg::deserialize(buf, length);
    case FILTER_TAG:
      return FilterMsg::deserialize(buf, length);
    case AGGREGATE_TAG:
      return AggregateMsg::deserialize(buf, length);
    case PARALLEL_LOAD_TAG:
      return ParallelLoadMsg::deserialize(buf, length);
    case JOIN_TAG:
      return JoinMsg::deserialize(buf, length);
    case ACK_TAG:
      return AckMsg::deserialize(buf, length);
  }
  throw MessageException("trying to deserailze msg of unknown type");
}

/******************************************************
 ******************* SubArray MESSAGE *****************
 ******************************************************/

SubarrayMsg::SubarrayMsg(std::string result_name, ArraySchema schema, std::vector<double> ranges) : Msg(SUBARRAY_TAG) {
  result_array_name_ = result_name;
  ranges_ = ranges;
  array_schema_ = schema;
}

std::pair<char*, uint64_t> SubarrayMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;
  uint64_t length;

  // serialize relevant sub components
  std::pair<char*, uint64_t> as_pair = array_schema_.serialize();

  // calculating buffer_size
  buffer_size += sizeof(size_t); // result arrayname length
  buffer_size += result_array_name_.size(); // result_arrayname
  buffer_size += sizeof(uint64_t); // array schema length
  buffer_size += as_pair.second; // array schema
  buffer_size += ranges_.size(); // ranges length

  for (int i = 0; i < ranges_.size(); ++i) {
    buffer_size += sizeof(double); // add each range part
  }

  // creating buffer
  buffer = new char[buffer_size];

  // serialize filename
  length = result_array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], result_array_name_.c_str(), length);
  pos += length;

  // serialize array schema
  length = as_pair.second;
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], as_pair.first, length);
  pos += length;

  // serialize ranges
  length = ranges_.size();
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);

  std::vector<double>::iterator it = ranges_.begin();
  for (; it != ranges_.end(); it++, pos += sizeof(double)) {
    double extent = *it;

    memcpy(&buffer[pos], &extent, sizeof(double));
  }

  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

SubarrayMsg* SubarrayMsg::deserialize(char* buffer, uint64_t buffer_length){

  uint64_t counter = 0;
  std::stringstream ss;
  std::vector<double> ranges;

  // deserialize array name
  size_t filename_length;
  memcpy(&filename_length, &buffer[counter], sizeof(size_t));
  counter += sizeof(size_t);
  ss.write(&buffer[counter], filename_length);

  std::string array_name = ss.str(); // first arg
  counter += filename_length;

  //deserailize schema
  uint64_t arrayschema_length;
  memcpy(&arrayschema_length, &buffer[counter], sizeof(uint64_t));
  counter += sizeof(uint64_t);
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[counter], arrayschema_length); 

  counter += arrayschema_length;

  //deserialize vector
  size_t num_doubles;
  memcpy(&num_doubles, &buffer[counter], sizeof(size_t));
  counter += sizeof(size_t);

  for (size_t i = 0; i < num_doubles; i++) {
    double extent;
    memcpy(&extent, &buffer[counter], sizeof(double));
    ranges.push_back(extent);
    counter += sizeof(double);
  }

  return new SubarrayMsg(array_name, *schema, ranges);

}

/******************************************************
 ********************* LOAD MESSAGE *******************
 ******************************************************/
LoadMsg::LoadMsg() : Msg(LOAD_TAG) { }

LoadMsg::LoadMsg(const std::string filename, 
    ArraySchema& array_schema, 
    PartitionType type,
    LoadMethod method,
    uint64_t num_samples) :Msg(LOAD_TAG) {
  filename_ = filename;
  array_schema_ = array_schema;
  type_ = type;
  method_ = method;
  num_samples_ = num_samples;
}

std::pair<char*, uint64_t> LoadMsg::serialize() {

  uint64_t buffer_size = 0;
  uint64_t pos = 0;
  char* buffer;
  uint64_t length;

  // serialize relevant components
  std::pair<char*, uint64_t> as_pair = array_schema_.serialize();

  // calculate buffer size
  buffer_size += sizeof(size_t); // filename length
  buffer_size += filename_.size(); // filename
  buffer_size += sizeof(uint64_t); // array schema length
  buffer_size += as_pair.second; // array schema
  buffer_size += sizeof(PartitionType); // partition type
  buffer_size += sizeof(LoadMethod); // load method (sort or sample)
  buffer_size += sizeof(uint64_t); // num_samples

  buffer = new char[buffer_size];

  // serialize filename
  length = filename_.size();
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], filename_.c_str(), length);
  pos += length;

  // serialize array schema
  length = as_pair.second;
  memcpy(&buffer[pos], &length, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  memcpy(&buffer[pos], as_pair.first, length);
  pos += length;

  // serialize partition type
  memcpy(&buffer[pos], (char *) &type_, sizeof(PartitionType));
  pos += sizeof(PartitionType);

  // serialize load method
  memcpy(&buffer[pos], (char *) &method_, sizeof(LoadMethod));
  pos += sizeof(LoadMethod);

  // serialize num_samples
  memcpy(&buffer[pos], (char *) &num_samples_, sizeof(uint64_t));
  assert(pos + sizeof(uint64_t) == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

LoadMsg* LoadMsg::deserialize(char* buffer, uint64_t buffer_length) {
  std::string filename;
  std::stringstream ss;
  uint64_t counter = 0;

  size_t filename_length;
  memcpy(&filename_length, &buffer[counter], sizeof(size_t));
  counter += sizeof(size_t);
  ss.write(&buffer[counter], filename_length);

  filename = ss.str(); // first arg
  counter += filename_length;

  uint64_t arrayschema_length;
  memcpy(&arrayschema_length, &buffer[counter], sizeof(uint64_t));
  counter += sizeof(uint64_t);

  // this is creating space for it on the heap.
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[counter], arrayschema_length); // second arg
  counter += arrayschema_length;

  // partition type
  PartitionType type;
  memcpy(&type, &buffer[counter], sizeof(PartitionType));
  counter += sizeof(PartitionType);

  // load method
  LoadMethod method;
  memcpy(&method, &buffer[counter], sizeof(LoadMethod));
  counter += sizeof(LoadMethod);

  // num samples
  uint64_t num_samples;
  memcpy(&num_samples, &buffer[counter], sizeof(uint64_t));
  counter += sizeof(uint64_t);

  // sanity check
  assert(counter == buffer_length);

  return new LoadMsg(filename, *schema, type, method, num_samples);
}


/******************************************************
 ********************* GET MESSAGE ********************
 ******************************************************/
GetMsg::GetMsg() : Msg(GET_TAG) {};

GetMsg::GetMsg(std::string array_name) : Msg(GET_TAG)  {
  array_name_ = array_name;
}

std::pair<char*, uint64_t> GetMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  size_t length = array_name_.size();
  buffer_size += sizeof(size_t);
  buffer_size += length;

  buffer = new char[buffer_size];

  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], array_name_.c_str(), length);

  assert(pos + length == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

GetMsg* GetMsg::deserialize(char* buffer, uint64_t buffer_length) {

  //getmsg args
  std::string arrayname;
  std::stringstream ss;
  uint64_t counter = 0;

  size_t array_name_length;
  memcpy(&array_name_length, &buffer[counter], sizeof(size_t));
  counter += sizeof(size_t);
  ss.write(&buffer[counter], array_name_length);

  arrayname = ss.str(); // first arg
  return new GetMsg(arrayname);
}


/******************************************************
 *************** ARRAYSCHEMA MESSAGE ******************
 ******************************************************/
DefineArrayMsg::DefineArrayMsg() : Msg(DEFINE_ARRAY_TAG) {};

DefineArrayMsg::DefineArrayMsg(ArraySchema& schema) : Msg(DEFINE_ARRAY_TAG)  {
  array_schema_ = schema;
}

std::pair<char*, uint64_t> DefineArrayMsg::serialize() {
  return array_schema_.serialize();
}

DefineArrayMsg* DefineArrayMsg::deserialize(char* buffer, uint64_t buffer_length) {
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(buffer, buffer_length);
  return new DefineArrayMsg(*schema);
}


/******************************************************
 ****************** FILTER MESSAGE ********************
 ******************************************************/
FilterMsg::FilterMsg() : Msg(FILTER_TAG) {}

FilterMsg::FilterMsg(
    std::string& array_name,
    std::string& expression, 
    std::string& result_array_name) : Msg(FILTER_TAG) {
  array_name_ = array_name;
  expr_ = expression;
  result_array_name_ = result_array_name;
}

std::pair<char*, uint64_t> FilterMsg::serialize() {

  uint64_t buffer_size = 0;
  uint64_t pos = 0;
  char* buffer;
  uint64_t length;


  // calculate buffer size
  buffer_size += sizeof(size_t); // result array name length
  buffer_size += result_array_name_.size(); // result array name
  buffer_size += sizeof(size_t); // expr str length
  buffer_size += expr_.size(); // expr str
  buffer_size += sizeof(size_t); // array name length
  buffer_size += array_name_.size(); // array name

  // creating buffer
  buffer = new char[buffer_size];

  // serialize resulting array name
  length = result_array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], result_array_name_.c_str(), length);
  pos += length;

  // serialize expr str
  length = expr_.size();
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], expr_.c_str(), length);
  pos += length;

  // serialize array name
  length = array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], array_name_.c_str(), length);

  assert(pos + length == buffer_size);

  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

FilterMsg* FilterMsg::deserialize(char* buffer, uint64_t buf_length) {
  std::stringstream ss;
  uint64_t pos = 0;

  // parse result array name
  size_t length;
  memcpy(&length, &buffer[pos], sizeof(size_t));
  pos += sizeof(size_t);
  ss.write(&buffer[pos], length);
  std::string result_array_name = ss.str(); // first arg
  pos += length;
  ss.str(std::string());

  // parse expr str
  memcpy(&length, &buffer[pos], sizeof(size_t));
  pos += sizeof(size_t);
  ss.write(&buffer[pos], length);
  std::string expr = ss.str();
  pos += length;
  ss.str(std::string());

  // parse array name
  length = (size_t) buffer[pos];
  pos += sizeof(size_t);
  ss.write(&buffer[pos], length);
  std::string array_name = ss.str();

  // finished parsing
  assert(length + pos == buf_length);

  return new FilterMsg(array_name, expr, result_array_name);
}


/*********************************************************
 ***************** PARALLEL LOAD MESSAGE *****************
 *********************************************************/
ParallelLoadMsg::ParallelLoadMsg() : Msg(PARALLEL_LOAD_TAG) {}

ParallelLoadMsg::ParallelLoadMsg(
    std::string filename,
    PartitionType type,
    ArraySchema& array_schema,
    uint64_t num_samples) : Msg(PARALLEL_LOAD_TAG) {

  filename_ = filename;
  type_ = type;
  array_schema_ = array_schema;
  num_samples_ = num_samples;
}

std::pair<char*, uint64_t> ParallelLoadMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  // serialize relevant components
  std::pair<char*, uint64_t> as_pair = array_schema_.serialize();

  // calculate buffer size
  buffer_size += sizeof(size_t); // filename length
  buffer_size += filename_.size(); // filename
  buffer_size += sizeof(PartitionType); // load type
  buffer_size += sizeof(uint64_t); // array schema length
  buffer_size += as_pair.second; // array schema
  buffer_size += sizeof(uint64_t); // num samples

  // creating buffer
  buffer = new char[buffer_size];

  // serialize filename
  size_t length = filename_.size();
  memcpy(&buffer[pos], &length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], filename_.c_str(), length);
  pos += length;

  // serialize load type
  memcpy(&buffer[pos], (char *) &type_, sizeof(PartitionType));
  pos += sizeof(PartitionType);

  // serialize array schema
  uint64_t schema_length = as_pair.second;
  memcpy(&buffer[pos], &schema_length, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  memcpy(&buffer[pos], as_pair.first, schema_length);
  pos += schema_length;

  // serialize num samples
  memcpy(&buffer[pos], &num_samples_, sizeof(uint64_t)); 

  assert(pos + sizeof(uint64_t) == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

ParallelLoadMsg* ParallelLoadMsg::deserialize(char* buffer, uint64_t buffer_size) {
  std::string filename;
  uint64_t pos = 0;

  // filename
  size_t length;
  memcpy(&length, &buffer[pos], sizeof(size_t));
  pos += sizeof(size_t);
  filename = std::string(&buffer[pos], length);
  pos += length;

  // load type
  PartitionType type = static_cast<PartitionType>(buffer[pos]);
  pos += sizeof(PartitionType);

  // array schema
  memcpy(&length, &buffer[pos], sizeof(uint64_t));
  pos += sizeof(uint64_t);
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[pos], length);
  pos += length;

  // num samples
  uint64_t num_samples;
  memcpy(&num_samples, &buffer[pos], sizeof(uint64_t));
  assert(pos + sizeof(uint64_t) == buffer_size);
  return new ParallelLoadMsg(filename, type, *schema, num_samples);
}



/******************************************************
 ******************* AGGREGATE MESSAGE ****************
 ******************************************************/

AggregateMsg::AggregateMsg() : Msg(AGGREGATE_TAG) {};

AggregateMsg::AggregateMsg(std::string array_name, int attr_index): Msg(AGGREGATE_TAG) {
  attr_index_ = attr_index;
  array_name_ = array_name;
}

std::pair<char*, uint64_t> AggregateMsg::serialize() {

  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  // calculate buffer size
  buffer_size += sizeof(uint64_t); // array name size
  buffer_size += array_name_.size(); // array name
  buffer_size += sizeof(int); // attr index

  buffer = new char[buffer_size];

  // serialize array name
  uint64_t length = array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(uint64_t));
  pos += sizeof(uint64_t);
  memcpy(&buffer[pos], array_name_.c_str(), length);
  pos += length;

  // serialize attr int
  memcpy(&buffer[pos], &attr_index_, sizeof(int));
  assert(pos += sizeof(int) == buffer_size);

  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

AggregateMsg* AggregateMsg::deserialize(char* buf, uint64_t len) {
  uint64_t pos = 0;

  // deserialize array name
  uint64_t length = (uint64_t) buf[pos];
  pos += sizeof(uint64_t);

  std::string array_name = std::string(&buf[pos], length);
  pos += length;

  // deserialize attribute index
  int attr_index = (int) buf[pos];

  assert(pos + sizeof(int) == len);
  return new AggregateMsg(array_name, attr_index);
}

/******************************************************
 ********************* JOIN MESSAGE *******************
 ******************************************************/
JoinMsg::JoinMsg() : Msg(JOIN_TAG) {};

JoinMsg::JoinMsg(std::string array_name_A,
                 std::string array_name_B,
                 std::string result_array_name) : Msg(JOIN_TAG)  {
  array_name_A_ = array_name_A;
  array_name_B_ = array_name_B;
  result_array_name_ = result_array_name;
}

std::pair<char*, uint64_t> JoinMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  // Compute lengths
  size_t A_length = array_name_A_.size();
  size_t B_length = array_name_B_.size();
  size_t result_length = result_array_name_.size();
  buffer_size += sizeof(size_t);
  buffer_size += A_length;
  buffer_size += sizeof(size_t);
  buffer_size += B_length;
  buffer_size += sizeof(size_t);
  buffer_size += result_length;

  // creating buffer
  buffer = new char[buffer_size];

  // Serializing array_name_A_
  memcpy(&buffer[pos], &A_length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], array_name_A_.c_str(), A_length);
  pos += A_length;

  // Serializing array_name_B_
  memcpy(&buffer[pos], &B_length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], array_name_B_.c_str(), B_length);
  pos += B_length;

  // Serializing result_array_name_
  memcpy(&buffer[pos], &result_length, sizeof(size_t));
  pos += sizeof(size_t);
  memcpy(&buffer[pos], result_array_name_.c_str(), result_length);

  assert(pos + result_length == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

JoinMsg* JoinMsg::deserialize(char* buffer, uint64_t buffer_length) {

  std::string array_name_A;
  std::string array_name_B;
  std::string result_array_name;
  uint64_t pos = 0;
  std::stringstream ss;

  // deserializing array_name_A_
  size_t length;
  memcpy(&length, &buffer[pos], sizeof(size_t));
  pos += sizeof(size_t);
  ss.write(&buffer[pos], length);
  pos += length;
  array_name_A = ss.str();
  ss.str(std::string());

  // deserializing array_name_B_
  memcpy(&length, &buffer[pos], sizeof(size_t));
  pos += sizeof(size_t);
  ss.write(&buffer[pos], length);
  pos += length;
  array_name_B = ss.str();
  ss.str(std::string());

  // deserializing result_array_name_
  memcpy(&length, &buffer[pos], sizeof(size_t));
  pos += sizeof(size_t);
  ss.write(&buffer[pos], length);
  result_array_name = ss.str();

  assert(pos + length == buffer_length);
  return new JoinMsg(array_name_A, array_name_B, result_array_name);
}

/******************************************************
 ******************** ACK MESSAGE *********************
 ******************************************************/
AckMsg::AckMsg() : Msg(ACK_TAG) {};

AckMsg::AckMsg(Result r, int tag, double time) : Msg(ACK_TAG) {
  result_ = r;
  tag_ = tag;
  time_ = time;
}

std::pair<char*, uint64_t> AckMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  buffer_size = sizeof(Result); // result
  buffer_size += sizeof(int); // tag
  buffer_size += sizeof(double); // time

  buffer = new char[buffer_size];

  // serialize result
  memcpy(&buffer[pos], &result_, sizeof(Result));
  pos += sizeof(Result);

  // serialize tag
  memcpy(&buffer[pos], &tag_, sizeof(int));
  pos += sizeof(int);

  // serialize time
  memcpy(&buffer[pos], &time_, sizeof(double));
  pos += sizeof(double);

  assert(pos == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

AckMsg* AckMsg::deserialize(char* buffer, uint64_t buffer_length) {

  // getmsg args
  int pos = 0;

  // deserialize result
  Result result;
  memcpy(&result, &buffer[pos], sizeof(Result));
  pos += sizeof(Result);

  // deserialize tag
  int tag;
  memcpy(&tag, &buffer[pos], sizeof(int));
  pos += sizeof(int);

  // deserialize time
  double time;
  memcpy(&time, &buffer[pos], sizeof(double));
  pos += sizeof(double);

  // sanity check
  assert(pos == buffer_length);
  return new AckMsg(result, tag, time);
}

std::string AckMsg::to_string() {
  std::stringstream ss;
  switch (tag_) {
    case GET_TAG:
      ss << "GET";
      break;
    case DEFINE_ARRAY_TAG:
      ss << "DEFINE_ARRAY_TAG";
      break;
    case LOAD_TAG:
      ss << "LOAD";
      break;
    case SUBARRAY_TAG:
      ss << "SUBARRAY";
      break;
    case FILTER_TAG:
      ss << "FILTER";
      break;
    case AGGREGATE_TAG:
      ss << "AGGREGATE";
      break;
    case PARALLEL_LOAD_TAG:
      ss << "PARALLEL_LOAD";
      break;
    case JOIN_TAG:
      ss << "JOIN_TAG";
      break;
    default:
      break;
  }

  if (result_ == DONE) {
    ss << "[DONE]";
  } else {
    assert(result_ == ERROR);
    ss << "[ERROR]";
  }

  ss << " Time[" << time_ << " secs]";

  return ss.str();
}

/******************************************************
 ******************* Samples MESSAGE ******************
 ******************************************************/
SamplesMsg::SamplesMsg() : Msg(SAMPLES_TAG) {};

SamplesMsg::SamplesMsg(std::vector<uint64_t> samples) : Msg(SAMPLES_TAG)  {
  samples_ = samples;
}

std::pair<char*, uint64_t> SamplesMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  buffer_size = sizeof(uint64_t) * samples_.size();

  buffer = new char[buffer_size];

  for (std::vector<uint64_t>::iterator it = samples_.begin();
       it != samples_.end(); ++it, pos += sizeof(uint64_t)) {
    uint64_t sample = *it;
    memcpy(&buffer[pos], &sample, sizeof(uint64_t));
  }

  assert(pos == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

SamplesMsg* SamplesMsg::deserialize(char* buffer, uint64_t buffer_length) {

  std::vector<uint64_t> samples; 
  uint64_t pos;

  assert(buffer_length % 8 == 0);
  for (pos = 0; pos < buffer_length; pos += sizeof(uint64_t)) {
    uint64_t sample;
    memcpy(&sample, &buffer[pos], sizeof(uint64_t));
    samples.push_back(sample);
  }

  assert(samples.size() * 8 == buffer_length);
  return new SamplesMsg(samples);
}

/******************************************************
 *************** Bounding Coords MESSAGE **************
 ******************************************************/
BoundingCoordsMsg::BoundingCoordsMsg() : Msg(BOUNDING_COORDS_TAG) {};

BoundingCoordsMsg::BoundingCoordsMsg(
    StorageManager::BoundingCoordinates bounding_coords) :
  Msg(BOUNDING_COORDS_TAG)  {
  bounding_coords_ = bounding_coords;
}

std::pair<char*, uint64_t> BoundingCoordsMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  int num_dim = 0;

  if (bounding_coords_.size() > 0) {
    num_dim = bounding_coords_[0].first.size();
  }

  buffer_size = sizeof(int); // number of dimensions
  buffer_size += 2 * num_dim * bounding_coords_.size() * sizeof(double); // size of bounding coordinates

  buffer = new char[buffer_size];

  // serialize num dim
  memcpy(&buffer[pos], &num_dim, sizeof(int));
  pos += sizeof(int);

  // serialize bounding coordinates
  for (int i = 0; i < bounding_coords_.size(); ++i) {
    // serialize first coords in pair
    for (std::vector<double>::iterator it = bounding_coords_[i].first.begin();
        it != bounding_coords_[i].first.end(); ++it) {
      double coord = *it;
      memcpy(&buffer[pos], &coord, sizeof(double));
      pos += sizeof(double);
    }

    // serialize second coords in pair
    for (std::vector<double>::iterator it = bounding_coords_[i].second.begin();
        it != bounding_coords_[i].second.end(); ++it) {
      double coord = *it;
      memcpy(&buffer[pos], &coord, sizeof(double));
      pos += sizeof(double);
    }

  }

  assert(pos == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

BoundingCoordsMsg* BoundingCoordsMsg::deserialize(char* buffer, uint64_t buffer_length) {

  StorageManager::BoundingCoordinates bounding_coords;
  if (buffer_length == 0) {
    return new BoundingCoordsMsg(bounding_coords);
  }

  uint64_t pos = 0;

  // deserialize num_dim
  int num_dim;
  memcpy(&num_dim, &buffer[pos], sizeof(int));
  pos += sizeof(int);

  // deserialize all bounding coords
  for (; pos < buffer_length; pos += 2 * num_dim * sizeof(double)) {
    std::vector<double> coords1;
    std::vector<double> coords2;
    for (int i = 0; i < num_dim; ++i) {
      double coord;
      memcpy(&coord, &buffer[pos + i*sizeof(double)], sizeof(double));
      coords1.push_back(coord);
    }

    int offset = num_dim * sizeof(double);
    for (int i = 0; i < num_dim; ++i) {
      double coord;
      memcpy(&coord, &buffer[pos + i*sizeof(double) + offset], sizeof(double));
      coords2.push_back(coord);
    }

    bounding_coords.push_back(
        StorageManager::BoundingCoordinatesPair(coords1, coords2));
  }

  // TODO fix when buffer is empty...
  if (buffer_length > 0) {
    assert(pos == buffer_length);
  }
  return new BoundingCoordsMsg(bounding_coords);
}

/******************************************************
 ******************** TILE MESSAGE ********************
 ******************************************************/
TileMsg::TileMsg() : Msg(BOUNDING_COORDS_TAG) {};

TileMsg::TileMsg(std::string array_name,
    int attr_id,
    const char* payload,
    uint64_t num_cells,
    uint64_t cell_size) : Msg(TILE_TAG)  {
  array_name_ = array_name;
  attr_id_ = attr_id;
  payload_ = payload;
  num_cells_ = num_cells;
  cell_size_ = cell_size;
}

std::pair<char*, uint64_t> TileMsg::serialize() {
  uint64_t buffer_size = 0, pos = 0;
  char* buffer;

  buffer_size = sizeof(int); // array_name length
  buffer_size += array_name_.size(); // array_name
  buffer_size += sizeof(int); // attr_id
  buffer_size += sizeof(uint64_t); // num_cells
  buffer_size += sizeof(uint64_t); // cell_size
  buffer_size += payload_size(); // payload size

  buffer = new char[buffer_size];

  // serialize array name
  int length = array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);

  memcpy(&buffer[pos], array_name_.c_str(), length);
  pos += length;

  // serialize attr id
  memcpy(&buffer[pos], &attr_id_, sizeof(int));
  pos += sizeof(int);

  // serialize num cells
  memcpy(&buffer[pos], &num_cells_, sizeof(uint64_t));
  pos += sizeof(uint64_t);

  // serialize cell size
  memcpy(&buffer[pos], &cell_size_, sizeof(uint64_t));
  pos += sizeof(uint64_t);

  // serialize payload
  memcpy(&buffer[pos], payload_, payload_size());
  pos += payload_size();

  assert(pos == buffer_size);
  return std::pair<char*, uint64_t>(buffer, buffer_size);
}

TileMsg* TileMsg::deserialize(char* buffer, uint64_t buffer_length) {
  std::stringstream ss;
  uint64_t pos = 0;

  // deserialize array name
  int length;
  memcpy(&length, &buffer[pos], sizeof(int));
  pos += sizeof(int);
  ss.write(&buffer[pos], length);
  std::string array_name = ss.str();

  pos += length;
  ss.str(std::string());

  // deserialize attr id
  int attr_id;
  memcpy(&attr_id, &buffer[pos], sizeof(int));
  pos += sizeof(int);

  // deserialize num cells
  uint64_t num_cells;
  memcpy(&num_cells, &buffer[pos], sizeof(uint64_t));
  pos += sizeof(uint64_t);

  // deserialize cell size
  uint64_t cell_size;
  memcpy(&cell_size, &buffer[pos], sizeof(uint64_t));
  pos += sizeof(uint64_t);

  // deserialize payload
  uint64_t payload_size = cell_size * num_cells;

  assert(payload_size < buffer_length);
  const char *payload = new char[payload_size];
  memcpy((char *)payload, &buffer[pos], payload_size);
  pos += payload_size;

  assert(pos == buffer_length);
  return new TileMsg(array_name, attr_id, payload, num_cells, cell_size);
}


