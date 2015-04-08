#include "messages.h"
#include <assert.h>
#include <cstring>
#include <functional>
#include "debug.h"

/******************************************************
 *********************** MESSAGE **********************
 ******************************************************/
std::pair<char*, int> Msg::serialize() {
  throw MessageException("Bad function call");
}

Msg* deserialize_msg(int type, char* buf, int length){
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

std::pair<char*, int> SubarrayMsg::serialize() {
  int buffer_size = 0, pos = 0;
  char* buffer;
  int length;

  // serialize relevant sub components
  std::pair<char*, int> as_pair = array_schema_.serialize();

  // calculating buffer_size
  buffer_size += sizeof(int); // result arrayname length
  buffer_size += result_array_name_.size(); // result_arrayname
  buffer_size += sizeof(int); // array schema length
  buffer_size += as_pair.second; // array schema
  buffer_size += ranges_.size(); // ranges length

  for (int i = 0; i < ranges_.size(); ++i) {
    buffer_size += sizeof(double); // add each range part
  }

  // creating buffer
  buffer = new char[buffer_size];

  // serialize filename
  length = result_array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], result_array_name_.c_str(), length);
  pos += length;

  // serialize array schema
  length = as_pair.second;
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], as_pair.first, length);
  pos += length;

  // serialize ranges
  length = ranges_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);

  std::vector<double>::iterator it = ranges_.begin();
  for (; it != ranges_.end(); it++, pos += sizeof(double)) {
    double extent = *it;

    memcpy(&buffer[pos], &extent, sizeof(double));
  }

  return std::pair<char*, int>(buffer, buffer_size);
}

SubarrayMsg* SubarrayMsg::deserialize(char* buffer, int buffer_length){

  int counter = 0;
  std::stringstream ss;
  std::vector<double> ranges;

  // deserialize array name
  int filename_length = (int) buffer[counter];
  counter += sizeof(int);
  ss.write(&buffer[counter], filename_length);

  std::string array_name = ss.str(); // first arg
  counter += filename_length;

  //deserailize schema
  int arrayschema_length = (int) buffer[counter];
  counter += sizeof(int);
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[counter], arrayschema_length); 

  counter += arrayschema_length;

  //deserialize vector
  int num_doubles = (int) buffer[counter];
  counter += sizeof(int);

  for (int i = 0; i < num_doubles; i++) {
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

LoadMsg::LoadMsg(const std::string filename, ArraySchema& array_schema, PartitionType type) :Msg(LOAD_TAG) {
  filename_ = filename;
  array_schema_ = array_schema;
  type_ = type;
}

std::pair<char*, int> LoadMsg::serialize() {

  int buffer_size = 0;
  int pos = 0;
  char* buffer;
  int length;

  // serialize relevant components
  std::pair<char*, int> as_pair = array_schema_.serialize();

  // calculate buffer size
  buffer_size += sizeof(int); // filename length
  buffer_size += filename_.size(); // filename
  buffer_size += sizeof(int); // array schema length
  buffer_size += as_pair.second; // array schema
  buffer_size += sizeof(PartitionType); // load type

  buffer = new char[buffer_size];

  // serialize filename
  length = filename_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], filename_.c_str(), length);
  pos += length;

  // serialize array schema
  length = as_pair.second;
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], as_pair.first, length);
  pos += length;

  // serialize load type
  memcpy(&buffer[pos], (char *) &type_, sizeof(PartitionType));

  assert(pos + sizeof(PartitionType) == buffer_size);
  return std::pair<char*, int>(buffer, buffer_size);
}

LoadMsg* LoadMsg::deserialize(char* buffer, int buffer_length) {
  std::string filename;
  std::stringstream ss;
  int counter = 0;

  int filename_length;
  memcpy(&filename_length, &buffer[counter], sizeof(int));
  counter += sizeof(int);
  ss.write(&buffer[counter], filename_length);

  filename = ss.str(); // first arg
  counter += filename_length;

  int arrayschema_length;
  memcpy(&arrayschema_length, &buffer[counter], sizeof(int));
  counter += sizeof(int);

  // this is creating space for it on the heap.
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[counter], arrayschema_length); // second arg
  counter += arrayschema_length;

  // load type
  PartitionType type = static_cast<PartitionType>(buffer[counter]);


  // finished parsing
  assert(counter + sizeof(PartitionType) == buffer_length);
  return new LoadMsg(filename, *schema, type);
}


/******************************************************
 ********************* GET MESSAGE ********************
 ******************************************************/
GetMsg::GetMsg() : Msg(GET_TAG) {};

GetMsg::GetMsg(std::string array_name) : Msg(GET_TAG)  {
  array_name_ = array_name;
}

std::pair<char*, int> GetMsg::serialize() {
  int buffer_size = 0, pos = 0;
  char* buffer;

  int length = array_name_.size();
  buffer_size += sizeof(int);
  buffer_size += length;

  buffer = new char[buffer_size];

  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], array_name_.c_str(), length);

  assert(pos + length == buffer_size);
  return std::pair<char*, int>(buffer, buffer_size);
}

GetMsg* GetMsg::deserialize(char* buffer, int buffer_length) {

  //getmsg args
  std::string arrayname;
  std::stringstream ss;
  int counter = 0;

  int array_name_length = (int) buffer[counter];
  counter += sizeof(int);
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

std::pair<char*, int> DefineArrayMsg::serialize() {
  return array_schema_.serialize();
}

DefineArrayMsg* DefineArrayMsg::deserialize(char* buffer, int buffer_length) {
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

std::pair<char*, int> FilterMsg::serialize() {

  int buffer_size = 0;
  int pos = 0;
  char* buffer;
  int length;


  // calculate buffer size
  buffer_size += sizeof(int); // result array name length
  buffer_size += result_array_name_.size(); // result array name
  buffer_size += sizeof(int); // expr str length
  buffer_size += expr_.size(); // expr str
  buffer_size += sizeof(int); // array name length
  buffer_size += array_name_.size(); // array name

  // creating buffer
  buffer = new char[buffer_size];

  // serialize resulting array name
  length = result_array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], result_array_name_.c_str(), length);
  pos += length;

  // serialize expr str
  length = expr_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], expr_.c_str(), length);
  pos += length;

  // serialize array name
  length = array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], array_name_.c_str(), length);

  assert(pos + length == buffer_size);

  return std::pair<char*, int>(buffer, buffer_size);
}

FilterMsg* FilterMsg::deserialize(char* buffer, int buf_length) {
  std::stringstream ss;
  int pos = 0;

  // parse result array name
  int length = (int) buffer[pos];
  pos += sizeof(int);
  ss.write(&buffer[pos], length);
  std::string result_array_name = ss.str(); // first arg
  pos += length;
  ss.str(std::string());

  // parse expr str
  length = (int) buffer[pos];
  pos += sizeof(int);
  ss.write(&buffer[pos], length);
  std::string expr = ss.str();
  pos += length;
  ss.str(std::string());

  // parse array name
  length = (int) buffer[pos];
  pos += sizeof(int);
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
    int num_samples) : Msg(PARALLEL_LOAD_TAG) {

  filename_ = filename;
  type_ = type;
  array_schema_ = array_schema;
  num_samples_ = num_samples;
}

std::pair<char*, int> ParallelLoadMsg::serialize() {
  int buffer_size = 0, pos = 0;
  char* buffer;

  // serialize relevant components
  std::pair<char*, int> as_pair = array_schema_.serialize();

  // calculate buffer size
  buffer_size += sizeof(int); // filename length
  buffer_size += filename_.size(); // filename
  buffer_size += sizeof(PartitionType); // load type
  buffer_size += sizeof(int); // array schema length
  buffer_size += as_pair.second; // array schema
  buffer_size += sizeof(int); // num samples

  // creating buffer
  buffer = new char[buffer_size];

  // serialize filename
  int length = filename_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], filename_.c_str(), length);
  pos += length;

  // serialize load type
  memcpy(&buffer[pos], (char *) &type_, sizeof(PartitionType));
  pos += sizeof(PartitionType);

  // serialize array schema
  length = as_pair.second;
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], as_pair.first, length);
  pos += length;

  // serialize num samples
  memcpy(&buffer[pos], &num_samples_, sizeof(int)); 

  assert(pos + sizeof(int) == buffer_size);
  return std::pair<char*, int>(buffer, buffer_size);
}

ParallelLoadMsg* ParallelLoadMsg::deserialize(char* buffer, int buffer_size) {
  std::string filename;
  int pos = 0;

  // filename
  int length = (int) buffer[pos];
  pos += sizeof(int);
  filename = std::string(&buffer[pos], length);
  pos += length;

  // load type
  PartitionType type = static_cast<PartitionType>(buffer[pos]);
  pos += sizeof(PartitionType);

  // array schema
  memcpy(&length, &buffer[pos], sizeof(int));
  pos += sizeof(int);
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[pos], length);
  pos += length;

  // num samples
  int num_samples;
  memcpy(&num_samples, &buffer[pos], sizeof(int));
  assert(pos + sizeof(int) == buffer_size);
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

std::pair<char*, int> AggregateMsg::serialize() {

  int buffer_size = 0, pos = 0;
  char* buffer;

  // calculate buffer size
  buffer_size += sizeof(int); // array name size
  buffer_size += array_name_.size(); // array name
  buffer_size += sizeof(int); // attr index

  buffer = new char[buffer_size];

  // serialize array name
  int length = array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], array_name_.c_str(), length);
  pos += length;

  // serialize attr int
  memcpy(&buffer[pos], &attr_index_, sizeof(int));
  assert(pos += sizeof(int) == buffer_size);

  return std::pair<char*, int>(buffer, buffer_size);
}

AggregateMsg* AggregateMsg::deserialize(char* buf, int len) {
  int pos = 0;

  // deserialize array name
  int length = (int) buf[pos];
  pos += sizeof(int);

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

std::pair<char*, int> JoinMsg::serialize() {
  int buffer_size = 0, pos = 0;
  char* buffer;

  // Compute lengths
  int A_length = array_name_A_.size();
  int B_length = array_name_B_.size();
  int result_length = result_array_name_.size();
  buffer_size += sizeof(int);
  buffer_size += A_length;
  buffer_size += sizeof(int);
  buffer_size += B_length;
  buffer_size += sizeof(int);
  buffer_size += result_length;

  // creating buffer
  buffer = new char[buffer_size];

  // Serializing array_name_A_
  memcpy(&buffer[pos], &A_length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], array_name_A_.c_str(), A_length);
  pos += A_length;

  // Serializing array_name_B_
  memcpy(&buffer[pos], &B_length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], array_name_B_.c_str(), B_length);
  pos += B_length;

  // Serializing result_array_name_
  memcpy(&buffer[pos], &result_length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], result_array_name_.c_str(), result_length);

  assert(pos + result_length == buffer_size);
  return std::pair<char*, int>(buffer, buffer_size);
}

JoinMsg* JoinMsg::deserialize(char* buffer, int buffer_length) {

  std::string array_name_A;
  std::string array_name_B;
  std::string result_array_name;
  int pos = 0;
  std::stringstream ss;

  // deserializing array_name_A_
  int length = (int) buffer[pos];
  pos += sizeof(int);
  ss.write(&buffer[pos], length);
  pos += length;
  array_name_A = ss.str();
  ss.str(std::string());

  // deserializing array_name_B_
  length = (int) buffer[pos];
  pos += sizeof(int);
  ss.write(&buffer[pos], length);
  pos += length;
  array_name_B = ss.str();
  ss.str(std::string());

  // deserializing result_array_name_
  length = (int) buffer[pos];
  pos += sizeof(int);
  ss.write(&buffer[pos], length);
  result_array_name = ss.str();

  assert(pos + length == buffer_length);
  return new JoinMsg(array_name_A, array_name_B, result_array_name);
}


/******************************************************
 ******************* Samples MESSAGE ******************
 ******************************************************/
SamplesMsg::SamplesMsg() : Msg(SAMPLES_TAG) {};

SamplesMsg::SamplesMsg(std::vector<int64_t> samples) : Msg(SAMPLES_TAG)  {
  samples_ = samples;
}

std::pair<char*, int> SamplesMsg::serialize() {
  int buffer_size = 0, pos = 0;
  char* buffer;

  buffer_size = sizeof(int64_t) * samples_.size();

  buffer = new char[buffer_size];

  for (std::vector<int64_t>::iterator it = samples_.begin();
       it != samples_.end(); ++it, pos += sizeof(int64_t)) {
    int64_t sample = *it;
    memcpy(&buffer[pos], &sample, sizeof(int64_t));
  }

  assert(pos == buffer_size);
  return std::pair<char*, int>(buffer, buffer_size);
}

SamplesMsg* SamplesMsg::deserialize(char* buffer, int buffer_length) {

  std::vector<int64_t> samples; 
  int pos;

  assert(buffer_length % 8 == 0);
  for (pos = 0; pos < buffer_length; pos += sizeof(int64_t)) {
    int64_t sample;
    memcpy(&sample, &buffer[pos], sizeof(int64_t));
    samples.push_back(sample);
  }

  assert(samples.size() * 8 == buffer_length);
  return new SamplesMsg(samples);
}


