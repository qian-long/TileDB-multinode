#include "messages.h"
#include <assert.h>
#include <cstring>
#include "debug.h"
#include <functional>
#include <stdexcept>      // std::invalid_argument

/******************************************************
 *********************** MESSAGE **********************
 ******************************************************/
std::pair<char*, int> Msg::serialize() {
  throw std::bad_function_call();
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
    case AGGREGATE_TAG:
      return AggregateMsg::deserialize(buf, length);
    case PARALLEL_LOAD_TAG:
      DEBUG_MSG("parallel load tag detected");
      return ParallelLoadMsg::deserialize(buf, length);
  }
  throw std::invalid_argument("trying to deserailze msg of unknown type");
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

LoadMsg::LoadMsg(const std::string filename, ArraySchema& array_schema) :Msg(LOAD_TAG) {
  filename_ = filename;
  array_schema_ = array_schema;
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

  assert(pos + length == buffer_size);
  return std::pair<char*, int>(buffer, buffer_size);
}

LoadMsg* LoadMsg::deserialize(char* buffer, int buffer_length) {
  std::string filename;
  std::stringstream ss;
  int counter = 0;

  int filename_length = (int) buffer[counter];
  counter += sizeof(int);
  ss.write(&buffer[counter], filename_length);

  filename = ss.str(); // first arg
  counter += filename_length;

  int arrayschema_length = (int) buffer[counter];

  counter += sizeof(int);

  // this is creating space for it on the heap.
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[counter], arrayschema_length); // second arg

  // finished parsing
  assert(counter + arrayschema_length == buffer_length);
  return new LoadMsg(filename, *schema);
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

template<class T>
FilterMsg<T>::FilterMsg() : Msg(FILTER_TAG) {}

template<class T>
FilterMsg<T>::FilterMsg(
    const ArraySchema::CellType& attr_type, 
    ArraySchema& array_schema, 
    Predicate<T>& predicate, 
    const std::string& result_array_name) : Msg(FILTER_TAG) {
  attr_type_ = attr_type;
  array_schema_ = array_schema;
  predicate_ = predicate;
  result_array_name_ = result_array_name;
}

// TODO fix
template<class T>
FilterMsg<T>::~FilterMsg() {
  //delete &array_schema_;
  //delete &predicate_;
}

template<class T>
std::pair<char*, int> FilterMsg<T>::serialize() {

  int buffer_size = 0;
  int pos = 0;
  char* buffer;
  int length;

  // serialize relevent components
  std::pair<char*, int> pred_pair = predicate_.serialize();
  std::pair<char*, int> as_pair = array_schema_.serialize();

  // calculate buffer size
  buffer_size += sizeof(ArraySchema::CellType); // type of filter col
  buffer_size += sizeof(int); // result array name length
  buffer_size += result_array_name_.size(); // result array name
  buffer_size += sizeof(int); // predicate length
  buffer_size += pred_pair.second; // predicate
  buffer_size += sizeof(int); // array schema length
  buffer_size += as_pair.second; // array schema

  // creating buffer
  buffer = new char[buffer_size];

  // serialize attr_type_
  length = sizeof(ArraySchema::CellType);
  memcpy(&buffer[pos], &attr_type_, length);
  pos += length;

  // serialize resulting array name
  length = result_array_name_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], result_array_name_.c_str(), length);
  pos += length;

  // serialize predicate
  length = pred_pair.second;
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], pred_pair.first, length);
  pos += length;

  // serialize array schema
  length = as_pair.second;
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], as_pair.first, length);

  assert(pos + length == buffer_size);

  return std::pair<char*, int>(buffer, buffer_size);
}

template<class T>
FilterMsg<T>* FilterMsg<T>::deserialize(char* buffer, int buf_length) {
  std::stringstream ss;
  int pos = 0;

  // parse attribute type
  ArraySchema::CellType datatype = static_cast<ArraySchema::CellType>(buffer[pos]);
  pos += sizeof(ArraySchema::CellType);

  // parse result array name
  int length = (int) buffer[pos];
  pos += sizeof(int);
  ss.write(&buffer[pos], length);
  std::string result_array_name = ss.str(); // first arg
  pos += length;

  // parse predicate
  length = (int) buffer[pos];

  pos += sizeof(int);
  Predicate<T>* pred = (Predicate<T>::deserialize(&buffer[pos], length));
  pos += length;

  // parse array schema
  length = (int) buffer[pos];
  pos += sizeof(int);
  ArraySchema *schema = new ArraySchema();
  schema->deserialize(&buffer[pos], length);

  // finished parsing
  assert(length + pos == buf_length);

  return new FilterMsg(datatype, *schema, *pred, result_array_name);
}


/*********************************************************
 ***************** PARALLEL LOAD MESSAGE *****************
 *********************************************************/
ParallelLoadMsg::ParallelLoadMsg() : Msg(PARALLEL_LOAD_TAG) {}

ParallelLoadMsg::ParallelLoadMsg(
    std::string filename,
    LoadType load_type,
    ArraySchema& array_schema) : Msg(PARALLEL_LOAD_TAG) {

  filename_ = filename;
  load_type_ = load_type;
  array_schema_ = array_schema;
}

std::pair<char*, int> ParallelLoadMsg::serialize() {
  int buffer_size = 0, pos = 0;
  char* buffer;

  // serialize relevant components
  std::pair<char*, int> as_pair = array_schema_.serialize();

  // calculate buffer size
  buffer_size += sizeof(int); // filename length
  buffer_size += filename_.size(); // filename
  buffer_size += sizeof(LoadType); // load type
  buffer_size += sizeof(int); // array schema length
  buffer_size += as_pair.second; // array schema

  // creating buffer
  buffer = new char[buffer_size];

  // serialize filename
  int length = filename_.size();
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], filename_.c_str(), length);
  pos += length;

  // serialize load type
  memcpy(&buffer[pos], (char *) &load_type_, sizeof(LoadType));
  pos += sizeof(LoadType);

  // serialize array schema
  length = as_pair.second;
  memcpy(&buffer[pos], &length, sizeof(int));
  pos += sizeof(int);
  memcpy(&buffer[pos], as_pair.first, length);

  assert(pos + length == buffer_size);
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
  LoadType load_type = static_cast<LoadType>(buffer[pos]);
  pos += sizeof(LoadType);

  // array schema
  length = (int) buffer[pos];
  pos += sizeof(int);
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(&buffer[pos], length);

  assert(pos + length == buffer_size);
  return new ParallelLoadMsg(filename, load_type, *schema);
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

// HELPER METHODS
ArraySchema::CellType parse_attr_type(const char* buffer, int buf_length) {
  // type is the first thing in the serial string, see serialize method
  return static_cast<ArraySchema::CellType>(buffer[0]);
}

// template instantiations
template class FilterMsg<int>;
template class FilterMsg<float>;
template class FilterMsg<double>;

