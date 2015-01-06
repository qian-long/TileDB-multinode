#include "messages.h"
#include <assert.h>
#include <cstring>
#include "debug.h"
#include <functional>
#include <stdexcept>      // std::invalid_argument

/******************************************************
 *********************** MESSAGE **********************
 ******************************************************/
std::string Msg::serialize() {
  DEBUG_MSG("you are not using the right serialze");
  throw std::bad_function_call();
}

Msg* deserialize_msg(int type, const char* buf, int length){
  switch(type){
    case GET_TAG:
      return GetMsg::deserialize(buf, length);
    case ARRAY_SCHEMA_TAG:
      return ArraySchemaMsg::deserialize(buf, length);
    case LOAD_TAG:
      return LoadMsg::deserialize(buf, length);
    case SUBARRAY_TAG:
      return SubArrayMsg::deserialize(buf, length);
    case AGGREGATE_TAG:
      return AggregateMsg::deserialize(buf, length);
  }
  throw std::invalid_argument("trying to deserailze msg of unknown type");
}

/******************************************************
 ******************* SubArray MESSAGE *****************
 ******************************************************/

SubArrayMsg::SubArrayMsg(std::string result_name, ArraySchema* schema, std::vector<double> ranges) : Msg(SUBARRAY_TAG) {
  result_arrayname_ = result_name;
  ranges_ = ranges;
  array_schema_ = schema;
}

std::string SubArrayMsg::serialize() {
  std::stringstream ss;
  // serialize filename
  int filename_length = result_arrayname_.size();
  ss.write((char *) &filename_length, sizeof(int));
  ss.write((char *) result_arrayname_.c_str(), filename_length);

  //serialize array schema
  std::pair<char*, int> pair = array_schema_->serialize();
  std::string schema_serial = std::string(pair.first, pair.second);
  int schema_serial_length = schema_serial.size();
  ss.write((char *) &schema_serial_length, sizeof(int));
  ss.write((char *) schema_serial.c_str(), schema_serial_length);

  //serailze ranges
  int ranges_size = ranges_.size();
  ss.write((char *) &ranges_size, sizeof(int));

  std::vector<double>::iterator it = ranges_.begin();
  for (; it != ranges_.end(); it++) {
    double extent = *it;

    ss.write((char *) &extent, sizeof(double));
  }

  return ss.str();
}
SubArrayMsg* SubArrayMsg::deserialize(const char* buffer, int buffer_length){

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

  return new SubArrayMsg(array_name, schema, ranges);

}

/******************************************************
 ********************* LOAD MESSAGE *******************
 ******************************************************/
LoadMsg::LoadMsg() : Msg(LOAD_TAG) { }

LoadMsg::LoadMsg(const std::string filename, ArraySchema* array_schema) : 
  Msg(LOAD_TAG){
  filename_ = filename;
  array_schema_ = array_schema;
}

std::string LoadMsg::serialize() {
  std::stringstream ss;

  // serialize filename
  int filename_length = filename_.size();
  ss.write((char *) &filename_length, sizeof(int));
  ss.write((char *) filename_.c_str(), filename_length);

  // serialize array schema
  std::pair<char*, int> pair = array_schema_->serialize();
  ss.write((char *) &pair.second, sizeof(int));
  ss.write((char *) pair.first, pair.second);

  return ss.str();
}

LoadMsg* LoadMsg::deserialize(const char * buffer, int buffer_length) {
  //Load vars
  std::string filename;
  ArraySchema::Order order;
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
  return new LoadMsg(filename, schema);
}


/******************************************************
 ********************* GET MESSAGE ********************
 ******************************************************/
GetMsg::GetMsg() : Msg(GET_TAG) {};

GetMsg::GetMsg(std::string array_name) : Msg(GET_TAG)  {
  array_name_ = array_name;
}

std::string GetMsg::serialize() {
  std::stringstream ss;
  int array_name_length = array_name_.size();
  ss.write((char *) &array_name_length, sizeof(int));
  ss.write((char *) array_name_.c_str(), array_name_length);

  return ss.str();
}

GetMsg* GetMsg::deserialize(const char* buffer, int buffer_length) {

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
ArraySchemaMsg::ArraySchemaMsg() : Msg(ARRAY_SCHEMA_TAG) {};

ArraySchemaMsg::ArraySchemaMsg(ArraySchema* schema) : Msg(ARRAY_SCHEMA_TAG)  {
  array_schema_ = schema;
}

std::string ArraySchemaMsg::serialize() {
  std::pair<char*, int> pair = array_schema_->serialize();
  return std::string(pair.first, pair.second);
}

ArraySchemaMsg* ArraySchemaMsg::deserialize(const char* buffer, int buffer_length) {
  ArraySchema* schema = new ArraySchema();
  schema->deserialize(buffer, buffer_length);
  return new ArraySchemaMsg(schema);
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
std::string FilterMsg<T>::serialize() {
  std::stringstream ss;

  // serialize attr_type_
  ss.write((char *) &attr_type_, sizeof(ArraySchema::CellType));

  // serialize resulting array name
  int length = result_array_name_.size();
  ss.write((char *) &length, sizeof(int));
  ss.write((char *) result_array_name_.c_str(), length);

  // serialize predicate
  std::string pred_serial = predicate_.serialize();
  int pred_serial_length = pred_serial.size();

  ss.write((char *) &pred_serial_length, sizeof(int));
  ss.write((char *) pred_serial.c_str(), pred_serial_length);

  // serialize array schema
  std::pair<char*, int> pair = array_schema_.serialize();
  std::string schema_serial = std::string(pair.first, pair.second);

  int schema_serial_length = schema_serial.size();
  ss.write((char *) &schema_serial_length, sizeof(int));
  ss.write((char *) schema_serial.c_str(), schema_serial_length);

  return ss.str();
}

template<class T>
FilterMsg<T>* FilterMsg<T>::deserialize(const char* buffer, int buf_length) {
  std::stringstream ss;
  int pos = 0;

  // parse attribute type
  ArraySchema::CellType datatype = static_cast<ArraySchema::CellType>(buffer[0]);
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


/******************************************************
 ******************* AGGREGATE MESSAGE ****************
 ******************************************************/

AggregateMsg::AggregateMsg() : Msg(AGGREGATE_TAG) {};

AggregateMsg::AggregateMsg(std::string array_name, int attr_index): Msg(AGGREGATE_TAG) {
  attr_index_ = attr_index;
  array_name_ = array_name;
}

std::string AggregateMsg::serialize() {
  std::stringstream ss;

  // serialize array name
  int length = array_name_.size();
  ss.write((char *) &length, sizeof(int));
  ss.write((char *) array_name_.c_str(), length);

  // serialize attr int
  ss.write((char *) &attr_index_, sizeof(int));

  return ss.str();
}

AggregateMsg* AggregateMsg::deserialize(const char* buf, int len) {
  int pos = 0;

  // deserialize array name
  int length = (int) buf[pos];
  pos += sizeof(int);

  std::string array_name = std::string(&buf[pos], length);
  std::cout << array_name << "\n";
  pos += length;

  // deserialize attribute index
  int attr_index = (int) buf[pos];

  assert(pos + sizeof(int) == len);
  return new AggregateMsg(array_name, attr_index);
}



ArraySchema::CellType parse_attr_type(const char* buffer, int buf_length) {
  // type is the first thing in the serial string, see serialize method
  return static_cast<ArraySchema::CellType>(buffer[0]);
}

// template instantiations
template class FilterMsg<int>;
template class FilterMsg<float>;
template class FilterMsg<double>;

