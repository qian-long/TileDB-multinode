#include "messages.h"
#include <assert.h>
#include <cstring>
#include "debug.h"
#include <functional>

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
    case LOAD_TAG: // TODO
      return LoadMsg::deserialize(buf, length);
    case SUB_ARRAY_TAG: // TODO
      return SubArrayMsg::deserialize(buf, length);
  }
  throw std::invalid_argument("trying to deserailze msg of unknown type");
}

/******************************************************
 ******************* SubArray MESSAGE *****************
 ******************************************************/
SubArrayMsg::SubArrayMsg(std::string result_name, ArraySchema* schema, std::vector<double> ranges) : Msg(SUB_ARRAY_TAG) {
  this->result_array_name = result_name;
  this->ranges = ranges;
  this->array_schema = schema;
}

std::string SubArrayMsg::serialize() {
  std::stringstream ss;
  // serialize filename
  int filename_length = result_array_name.size();
  ss.write((char *) &filename_length, sizeof(int));
  ss.write((char *) result_array_name.c_str(), filename_length);

  //serialize array schema
  std::string schema_serial = array_schema->serialize();
  int schema_serial_length = schema_serial.size();
  ss.write((char *) &schema_serial_length, sizeof(int));
  ss.write((char *) schema_serial.c_str(), schema_serial_length);

  //serailze ranges

  int ranges_size = ranges.size();
  ss.write((char *) &ranges_size, sizeof(int));

  std::vector<double>::iterator it = this->ranges.begin();
  for (; it != this->ranges.end(); it++) {
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
  
  ArraySchema* schema = ArraySchema::deserialize(&buffer[counter], arrayschema_length); 

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
std::string LoadMsg::serialize() {
  std::stringstream ss;
  // serialize filename
  int filename_length = filename.size();
  ss.write((char *) &filename_length, sizeof(int));
  ss.write((char *) filename.c_str(), filename_length);

  // serialize order
  ss.write((char *) &order, sizeof(Loader::Order));

  // serialize array schema
  std::string schema_serial = array_schema->serialize();
  int schema_serial_length = schema_serial.size();
  ss.write((char *) &schema_serial_length, sizeof(int));
  ss.write((char *) schema_serial.c_str(), schema_serial_length);

  return ss.str();
}

LoadMsg* LoadMsg::deserialize(const char * buffer, int buffer_length) {
  //Load vars
  std::string filename;
  Loader::Order order;
  std::stringstream ss;
  int counter = 0;

  int filename_length = (int) buffer[counter];
  counter += sizeof(int);
  ss.write(&buffer[counter], filename_length);

  filename = ss.str(); // first arg
  counter += filename_length;

  memcpy(&order, &buffer[counter], sizeof(Loader::Order));
  counter += sizeof(Loader::Order);

  int arrayschema_length = (int) buffer[counter];
  counter += sizeof(int);
  
  // this is creating space for it on the heap. 
  ArraySchema* schema = ArraySchema::deserialize(&buffer[counter], arrayschema_length); // 3rd arg

  // finished parsing
  assert(counter + arrayschema_length == buffer_length);
  return new LoadMsg(filename, schema, order);
}

LoadMsg::LoadMsg() : Msg(LOAD_TAG) { }

LoadMsg::LoadMsg(const std::string filename, ArraySchema* array_schema, Loader::Order order) : 
  Msg(LOAD_TAG){
  this->filename = filename;
  this->order = order;
  this->array_schema = array_schema;
}

/******************************************************
 ********************* GET MESSAGE ********************
 ******************************************************/


GetMsg::GetMsg() : Msg(GET_TAG) {};

GetMsg::GetMsg(std::string arrayname) : Msg(GET_TAG)  {
  this->array_name = arrayname;
}

std::string GetMsg::serialize() {
  std::stringstream ss;
  int array_name_length = array_name.size();
  ss.write((char *) &array_name_length, sizeof(int));
  ss.write((char *) array_name.c_str(), array_name_length);

  return ss.str();
}

GetMsg* GetMsg::deserialize(const char* buffer, int buffer_length) {

  //getmsg args
  std::string array_name;
  std::stringstream ss;
  int counter = 0;

  int array_name_length = (int) buffer[counter];
  counter += sizeof(int);
  ss.write(&buffer[counter], array_name_length);

  array_name = ss.str(); // first arg
  return new GetMsg(array_name);
}

ArraySchemaMsg::ArraySchemaMsg() : Msg(ARRAY_SCHEMA_TAG) {};

ArraySchemaMsg::ArraySchemaMsg(ArraySchema* schema) : Msg(ARRAY_SCHEMA_TAG)  {
  this->array_schema = schema;
}

std::string ArraySchemaMsg::serialize() {
  return this->array_schema->serialize();
}

ArraySchemaMsg* ArraySchemaMsg::deserialize(const char* buffer, int buffer_length) {
  ArraySchema* schema = ArraySchema::deserialize(buffer, buffer_length);
  return new ArraySchemaMsg(schema);
}


/******************************************************
 ****************** FILTER MESSAGE ********************
 ******************************************************/

template<class T>
FilterMsg<T>::FilterMsg() : Msg(FILTER_TAG) {}

template<class T>
FilterMsg<T>::FilterMsg(
    const ArraySchema::DataType& attr_type, 
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
  ss.write((char *) &attr_type_, sizeof(ArraySchema::DataType));

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
  std::string schema_serial = array_schema_.serialize();
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
  ArraySchema::DataType datatype = static_cast<ArraySchema::DataType>(buffer[0]);
  pos += sizeof(ArraySchema::DataType);

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
  ArraySchema *schema = ArraySchema::deserialize(&buffer[pos], length);

  // finished parsing
  assert(length + pos == buf_length);

  return new FilterMsg(datatype, *schema, *pred, result_array_name);
}


ArraySchema::DataType parse_attr_type(const char* buffer, int buf_length) {
  // type is the first thing in the serial string, see serialize method
  return static_cast<ArraySchema::DataType>(buffer[0]);
}

// template instantiations
template class FilterMsg<int>;
template class FilterMsg<float>;
template class FilterMsg<double>;

