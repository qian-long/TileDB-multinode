#include "messages.h"
#include <assert.h>
#include <cstring>
#include "debug.h"
#include <functional>

std::string Msg::serialize() {
  DEBUG_MSG("you are not using the right serialze");
  throw std::bad_function_call();
}

std::string LoadMsg::serialize() {
  
  std::stringstream ss;
  // serialize filename
  int filename_length = filename.size();
  ss.write((char *) &filename_length, sizeof(int));
  ss.write((char *) filename.c_str(), filename_length);

  // serialize order
  ss.write((char *) &order, sizeof(Loader::Order));

  // serialize array schema
  std::string schema_serial = array_schema.serialize();
  int schema_serial_length = schema_serial.size();
  ss.write((char *) &schema_serial_length, sizeof(int));
  ss.write((char *) schema_serial.c_str(), schema_serial_length);

  return ss.str();
}

void LoadMsg::deserialize(LoadMsg* msg, const char * buffer, int buffer_length) {

  std::stringstream ss;
  int counter = 0;

  int filename_length = (int) buffer[counter];
  counter += sizeof(int);
  ss.write(&buffer[counter], filename_length);

  msg->filename = ss.str(); // first arg
  counter += filename_length;

  memcpy(&msg->order, &buffer[counter], sizeof(Loader::Order));
  counter += sizeof(Loader::Order);

  int arrayschema_length = (int) buffer[counter];
  counter += sizeof(int);

  ArraySchema::deserialize(&msg->array_schema, &buffer[counter], arrayschema_length); // 3rd arg

  // finished parsing
  assert(counter + arrayschema_length == buffer_length);

  return;
}

LoadMsg::LoadMsg() : Msg(LOAD_TAG) { }

LoadMsg::LoadMsg(const std::string filename, ArraySchema array_schema, Loader::Order order) : 
  Msg(LOAD_TAG){
  this->filename = filename;
  this->order = order;
  this->array_schema = array_schema;
}

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

void GetMsg::deserialize(GetMsg* msg, const char* buffer, int buffer_length) {
  std::stringstream ss;
  int counter = 0;

  int array_name_length = (int) buffer[counter];
  counter += sizeof(int);
  ss.write(&buffer[counter], array_name_length);

  msg->array_name = ss.str(); // first arg
}
