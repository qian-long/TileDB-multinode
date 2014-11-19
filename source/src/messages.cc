#include "messages.h"
#include <assert.h>

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

  LoadMsg* msg = new LoadMsg();
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

  msg->array_schema = ArraySchema::deserialize(&buffer[counter], arrayschema_length); // 3rd arg

  // finished parsing
  assert(counter + arrayschema_length == buffer_length);

  return msg;
}

LoadMsg::LoadMsg() : Msg(LOAD_TAG) { }

LoadMsg::LoadMsg(const std::string filename, ArraySchema* array_schema, Loader::Order order) : 
  Msg(LOAD_TAG){
  this->filename = filename;
  this->order = order;
  this->array_schema = array_schema;
}
