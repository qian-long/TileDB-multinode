#ifndef MESSAGES_H
#define MESSAGES_H

#include "array_schema.h"
#include <stdio.h>

#define QUIT_TAG 0
#define DEF_TAG 1 // default
#define GET_TAG 2 // msg == array_name to get,
#define INIT_TAG 3 // partition data, send array schema
#define ARRAY_SCHEMA_TAG 4
#define LOAD_TAG 5

class Msg {

  public:
    Msg(int type) {
      msg_tag= type;
    }
    ~Msg(){};

    vritual std::string serialize() = 0;
    virtual static Msg deserialize(const char* buffer, int buffer_length) = 0;

  private:
    int msg_type;
}

class LoadMsg : public Msg {
  
  public:   
    LoadMsg(const std::string filename, ArraySchema array_schema, Loader::Order order);

    //have to actually delete things
    ~LoadMsg();

    std::string serialize();
    static Msg deserialize(const char* buffer, int buffer_length);

  private: 
    
    LoadMsg();

    std::string filename;
    Order order;
    ArraySchema array_schema;

}

#endif 

