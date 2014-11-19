#ifndef MESSAGES_H
#define MESSAGES_H

#include <stdio.h>
#include "array_schema.h"
#include "loader.h"

#define QUIT_TAG 0
#define DEF_TAG 1 // default
#define GET_TAG 2 // msg == array_name to get,
#define INIT_TAG 3 // partition data, send array schema
#define ARRAY_SCHEMA_TAG 4
#define LOAD_TAG 5

class Msg {

  public:
    int msg_tag;
    Msg(int type) {
      this->msg_tag= type;
    }
    ~Msg(){};

    virtual std::string serialize();
    static void deserialize(Msg* msg, const char* buffer, int buffer_length);

};

class LoadMsg : public Msg {
  
  public:   
    std::string filename;
    Loader::Order order;
    ArraySchema array_schema;

    LoadMsg();
    LoadMsg(const std::string filename, ArraySchema array_schema, Loader::Order order);

    ~LoadMsg(){};

    std::string serialize();
    static void deserialize(LoadMsg* msg, const char* buffer, int buffer_length);

  private: 
    


};

class GetMsg : public Msg {
  
  public:   
    std::string array_name;

    GetMsg();
    GetMsg(const std::string arrayname);

    ~GetMsg(){};

    std::string serialize();
    static void deserialize(GetMsg* msg, const char* buffer, int buffer_length);

  private: 
    
};

#endif 

