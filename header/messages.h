#ifndef MESSAGES_H
#define MESSAGES_H

#include <stdio.h>
#include "array_schema.h"
#include "loader.h"
#include "predicate.h"

#define QUIT_TAG 0
#define DEF_TAG 1
#define GET_TAG 2 // msg == array_name to get,
#define INIT_TAG 3 // partition data, send array schema
#define ARRAY_SCHEMA_TAG 4
#define LOAD_TAG 5
#define FILTER_TAG 6
#define SUBARRAY_TAG 7
#define AGGREGATE_TAG 8
#define ERROR_TAG 9
#define DONE_TAG 10

class Msg {

  public:
    int msg_tag;
    Msg(int type) {
      this->msg_tag= type;
    };

    ~Msg(){};

    virtual std::string serialize();
    //static void deserialize(Msg* msg, const char* buffer, int buffer_length);

};

class SubArrayMsg : public Msg {
  public: 
    std::string result_array_name;
    std::vector<double> ranges;
    ArraySchema* array_schema;

    ~SubArrayMsg(){};
    SubArrayMsg(std::string result_name, ArraySchema* schema, std::vector<double> ranges);

    std::string serialize();
    static SubArrayMsg* deserialize(const char* buffer, int buffer_length);

};

class LoadMsg : public Msg {
  
  public:   
    std::string filename;
    Loader::Order order;
    ArraySchema* array_schema;

    LoadMsg();
    LoadMsg(const std::string filename, ArraySchema* array_schema, Loader::Order order);

    ~LoadMsg(){};

    std::string serialize();
    static LoadMsg* deserialize(const char* buffer, int buffer_length);

};

class GetMsg : public Msg {
  
  public:   
    std::string array_name;

    GetMsg();
    GetMsg(const std::string arrayname);

    ~GetMsg(){};

    std::string serialize();
    static GetMsg* deserialize(const char* buffer, int buffer_length);
};

class ArraySchemaMsg : public Msg {
  
  public:   
    ArraySchema* array_schema;

    ArraySchemaMsg();
    ArraySchemaMsg(ArraySchema* array_schema);

    ~ArraySchemaMsg(){};

    std::string serialize();
    static ArraySchemaMsg* deserialize(const char* buffer, int buffer_length);
};

Msg* deserialize_msg(int MsgType, const char* buffer, int buffer_length);


template<class T>
class FilterMsg : public Msg {

  public:
    // MEMBERS
    ArraySchema array_schema_;
    std::string result_array_name_;
    Predicate<T> predicate_; 
    ArraySchema::DataType attr_type_;

    // CONSTRUCTORS
    FilterMsg();

    FilterMsg(const ArraySchema::DataType& attr_type, ArraySchema& schema, Predicate<T>& predicate, const std::string& result_array_name);

    // DESTRUCTOR
    ~FilterMsg();

    // SERIALIZE
    std::string serialize();

    // DESERIALIZE
    static FilterMsg<T>* deserialize(const char* buffer, int buf_length);

    // HELPER METHODS 
    static ArraySchema::DataType parse_attr_type(const char* buffer, int buf_length);

  private:

};


class AggregateMsg : public Msg {
  
  public:   
    ArraySchema array_schema_;
    int attr_index_;

    AggregateMsg();
    AggregateMsg(const ArraySchema& array_schema, int attr_index);

    ~AggregateMsg(){};

    std::string serialize();
    static AggregateMsg* deserialize(const char* buf, int len);
};


#endif 

