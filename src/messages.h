#ifndef MESSAGES_H
#define MESSAGES_H

#include <stdio.h>
#include "array_schema.h"
#include "loader.h"
#include "predicate.h"

#define QUIT_TAG          0
#define DEF_TAG           1
#define GET_TAG           2 // msg == array_name to get,
#define INIT_TAG          3 // partition data, send array schema
#define ARRAY_SCHEMA_TAG  4
#define LOAD_TAG          5
#define FILTER_TAG        6
#define SUBARRAY_TAG      7
#define AGGREGATE_TAG     8
#define ERROR_TAG         9
#define DONE_TAG          10

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


Msg* deserialize_msg(int MsgType, const char* buffer, int buffer_length);

class SubArrayMsg : public Msg {
  public:
    // Constructor
    SubArrayMsg(std::string result_name, ArraySchema* schema, std::vector<double> ranges);

    // Destructor
    ~SubArrayMsg(){};

    // Getters
    std::string result_arrayname() { return result_arrayname_; }
    std::vector<double> ranges() { return ranges_; }
    ArraySchema* array_schema() { return array_schema_; }

    // Methods
    std::string serialize();
    static SubArrayMsg* deserialize(const char* buffer, int buffer_length);

  private:
    std::string result_arrayname_;
    std::vector<double> ranges_;
    ArraySchema* array_schema_;

};

class LoadMsg : public Msg {

  public:
    // CONSTRUCTORS
    LoadMsg();
    LoadMsg(const std::string filename, ArraySchema* array_schema);

    // DESTRUCTORS
    ~LoadMsg(){};

    // ACCESSORS
    std::string filename() { return filename_; }
    ArraySchema* array_schema() { return array_schema_; }

    // METHODS
    std::string serialize();
    static LoadMsg* deserialize(const char* buffer, int buffer_length);

  private:
    std::string filename_;
    ArraySchema* array_schema_;
};

class GetMsg : public Msg {

  public:
    // CONSTRUCTOR
    GetMsg();
    GetMsg(const std::string array_name);

    // DESTRUCTOR
    ~GetMsg(){};

    // ACCESSORS
    std::string array_name() { return array_name_; }

    // METHODS
    std::string serialize();
    static GetMsg* deserialize(const char* buffer, int buffer_length);

  private:
    std::string array_name_;

};

class ArraySchemaMsg : public Msg {

  public:
    // CONSTRUCTOR
    ArraySchemaMsg();
    ArraySchemaMsg(ArraySchema* array_schema);

    // DESTRUCTOR
    ~ArraySchemaMsg(){};

    // ACCESSORS
    ArraySchema* array_schema() { return array_schema_; }

    // METHODS
    std::string serialize();
    static ArraySchemaMsg* deserialize(const char* buffer, int buffer_length);

  private:
    ArraySchema* array_schema_;
};

template<class T>
class FilterMsg : public Msg {

  public:
    // CONSTRUCTORS
    FilterMsg();
    FilterMsg(const ArraySchema::CellType& attr_type, ArraySchema& schema, Predicate<T>& predicate, const std::string& result_array_name);

    // DESTRUCTOR
    ~FilterMsg();

    // ACCESSORS
    ArraySchema array_schema() { return array_schema_; }
    std::string result_array_name() { return result_array_name_; }
    Predicate<T> predicate() { return predicate_; }
    ArraySchema::CellType attr_type() { return attr_type_; }

    // METHODS
    std::string serialize();
    static FilterMsg<T>* deserialize(const char* buffer, int buf_length);

    // HELPER METHODS
    static ArraySchema::CellType parse_attr_type(const char* buffer, int buf_length);

  private:
    // MEMBERS
    ArraySchema array_schema_;
    std::string result_array_name_;
    Predicate<T> predicate_;
    ArraySchema::CellType attr_type_;


};


// TODO finish?
class AggregateMsg : public Msg {
  public:
    // CONSTRUCTORS
    AggregateMsg();
    AggregateMsg(std::string array_name, int attr_index);

    // DESTRUCTORS
    ~AggregateMsg(){};

    // ACCESSORS
    std::string array_name() { return array_name_; }
    int attr_index() { return attr_index_; }

    // METHODS
    std::string serialize();
    static AggregateMsg* deserialize(const char* buf, int len);

  private:
    std::string array_name_;
    int attr_index_;


};


#endif 

