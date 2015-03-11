#ifndef MESSAGES_H
#define MESSAGES_H

#include <stdio.h>
#include "array_schema.h"
#include "loader.h"
#include "predicate.h"

#define QUIT_TAG            0
#define DEF_TAG             1
#define GET_TAG             2 // msg == array_name to get,
#define INIT_TAG            3 // partition data, send array schema
#define DEFINE_ARRAY_TAG    4
#define LOAD_TAG            5
#define FILTER_TAG          6
#define SUBARRAY_TAG        7
#define AGGREGATE_TAG       8
#define ERROR_TAG           9
#define DONE_TAG            10
#define PARALLEL_LOAD_TAG   11
#define KEEP_RECEIVING_TAG  12

class Msg {

  public:
    int msg_tag;
    Msg(int type) {
      this->msg_tag= type;
    };

    ~Msg(){};

    virtual std::pair<char*, int> serialize();
    //static void deserialize(Msg* msg, const char* buffer, int buffer_length);

};

Msg* deserialize_msg(int MsgType, char* buffer, int buffer_length);

/******************************************************
 ******************* SubArray MESSAGE *****************
 ******************************************************/
class SubarrayMsg : public Msg {
  public:
    // Constructor
    SubarrayMsg(std::string result_name, ArraySchema schema, std::vector<double> ranges);

    // Destructor
    ~SubarrayMsg(){};

    // Getters
    std::string result_array_name() { return result_array_name_; }
    std::vector<double> ranges() { return ranges_; }
    ArraySchema array_schema() { return array_schema_; }

    // Methods
    std::pair<char*, int> serialize();
    static SubarrayMsg* deserialize(char* buffer, int buffer_length);

  private:
    std::string result_array_name_;
    std::vector<double> ranges_;
    ArraySchema array_schema_;

};

/******************************************************
 ******************** LOAD MESSAGE ********************
 ******************************************************/
class LoadMsg : public Msg {

  public:
    // CONSTRUCTORS
    LoadMsg();
    LoadMsg(const std::string filename, ArraySchema& array_schema);

    // DESTRUCTORS
    ~LoadMsg(){};

    // ACCESSORS
    std::string filename() { return filename_; }
    ArraySchema& array_schema() { return array_schema_; }

    // METHODS
    std::pair<char*, int> serialize();
    static LoadMsg* deserialize(char* buffer, int buffer_length);

  private:
    std::string filename_;
    ArraySchema array_schema_;
};


/******************************************************
 ********************* GET MESSAGE ********************
 ******************************************************/
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
    std::pair<char*, int> serialize();
    static GetMsg* deserialize(char* buffer, int buffer_length);

  private:
    std::string array_name_;

};

/******************************************************
 *************** ARRAYSCHEMA MESSAGE ******************
 ******************************************************/
class DefineArrayMsg : public Msg {

  public:
    // CONSTRUCTOR
    DefineArrayMsg();
    DefineArrayMsg(ArraySchema& array_schema);

    // DESTRUCTOR
    ~DefineArrayMsg(){};

    // ACCESSORS
    ArraySchema& array_schema() { return array_schema_; }

    // METHODS
    std::pair<char*, int> serialize();

    // TODO Caller should delete internal array_schema?
    static DefineArrayMsg* deserialize(char* buffer, int buffer_length);

  private:
    ArraySchema array_schema_;
};

/*******************************************************
 ******************* FILTER MESSAGE ********************
 *******************************************************/
template<class T>
class FilterMsg : public Msg {

  public:
    // CONSTRUCTORS
    FilterMsg();
    FilterMsg(const ArraySchema::CellType& attr_type, ArraySchema& schema, Predicate<T>& predicate, const std::string& result_array_name);

    // DESTRUCTOR
    ~FilterMsg();

    // ACCESSORS
    ArraySchema& array_schema() { return array_schema_; }
    std::string result_array_name() { return result_array_name_; }
    Predicate<T>& predicate() { return predicate_; }
    ArraySchema::CellType attr_type() { return attr_type_; }

    // METHODS
    std::pair<char*, int> serialize();
    static FilterMsg<T>* deserialize(char* buffer, int buf_length);

    // HELPER METHODS
    static ArraySchema::CellType parse_attr_type(char* buffer, int buf_length);

  private:
    // MEMBERS
    ArraySchema array_schema_;
    std::string result_array_name_;
    Predicate<T> predicate_;
    ArraySchema::CellType attr_type_;
};

/*******************************************************
 ***************** AGGREGATE MESSAGE *******************
 *******************************************************/
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
    std::pair<char*, int> serialize();
    static AggregateMsg* deserialize(char* buf, int len);

  private:
    std::string array_name_;
    int attr_index_;
};

/********************************************************
 **************** PARALLEL LOAD MESSAGE *****************
 ********************************************************/

class ParallelLoadMsg : public Msg {
  public:
    // which load algo to use
    enum LoadType {NAIVE, HASH_PARTITION, MERGE_SORT, SAMPLING};

    // CONSTRUCTORS
    ParallelLoadMsg();
    ParallelLoadMsg(std::string filename, LoadType load_type, ArraySchema& array_schema);

    // DESTRUCTORS
    ~ParallelLoadMsg(){};

    // ACCESSORS
    std::string filename() { return filename_; }
    LoadType load_type() { return load_type_; }
    ArraySchema& array_schema() { return array_schema_;}

    // METHODS
    std::pair<char*, int> serialize();
    static ParallelLoadMsg* deserialize(char* buffer, int buffer_size);

  private:
    std::string filename_;
    LoadType load_type_;
    ArraySchema array_schema_;
};
#endif
