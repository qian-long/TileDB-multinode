#ifndef MESSAGES_H
#define MESSAGES_H

#include <stdio.h>
#include "array_schema.h"
#include "loader.h"
#include "predicate.h"
#include "constants.h"

#define QUIT_TAG            0
#define DEF_TAG             1
#define GET_TAG             2 // msg == array_name to get,
#define DEFINE_ARRAY_TAG    3
#define LOAD_TAG            4
#define FILTER_TAG          5
#define SUBARRAY_TAG        6
#define AGGREGATE_TAG       7
#define ERROR_TAG           8
#define DONE_TAG            9
#define PARALLEL_LOAD_TAG   10
#define JOIN_TAG            11
#define KEEP_RECEIVING_TAG  12
#define SAMPLES_TAG         13

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
 ******************* SUBARRAY MESSAGE *****************
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
    enum LoadMethod {SORT, SAMPLE};

    // CONSTRUCTORS
    LoadMsg();
    LoadMsg(const std::string filename,
        ArraySchema& array_schema,
        PartitionType type,
        LoadMethod method);

    // DESTRUCTORS
    ~LoadMsg(){};

    // ACCESSORS
    std::string filename() { return filename_; }
    ArraySchema& array_schema() { return array_schema_; }
    PartitionType partition_type() { return type_; }
    LoadMethod load_method() { return method_; }


    // METHODS
    std::pair<char*, int> serialize();
    static LoadMsg* deserialize(char* buffer, int buffer_length);

  private:
    std::string filename_;
    ArraySchema array_schema_;
    PartitionType type_;
    LoadMethod method_;
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
 *************** DEFINE ARRAY MESSAGE *****************
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
    static DefineArrayMsg* deserialize(char* buffer, int buffer_length);

  private:
    ArraySchema array_schema_;
};

/*******************************************************
 ******************* FILTER MESSAGE ********************
 *******************************************************/
class FilterMsg : public Msg {

  public:
    // CONSTRUCTORS
    FilterMsg();
    FilterMsg(std::string& array_name, std::string& expression, std::string& result_array_name);

    // DESTRUCTOR
    ~FilterMsg(){};

    // ACCESSORS
    std::string array_name() { return array_name_; }
    std::string result_array_name() { return result_array_name_; }
    std::string expression() { return expr_; }

    // METHODS
    std::pair<char*, int> serialize();
    static FilterMsg* deserialize(char* buffer, int buf_length);

  private:
    // MEMBERS
    std::string array_name_;
    std::string result_array_name_;
    std::string expr_;
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

    // CONSTRUCTORS
    ParallelLoadMsg();
    ParallelLoadMsg(std::string filename, PartitionType type, ArraySchema& array_schema, int num_samples = 10);

    // DESTRUCTORS
    ~ParallelLoadMsg(){};

    // ACCESSORS
    std::string filename() { return filename_; }
    PartitionType partition_type() { return type_; }
    ArraySchema& array_schema() { return array_schema_; }
    int num_samples() { return num_samples_; }

    // METHODS
    std::pair<char*, int> serialize();
    static ParallelLoadMsg* deserialize(char* buffer, int buffer_size);

  private:
    std::string filename_;
    PartitionType type_;
    ArraySchema array_schema_;
    int num_samples_; // for ordered parallel load, number of samples to pick from each worker
};

/******************************************************
 ********************* JOIN MESSAGE *******************
 ******************************************************/
class JoinMsg : public Msg {

  public:
    // CONSTRUCTOR
    JoinMsg();
    JoinMsg(const std::string array_name_A, 
            const std::string array_name_B, 
            const std::string result_array_name);

    // DESTRUCTOR
    ~JoinMsg(){};

    // ACCESSORS
    std::string array_name_A() { return array_name_A_; }
    std::string array_name_B() { return array_name_B_; }
    std::string result_array_name() { return result_array_name_; }

    // METHODS
    std::pair<char*, int> serialize();
    static JoinMsg* deserialize(char* buffer, int buffer_length);

  private:
    std::string array_name_A_;
    std::string array_name_B_;
    std::string result_array_name_;

};

// CONTENT MESSAGES
/******************************************************
 ******************* Samples MESSAGE ******************
 ******************************************************/

class SamplesMsg : public Msg {
  public:
    // CONSTRUCTOR
    SamplesMsg();
    SamplesMsg(std::vector<int64_t> samples);

    // DESTRUCTOR
    ~SamplesMsg(){};

    // ACCESSORS
    std::vector<int64_t> samples() { return samples_; }

    // METHODS
    std::pair<char*, int> serialize();
    static SamplesMsg* deserialize(char* buffer, int buffer_length);

  private:
    std::vector<int64_t> samples_;
};

/******************************************************
 ****************** MESSAGE EXCEPTION *****************
 ******************************************************/

class MessageException {
  public:
    MessageException(const std::string& msg): msg_(msg) {}
    ~MessageException() {}

    const std::string& what() const { return msg_; }

  private:
    std::string msg_;
};
#endif
