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
#define PARALLEL_LOAD_TAG   8
#define JOIN_TAG            9
#define KEEP_RECEIVING_TAG  10
#define SAMPLES_TAG         11
#define ACK_TAG             12
#define BOUNDING_COORDS_TAG 13 // bounding coordinates
#define TILE_TAG            14 // one physical tile

class Msg {

  public:
    int msg_tag;
    Msg(int type) {
      this->msg_tag= type;
    };

    ~Msg(){};

    virtual std::pair<char*, uint64_t> serialize();
    //static void deserialize(Msg* msg, const char* buffer, int buffer_length);

};

Msg* deserialize_msg(int MsgType, char* buffer, uint64_t buffer_length);

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
    std::pair<char*, uint64_t> serialize();
    static SubarrayMsg* deserialize(char* buffer, uint64_t buffer_length);

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
        LoadMethod method = LoadMsg::SAMPLE,
        uint64_t num_samples = 10);

    // DESTRUCTORS
    ~LoadMsg(){};

    // ACCESSORS
    std::string filename() { return filename_; }
    ArraySchema& array_schema() { return array_schema_; }
    PartitionType partition_type() { return type_; }
    LoadMethod load_method() { return method_; }
    uint64_t num_samples() { return num_samples_; }


    // METHODS
    std::pair<char*, uint64_t> serialize();
    static LoadMsg* deserialize(char* buffer, uint64_t buffer_length);

  private:
    std::string filename_;
    ArraySchema array_schema_;
    PartitionType type_;
    LoadMethod method_;
    uint64_t num_samples_;
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
    std::pair<char*, uint64_t> serialize();
    static GetMsg* deserialize(char* buffer, uint64_t buffer_length);

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
    std::pair<char*, uint64_t> serialize();
    static DefineArrayMsg* deserialize(char* buffer, uint64_t buffer_length);

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
    std::pair<char*, uint64_t> serialize();
    static FilterMsg* deserialize(char* buffer, uint64_t buf_length);

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
    std::pair<char*, uint64_t> serialize();
    static AggregateMsg* deserialize(char* buf, uint64_t len);

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
    ParallelLoadMsg(std::string filename, PartitionType type, ArraySchema& array_schema, uint64_t num_samples = 10);

    // DESTRUCTORS
    ~ParallelLoadMsg(){};

    // ACCESSORS
    std::string filename() { return filename_; }
    PartitionType partition_type() { return type_; }
    ArraySchema& array_schema() { return array_schema_; }
    uint64_t num_samples() { return num_samples_; }

    // METHODS
    std::pair<char*, uint64_t> serialize();
    static ParallelLoadMsg* deserialize(char* buffer, uint64_t buffer_size);

  private:
    std::string filename_;
    PartitionType type_;
    ArraySchema array_schema_;
    uint64_t num_samples_; // for ordered parallel load, number of samples to pick from each worker
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
    std::pair<char*, uint64_t> serialize();
    static JoinMsg* deserialize(char* buffer, uint64_t buffer_length);

  private:
    std::string array_name_A_;
    std::string array_name_B_;
    std::string result_array_name_;

};

/******************************************************
 ******************** ACK MESSAGE *********************
 ******************************************************/
class AckMsg: public Msg {

  public:

    enum Result {DONE, ERROR};

    // CONSTRUCTOR
    AckMsg();
    AckMsg(Result r, int tag, double time = -1);

    // DESTRUCTOR
    ~AckMsg(){};

    // ACCESSORS
    Result result() { return result_; }
    int tag() { return tag_; }
    double time() { return time_; }
    
    // METHODS
    std::pair<char*, uint64_t> serialize();
    static AckMsg* deserialize(char* buffer, uint64_t buffer_length);
    std::string to_string();

  private:
    Result result_;
    int tag_;
    double time_;

};

// CONTENT MESSAGES
/******************************************************
 ******************* Samples MESSAGE ******************
 ******************************************************/
class SamplesMsg : public Msg {
  public:
    // CONSTRUCTOR
    SamplesMsg();
    SamplesMsg(std::vector<uint64_t> samples);

    // DESTRUCTOR
    ~SamplesMsg(){};

    // ACCESSORS
    std::vector<uint64_t> samples() { return samples_; }

    // METHODS
    std::pair<char*, uint64_t> serialize();
    static SamplesMsg* deserialize(char* buffer, uint64_t buffer_length);

  private:
    std::vector<uint64_t> samples_;
};

/******************************************************
 *************** Bounding Coords MESSAGE **************
 ******************************************************/
class BoundingCoordsMsg : public Msg {
  public:
    // CONSTRUCTOR
    BoundingCoordsMsg();
    BoundingCoordsMsg(StorageManager::BoundingCoordinates bounding_coords);

    // DESTRUCTOR
    ~BoundingCoordsMsg(){};

    // ACCESSORS
    StorageManager::BoundingCoordinates bounding_coordinates() { return bounding_coords_; }

    // METHODS
    std::pair<char*, uint64_t> serialize();
    static BoundingCoordsMsg* deserialize(char* buffer, uint64_t buffer_length);

  private:
    StorageManager::BoundingCoordinates bounding_coords_;
};

/******************************************************
 ******************** Tile MESSAGE ********************
 ******************************************************/
// physical tile
class TileMsg : public Msg {

  public:
    // CONSTRUCTOR
    TileMsg();
    TileMsg(std::string array_name,
        int attr_id,
        const char* payload, 
        uint64_t num_cells,
        uint64_t cell_size);

    // DESTRUCTOR
    ~TileMsg(){
      delete payload_;
    };


    // ACCESSORS
    std::string array_name() { return array_name_; }
    int attr_id() { return attr_id_; }
    uint64_t num_cells() { return num_cells_; }
    // in bytes
    uint64_t cell_size() { return cell_size_; }
    //StorageManager::BoundingCoordinatesPair bounding_coords_pair() { return pair_; }
    // in bytes
    uint64_t payload_size() { return num_cells_ * cell_size_; }
    const char* payload() { return payload_; }
    
    // METHODS
    std::pair<char*, uint64_t> serialize();
    static TileMsg* deserialize(char* buffer, uint64_t buffer_length);

  private:
    std::string array_name_;
    int attr_id_;
    const char* payload_;
    uint64_t num_cells_;
    uint64_t cell_size_;

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
