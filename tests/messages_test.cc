#include "gtest/gtest.h"
#include "messages.h"
#include <cstdlib>


namespace {

  class MessagesTest: public ::testing::Test {
    protected:
      // ran before each test
      virtual void SetUp() {
        // Set array name
        std::string array_name = "test";

        // Set attribute names
        std::vector<std::string> attribute_names;
        attribute_names.push_back("attr1");
        attribute_names.push_back("attr2");

        // Set attribute types
        std::vector<const std::type_info*> types;
        types.push_back(&typeid(int));
        types.push_back(&typeid(int));

        // Set dimension type
        types.push_back(&typeid(int));

        // Set dimension names
        std::vector<std::string> dim_names;
        dim_names.push_back("i");
        dim_names.push_back("j");

        // Set dimension domains
        std::vector<std::pair<double,double> > dim_domains;
        dim_domains.push_back(std::pair<double,double>(0, 999));
        dim_domains.push_back(std::pair<double,double>(0, 999));

        // Create an array with irregular tiles
        array_schema_ = ArraySchema(array_name,
            attribute_names,
            dim_names,
            dim_domains,
            types,
            ArraySchema::ROW_MAJOR);

      }

      virtual void TearDown() {}

      // Test variables
      ArraySchema array_schema_;
  };

  // LOAD MSG TEST
  TEST_F(MessagesTest, LoadMsgTest) {

    std::string filename = "foo.csv";
    PartitionType part_type = ORDERED_PARTITION;
    LoadMsg::LoadMethod method = LoadMsg::SORT;
    int num_samples = 933;
    LoadMsg lmsg = LoadMsg(filename, array_schema_, part_type, method, num_samples);

    std::pair<char*, int> lserial = lmsg.serialize();

    LoadMsg* new_lmsg = LoadMsg::deserialize(lserial.first, lserial.second);

    // comparing message contents
    EXPECT_STREQ(filename.c_str(), new_lmsg->filename().c_str());
    EXPECT_STREQ(array_schema_.to_string().c_str(),
      new_lmsg->array_schema().to_string().c_str());
    EXPECT_EQ(part_type, new_lmsg->partition_type());
    EXPECT_EQ(method, new_lmsg->load_method());
    EXPECT_EQ(933, new_lmsg->num_samples());
  }


  // SUBARRAY MSG TEST
  TEST_F(MessagesTest, SubarrayMsgTest) {
    std::string result_name = "subarray_test";
    std::vector<double> ranges;

    ranges.push_back(0); ranges.push_back(5);
    ranges.push_back(0); ranges.push_back(5);

    SubarrayMsg smsg = SubarrayMsg(result_name, array_schema_, ranges);

    std::pair<char*, int> sserial = smsg.serialize();

    SubarrayMsg* new_smsg = SubarrayMsg::deserialize(sserial.first, sserial.second);

    // comparing message contents
    EXPECT_STREQ(result_name.c_str(), new_smsg->result_array_name().c_str());

    EXPECT_STREQ(array_schema_.to_string().c_str(),
        new_smsg->array_schema().to_string().c_str());

    std::vector<double>::iterator it1 = ranges.begin();
    std::vector<double>::iterator it2 = new_smsg->ranges().begin();
    for(; it1 != ranges.end(); it1++, it2++) {
      EXPECT_EQ(*it1, *it2);
    }
  }

  // GET MSG TEST
  TEST_F(MessagesTest, GetMsgTest) {
    std::string array_name = "get_test";
    GetMsg gmsg = GetMsg(array_name);

    std::pair<char*, int> gserial = gmsg.serialize();

    GetMsg* new_gmsg = GetMsg::deserialize(gserial.first, gserial.second);

    // comparing message contents
    EXPECT_STREQ(array_name.c_str(), new_gmsg->array_name().c_str());
  }


  // DEFINTE ARRAY MSG TEST
  TEST_F(MessagesTest, DefineArrayMsgTest) {
    DefineArrayMsg amsg = DefineArrayMsg(array_schema_);

    std::pair<char*, int> aserial = amsg.serialize();

    DefineArrayMsg* new_amsg = DefineArrayMsg::deserialize(aserial.first, aserial.second);

    // comparing message contents
    EXPECT_STREQ(array_schema_.to_string().c_str(),
        new_amsg->array_schema().to_string().c_str());
  }

  // AGGREGATE MSG TEST
  TEST_F(MessagesTest, AggregateMsgTest) {
    int attr_index = 3;
    std::string array_name = "aggregate_test";

    AggregateMsg amsg = AggregateMsg(array_name, attr_index);

    std::pair<char*, int> aserial = amsg.serialize();

    AggregateMsg* new_amsg = AggregateMsg::deserialize(aserial.first, aserial.second);

    // comparing message contents
    EXPECT_STREQ(array_name.c_str(), new_amsg->array_name().c_str());
    EXPECT_EQ(attr_index, new_amsg->attr_index());
  }

  // PARALLEL LOAD MSG TEST
  TEST_F(MessagesTest, ParallelLoadMsgTest) {
    std::string filename = "test";
    PartitionType part_type = ORDERED_PARTITION;
    int num_samples = 3;

    ParallelLoadMsg pmsg = ParallelLoadMsg(filename, part_type, array_schema_, num_samples);

    std::pair<char*, int> pserial = pmsg.serialize();

    ParallelLoadMsg* new_pmsg = ParallelLoadMsg::deserialize(pserial.first, pserial.second);

    // comparing message contents
    EXPECT_STREQ(filename.c_str(), new_pmsg->filename().c_str());
    EXPECT_EQ(part_type, new_pmsg->partition_type());
    EXPECT_STREQ(array_schema_.to_string().c_str(),
      new_pmsg->array_schema().to_string().c_str());
    EXPECT_EQ(num_samples, new_pmsg->num_samples());

  }

  // JOIN MSG TEST
  TEST_F(MessagesTest, JoinMsgTest) {
    std::string array_name_A = "blob";
    std::string array_name_B = "foo";
    std::string result_array_name = "foobar";

    JoinMsg jmsg = JoinMsg(array_name_A, array_name_B, result_array_name);

    std::pair<char*, int> jserial = jmsg.serialize();

    JoinMsg* new_jmsg = JoinMsg::deserialize(jserial.first, jserial.second);

    // comparing message contents
    EXPECT_STREQ(array_name_A.c_str(), new_jmsg->array_name_A().c_str());
    EXPECT_STREQ(array_name_B.c_str(), new_jmsg->array_name_B().c_str());
    EXPECT_STREQ(result_array_name.c_str(), new_jmsg->result_array_name().c_str());

  }

  // SAMPLES MSG TEST
  TEST_F(MessagesTest, SamplesMsgTest) {

    std::vector<uint64_t> samples;
    for (uint64_t i = 0; i < 10; ++i) {
      samples.push_back(i * 1232);
    }
    SamplesMsg smsg = SamplesMsg(samples);

    std::pair<char*, int> serial = smsg.serialize();

    SamplesMsg* new_smsg = SamplesMsg::deserialize(serial.first, serial.second);

    // comparing message contents
    EXPECT_EQ(samples.size(), new_smsg->samples().size());
    for (int i = 0; i < new_smsg->samples().size(); ++i) {
      EXPECT_EQ(samples[i], new_smsg->samples()[i]);
    }
  }

  // FILTER MSG TEST
  TEST_F(MessagesTest, FilterMsgTest) {
    std::string array_name = "filter_test";
    std::string expr = "expr asdfasdfsf";
    std::string result_array_name = "result_filter_array";
    FilterMsg msg = FilterMsg(array_name, expr, result_array_name);

    std::pair<char*, int> serial = msg.serialize();

    FilterMsg* new_msg = FilterMsg::deserialize(serial.first, serial.second);

    // comparing message contents
    EXPECT_STREQ(array_name.c_str(), new_msg->array_name().c_str());
    EXPECT_STREQ(expr.c_str(), new_msg->expression().c_str());
    EXPECT_STREQ(result_array_name.c_str(), new_msg->result_array_name().c_str());
  }

  // ACK MSG TEST
  TEST_F(MessagesTest, AckMsgTest) {

    AckMsg::Result result = AckMsg::DONE;
    int tag = 3;
    double time = 34.4;
    AckMsg msg = AckMsg(result, tag, time);

    std::pair<char*, int> buffer = msg.serialize();

    AckMsg* new_msg = AckMsg::deserialize(buffer.first, buffer.second);

    // comparing message contents
    EXPECT_EQ(result, new_msg->result());
    EXPECT_EQ(tag, new_msg->tag());
    EXPECT_EQ(time, new_msg->time());

  }

  // BOUNDING COORDINATES MSG TEST
  TEST_F(MessagesTest, BoundingCoordsMsgTest) {

    std::vector<uint64_t> samples;
    StorageManager::BoundingCoordinates bounding_coords;
    int num_dim = 3;
    int num_pairs = 123;

    for (int i = 0; i < num_pairs; ++i) {
      std::vector<double> coords1;
      std::vector<double> coords2;
      for (int j = 0; j < num_dim; ++j) {
        coords1.push_back((double) rand());
        coords2.push_back((double) rand());
      }
      bounding_coords.push_back(
          StorageManager::BoundingCoordinatesPair(coords1, coords2));
    }

    BoundingCoordsMsg msg = BoundingCoordsMsg(bounding_coords);

    std::pair<char*, int> buf_pair = msg.serialize();

    BoundingCoordsMsg* new_msg = BoundingCoordsMsg::deserialize(buf_pair.first, buf_pair.second);

    // comparing message contents
    EXPECT_EQ(num_pairs, new_msg->bounding_coordinates().size());
    for (int i = 0; i < num_pairs; ++i) {

      std::vector<double> new_coords1 = new_msg->bounding_coordinates()[i].first;
      std::vector<double> new_coords2 = new_msg->bounding_coordinates()[i].second;
      std::vector<double> old_coords1 = bounding_coords[i].first;
      std::vector<double> old_coords2 = bounding_coords[i].second;


      EXPECT_EQ(num_dim, new_coords1.size());
      EXPECT_EQ(num_dim, new_coords2.size());

      for (int j = 0; j < num_dim; ++j) {
        EXPECT_EQ(old_coords1[j], new_coords1[j]);
        EXPECT_EQ(old_coords2[j], new_coords2[j]);
      }

    }
  }

  // TILE MSG TEST
  TEST_F(MessagesTest, TileMsgTest) {

    std::string array_name = "test";
    int attr_id = 8;
    uint64_t num_cells = 10000;
    uint64_t cell_size = 8; // bytes
    uint64_t payload_size = num_cells * cell_size;
    char *payload = new char[payload_size];
    for (int i = 0; i < num_cells; ++i) {
      uint64_t cell = (uint64_t)rand();
      memcpy(&payload[i * cell_size], &cell, cell_size);
    }

    TileMsg msg(array_name, attr_id, payload, num_cells, cell_size);
    
    std::pair<char*, int> buf_pair = msg.serialize();

    TileMsg* new_msg = TileMsg::deserialize(buf_pair.first, buf_pair.second);

    // comparing message contents
    EXPECT_STREQ(array_name.c_str(), new_msg->array_name().c_str());
    EXPECT_EQ(attr_id, new_msg->attr_id());
    EXPECT_EQ(num_cells, new_msg->num_cells());
    EXPECT_EQ(cell_size, new_msg->cell_size());
    EXPECT_EQ(payload_size, new_msg->payload_size());
    EXPECT_STREQ(std::string(payload, payload_size).c_str(), new_msg->payload());
  }

}
