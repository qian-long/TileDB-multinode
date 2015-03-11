#include "gtest/gtest.h"
#include "messages.h"

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
    LoadMsg::LoadType load_type = LoadMsg::ORDERED;
    LoadMsg lmsg = LoadMsg(filename, array_schema_, load_type);

    std::pair<char*, int> lserial = lmsg.serialize();

    LoadMsg* new_lmsg = LoadMsg::deserialize(lserial.first, lserial.second);

    // comparing message contents
    EXPECT_STREQ(filename.c_str(), new_lmsg->filename().c_str());
    EXPECT_STREQ(array_schema_.to_string().c_str(),
      new_lmsg->array_schema().to_string().c_str());
    EXPECT_EQ(load_type, new_lmsg->load_type());
  }


  // FILTER MSG TEST
  TEST_F(MessagesTest, FilterMsgTest) {
    int attr_index = 1;
    Op op = GT;
    int operand = 4;
    Predicate<int> pred(attr_index, op, operand);
    std::string result_array_name = "filter_test";
    FilterMsg<int> fmsg = FilterMsg<int>(
      array_schema_.celltype(attr_index), 
      array_schema_, 
      pred, 
      result_array_name);

    std::pair<char*, int> fserial = fmsg.serialize();

    FilterMsg<int>* new_fmsg = FilterMsg<int>::deserialize(fserial.first, fserial.second); 

    // comparing message contents
    EXPECT_STREQ(array_schema_.to_string().c_str(),
        new_fmsg->array_schema().to_string().c_str());

    EXPECT_STREQ(result_array_name.c_str(),
        new_fmsg->result_array_name().c_str());

    EXPECT_STREQ(pred.to_string().c_str(),
        new_fmsg->predicate().to_string().c_str());

    EXPECT_EQ(array_schema_.celltype(attr_index), new_fmsg->attr_type());

    // cleanup
    delete new_fmsg;
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

    auto it1 = ranges.begin();
    auto it2 = new_smsg->ranges().begin();
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


  // ARRAY SCHEMA MSG TEST
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
    ParallelLoadMsg::ParallelLoadType load_type = ParallelLoadMsg::NAIVE;

    ParallelLoadMsg pmsg = ParallelLoadMsg(filename, load_type, array_schema_);

    std::pair<char*, int> pserial = pmsg.serialize();

    ParallelLoadMsg* new_pmsg = ParallelLoadMsg::deserialize(pserial.first, pserial.second);

    // comparing message contents
    EXPECT_STREQ(filename.c_str(), new_pmsg->filename().c_str());
    EXPECT_EQ(load_type, new_pmsg->load_type());
    EXPECT_STREQ(array_schema_.to_string().c_str(),
      new_pmsg->array_schema().to_string().c_str());

  }
}
