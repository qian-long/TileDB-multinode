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
        array_schema = ArraySchema(array_name,
            attribute_names,
            dim_names,
            dim_domains,
            types,
            ArraySchema::ROW_MAJOR);


      }

      virtual void TearDown() {}

      // Test variables
      ArraySchema array_schema;
  };

  TEST_F(MessagesTest, LoadMsgTest) {

    LoadMsg lmsg = LoadMsg("foo.csv", array_schema);

    std::pair<char*, int> lserial = lmsg.serialize();

    LoadMsg* new_lmsg = LoadMsg::deserialize(lserial.first, lserial.second);

    // comparing message contents
    EXPECT_STREQ(lmsg.filename().c_str(), (new_lmsg->filename()).c_str());
    EXPECT_STREQ(
      lmsg.array_schema().to_string().c_str(), 
      (new_lmsg->array_schema()).to_string().c_str());
  }


  TEST_F(MessagesTest, FilterMsgTest) {
    int attr_index = 1;
    Op op = GT;
    int operand = 4;
    Predicate<int> pred(attr_index, op, operand);
    std::string result_array_name = "filter_test";
    FilterMsg<int> fmsg = FilterMsg<int>(
      array_schema.celltype(attr_index), 
      array_schema, 
      pred, 
      result_array_name);

    std::pair<char*, int> fserial = fmsg.serialize();

    FilterMsg<int>* new_fmsg = FilterMsg<int>::deserialize(fserial.first, fserial.second); 

    // comparing message contents
    EXPECT_STREQ(fmsg.array_schema().to_string().c_str(),
        new_fmsg->array_schema().to_string().c_str());

    EXPECT_STREQ(fmsg.result_array_name().c_str(),
        new_fmsg->result_array_name().c_str());

    EXPECT_STREQ(fmsg.predicate().to_string().c_str(),
        (new_fmsg->predicate()).to_string().c_str());

    EXPECT_EQ(fmsg.attr_type(), new_fmsg->attr_type());

    // cleanup
    delete new_fmsg;
   }
}
