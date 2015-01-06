#include "gtest/gtest.h"
#include "messages.h"

namespace {

  class MessagesTest: public ::testing::Test {};

  TEST_F(MessagesTest, LoadMsgTest) {
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
    ArraySchema* array_schema = new ArraySchema(array_name,
        attribute_names,
        dim_names,
        dim_domains,
        types,
        ArraySchema::ROW_MAJOR);


    LoadMsg lmsg = LoadMsg("foo.csv", array_schema);

    std::string lserial = lmsg.serialize();

    
    LoadMsg* new_lmsg = LoadMsg::deserialize(lserial.c_str(), lserial.length());

    EXPECT_STREQ(lmsg.filename().c_str(), (new_lmsg->filename()).c_str());
    EXPECT_STREQ(((lmsg.array_schema())->to_string()).c_str(), ((new_lmsg->array_schema())->to_string()).c_str());
  }

}
