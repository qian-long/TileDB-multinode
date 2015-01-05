#include "gtest/gtest.h"
#include "array_schema.h"

#include <iostream>
namespace {

  // Fixture class for bundling tests together
  // empty for now but can add setup and cleanup later
  class ArraySchemaTest: public ::testing::Test {};

  TEST_F(ArraySchemaTest, ArraySchemaSerializeTest) {
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
    ArraySchema array_schema = ArraySchema(array_name,
        attribute_names,
        dim_names,
        dim_domains,
        types,
        ArraySchema::ROW_MAJOR);

    // Used for array schema equality comparison
    std::string expected_str = array_schema.to_string();

    std::pair<char*, uint64_t> pair = array_schema.serialize();

    // Creating new array schema to deserialize into
    ArraySchema new_array_schema = ArraySchema();
    new_array_schema.deserialize(pair.first, pair.second);

    std::string actual_str = new_array_schema.to_string();

    EXPECT_STREQ(expected_str.c_str(), actual_str.c_str());
  }
}
