#include "gtest/gtest.h"
#include "array_schema.h"

#include <iostream>
namespace {

  // Fixture class for bundling tests together
  // empty for now but can add setup and cleanup later
  class ArraySchemaTest: public ::testing::Test {};

  TEST_F(ArraySchemaTest, ArraySchemaSerializeTest) {

    std::string array_name = "ais_allzones";
    // Set attribute names
    std::vector<std::string> attribute_names;
    attribute_names.push_back("sog"); // float
    attribute_names.push_back("cog"); // float
    attribute_names.push_back("heading"); // float
    attribute_names.push_back("rot"); // float
    attribute_names.push_back("status"); // int
    attribute_names.push_back("voyageid"); // int64_t
    attribute_names.push_back("mmsi"); // int64_t


    // Set attribute types
    std::vector<const std::type_info*> types;
    types.push_back(&typeid(float));
    types.push_back(&typeid(float));
    types.push_back(&typeid(float));
    types.push_back(&typeid(float));
    types.push_back(&typeid(int));
    types.push_back(&typeid(int64_t));
    types.push_back(&typeid(int64_t));


    // Set dimension type
    types.push_back(&typeid(int64_t));

    // Set dimension names
    std::vector<std::string> dim_names;
    dim_names.push_back("coordX");
    dim_names.push_back("coordY");

    // Set dimension domains
    std::vector<std::pair<double,double> > dim_domains;
    dim_domains.push_back(std::pair<double,double>(0, 360000000));
    dim_domains.push_back(std::pair<double,double>(0, 180000000));


    // Create an array with irregular tiles
    ArraySchema array_schema = ArraySchema(array_name,
        attribute_names,
        dim_names,
        dim_domains,
        types,
        ArraySchema::HILBERT);

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
