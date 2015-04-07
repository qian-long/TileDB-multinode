#include "gtest/gtest.h"
#include "metadata_manager.h"

namespace {
  class ArrayManagerTest: public ::testing::Test {
    protected:
      // ran before each test
      virtual void SetUp() {}

      virtual void TearDown() {}
  };

  TEST_F(ArrayManagerTest, MetaDataOrderedTest) {
    PartitionType type = ORDERED_PARTITION;
    std::pair<int64_t, int64_t> my_range = std::pair<int64_t, int64_t>(27, 5749);
    std::vector<int64_t> samples;
    for (int i = 0; i < 10; ++i) {
      samples.push_back(i * 1232);
    }


    MetaData meta = MetaData(type, my_range, samples);
    std::pair<char*, int> serial = meta.serialize();

    MetaData new_meta = MetaData();
    new_meta.deserialize(serial.first, serial.second);

    EXPECT_EQ(type, new_meta.partition_type());
    EXPECT_EQ(samples.size(), new_meta.all_ranges().size());
    for (int i = 0; i < new_meta.all_ranges().size(); ++i) {
      EXPECT_EQ(samples[i], new_meta.all_ranges()[i]);
    }
  }

  TEST_F(ArrayManagerTest, MetaDataHashTest) {
    PartitionType type = HASH_PARTITION;
    MetaData meta = MetaData(type);
    std::pair<char*, int> serial = meta.serialize();

    MetaData new_meta = MetaData();
    new_meta.deserialize(serial.first, serial.second);

    EXPECT_EQ(type, new_meta.partition_type());
  }
}
