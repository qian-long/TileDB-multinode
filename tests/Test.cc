#include "gtest/gtest.h"

int main(int argc, char **argv) {
  printf("Running all unit tests\n");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
