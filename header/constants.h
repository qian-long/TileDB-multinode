#ifndef CONSTANTS_H
#define CONSTANTS_H
#include <cstdlib>

#define MASTER 0 // coordinator node id
#define MAX_DATA 4000 // num bytes
#define QUIT_TAG 0
#define DEF_TAG 1 // default
#define GET_TAG 2 // msg == array_name to get,
#define INIT_TAG 3 // partition data, send array schema
#define ARRAY_SCHEMA_TAG 4
#define LOAD_TAG 5

#endif
