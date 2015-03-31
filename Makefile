##########
# Macros #
##########

# --- Compiler --- #
#CXX = g++
CXX = mpic++.mpich2
DFLAGS = -DDEBUG

# --- Directories --- #
CORE_INCLUDE_DIR = core/include
CORE_SRC_DIR = core/src
CORE_OBJ_DIR = core/obj
CORE_BIN_DIR = core/bin
TILEDB_SRC_DIR = tiledb/src
TILEDB_OBJ_DIR = tiledb/obj
TILEDB_BIN_DIR = tiledb/bin
GTEST_DIR = gtest
GTEST_INCLUDE_DIR = gtest/include
GTEST_SRC_DIR = gtest/src
GTEST_OBJ_DIR = gtest/obj
GTEST_BIN_DIR = gtest/bin
TEST_SRC_DIR = test/src
TEST_OBJ_DIR = test/obj
TEST_BIN_DIR = test/bin
DOC_DIR = doc
MULTINODE_SRC_DIR = src
MULTINODE_OBJ_DIR = obj
UNIT_TEST_DIR = tests

# --- Paths --- #
CORE_INCLUDE_PATHS = -I$(CORE_INCLUDE_DIR)
MULTINODE_INCLUDE_PATHS = -I$(MULTINODE_SRC_DIR)

# --- Files --- #
CORE_INCLUDE := $(wildcard $(CORE_INCLUDE_DIR)/*.h)
CORE_SRC := $(wildcard $(CORE_SRC_DIR)/*.cc)
CORE_OBJ := $(patsubst $(CORE_SRC_DIR)/%.cc, $(CORE_OBJ_DIR)/%.o, $(CORE_SRC))
TILEDB_SRC := $(wildcard $(TILEDB_SRC_DIR)/*.cc)
TILEDB_OBJ := $(patsubst $(TILEDB_SRC_DIR)/%.cc, $(TILEDB_OBJ_DIR)/%.o, $(TILEDB_SRC))
TILEDB_BIN := $(patsubst $(TILEDB_SRC_DIR)/%.cc, $(TILEDB_BIN_DIR)/%, $(TILEDB_SRC))
GTEST_INCLUDE := $(wildcard $(GTEST_INCLUDE_DIR)/*.h)
GTEST_OBJ := $(patsubst $(GTEST_SRC_DIR)/%.cc, $(GTEST_OBJ_DIR)/%.o, $(GTEST_SRC))
TEST_SRC := $(wildcard $(TEST_SRC_DIR)/*.cc)
TEST_OBJ := $(patsubst $(TEST_SRC_DIR)/%.cc, $(TEST_OBJ_DIR)/%.o, $(TEST_SRC))
MULTINODE_INCLUDE := $(wildcard $(MULTINODE_SRC_DIR)/*.h)
MULTINODE_SRC := $(wildcard $(MULTINODE_SRC_DIR)/*.cc)
MULTINODE_OBJ := $(patsubst $(MULTINODE_SRC_DIR)/%.cc, $(MULTINODE_OBJ_DIR)/%.o, $(MULTINODE_SRC))
UNIT_TEST_SRC := $(wildcard $(UNIT_TEST_DIR)/*.cc)
UNIT_TEST_OBJ := $(UNIT_TEST_SRC:.cc=.o)

# --- Executables --- #
MULTINODE_EXEC = multinode_launcher
GTESTER = gtester


###################
# General Targets #
###################

.PHONY: core example gtest test doc doc_doxygen clean_core clean_example clean_gtest clean_test clean_multinode clean_tiledb clean

all: core gtest test tiledb doc

core: $(CORE_OBJ) 

tiledb: $(TILEDB_BIN)

gtest: $(GTEST_OBJ_DIR)/gtest-all.o

test: $(GTESTER)
	./$(GTESTER)

doc: doxyfile.inc

clean: clean_core clean_example clean_gtest clean_test clean_multinode clean_tiledb
	rm -f *.o

########
# Core #
########

# --- Compilation and dependency genration --- #

-include $(CORE_OBJ:.o=.d)

$(CORE_OBJ_DIR)/%.o: $(CORE_SRC_DIR)/%.cc
	mkdir -p $(CORE_OBJ_DIR)
	$(CXX) $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_core:
	rm -f $(CORE_OBJ_DIR)/* $(CORE_BIN_DIR)/* 

##########
# TileDB #
##########

# --- Compilation and dependency genration --- #

-include $(TILEDB_OBJ:.o=.d)

$(TILEDB_OBJ_DIR)/%.o: $(TILEDB_SRC_DIR)/%.cc
	mkdir -p $(TILEDB_OBJ_DIR)
	$(CXX) $(CORE_INCLUDE_PATHS) -c $< -o $@
	@$(CXX) -MM $(CORE_INCLUDE_PATHS) $< > $(@:.o=.d)
	@mv -f $(@:.o=.d) $(@:.o=.d.tmp)
	@sed 's|.*:|$@:|' < $(@:.o=.d.tmp) > $(@:.o=.d)
	@rm -f $(@:.o=.d.tmp)

clean_tiledb:
	rm -f $(TILEDB_OBJ_DIR)/* $(TILEDB_BIN_DIR)/* 

# --- Linking --- #

$(TILEDB_BIN_DIR)/tiledb: $(TILEDB_OBJ_DIR)/tiledb.o \
 $(CORE_OBJ_DIR)/*.o
	@mkdir -p $(TILEDB_BIN_DIR)
	$(CXX) -fopenmp $(INCLUDE_PATHS) -o $@ $^

###############
# Google test #
###############

$(GTEST_OBJ_DIR)/gtest-all.o: CXX = g++
$(GTEST_OBJ_DIR)/gtest-all.o: CXXFLAGS += -I$(GTEST_INCLUDE_DIR) -I$(GTEST_DIR) -DGTEST_HAS_TR1_TUPLE=0
$(GTEST_OBJ_DIR)/gtest-all.o: gtest/src/gtest-all.cc $(wildcard gtest/include/gtest/*.h)
	mkdir -p $(GTEST_OBJ_DIR)
	$(CXX) -isystem $(GTEST_INCLUDE_DIR) -I$(GTEST_DIR) -pthread -c $< -o $@

libgtest.a: $(GTEST_OBJ_DIR)/gtest-all.o
	ar r $@ $<

clean_gtest:
	rm -f $(GTEST_OBJ_DIR)/* $(GTEST_BIN_DIR)/*

#########
# Tests #
#########

# Building GTester
$(GTESTER): CXXFLAGS += -I$(GTEST_INCLUDE_DIR) $(CORE_INCLUDE_PATHS) $(MULTINODE_INCLUDE_PATHS)
$(GTESTER): $(CORE_INCLUDE) $(CORE_OBJ) $(UNIT_TEST_OBJ) $(MULTINODE_OBJ) libgtest.a
$(GTESTER):
	$(CXX) $(CXXFLAGS) -o $@ $(UNIT_TEST_OBJ) $(CORE_OBJ) $(MULTINODE_OBJ) -fopenmp libgtest.a -lpthread

clean_test:
	rm -f $(GTESTER) libgtest.a $(UNIT_TEST_DIR)/*.o
#########################
# Documentation doxygen #
#########################

doxyfile.inc: $(CORE_INCLUDE)
	@echo INPUT         =  $(DOC_DIR)/mainpage.dox $(CORE_INCLUDE) > doxyfile.inc
	@echo FILE_PATTERNS =  *.h >> doxyfile.inc
	doxygen Doxyfile.mk

####################
# Multinode TileDB #
####################
# prints out debug messages
multi-debug: CXX = mpic++.mpich2
multi-debug: CXX += -DDEBUG -g
multi-debug: multi

# compiling multinode src
$(MULTINODE_OBJ_DIR)/%.o: $(MULTINODE_SRC_DIR)/%.cc
	mkdir -p $(MULTINODE_OBJ_DIR)
	$(CXX) $(CORE_INCLUDE_PATHS) $(MULTINODE_INCLUDE_PATHS) -c $< -o $@

# $< gets name of first matching dependency, $@ gets target name
# $^ The names of all the prerequisites
main.o: main.cc
	$(CXX) $(CORE_INCLUDE_PATHS) $(MULTINODE_INCLUDE_PATHS) -c $< -o $@

# linking
$(MULTINODE_EXEC): main.o $(MULTINODE_OBJ) $(CORE_OBJ)
	$(CXX) -fopenmp $(CORE_INCLUDE_PATHS) $(MULTINODE_INCLUDE_PATHS) -o $@ $^

multi: $(MULTINODE_EXEC)
	./setup_env.sh

multi-run: $(MULTINODE_EXEC)
	./setup_env.sh
	mpiexec -f machinefile_prod ./$(MULTINODE_EXEC)

multi-run-local: $(MULTINODE_EXEC)
	./setup_env.sh
	mpiexec -f machinefile_local ./$(MULTINODE_EXEC)

multi-run-istc: $(MULTINODE_EXEC)
	./setup_env.sh
	mpiexec.mpich2 -f machinefile_local ./$(MULTINODE_EXEC)


clean_multinode:
	rm -f $(MULTINODE_EXEC) $(MULTINODE_OBJ_DIR)/* main.o


# LIB_PATHS = /usr/local/lib/libspatialindex.so
# LIBS = -lpqxx -lpthread
