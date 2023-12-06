TEST_DIR = /cad2/ece326f/tester/asst4

CXX = g++
LDFLAGS = -pthread
LD = g++
CXXFLAGS = -std=c++11 -Igoogletest/googletest/include -Igoogletest/googletest/

GTEST_SRCS = \
	googletest/googletest/src/gtest-all.cc \
	googletest/googletest/src/gtest_main.cc \

# tester programs
include $(TEST_DIR)/tester.mk

# add extra programs here for your own testing
PROGS = $(TEST_PROGS)

CXXFLAGS_Release = -O3 -Wall
CXXFLAGS_Debug = -g -Wall -Wextra -Wunused-result

BUILD=Release
 
include cc.mk

realclean: clean
	$(foreach var, $(TEST_PROGS), rm -rf $(var).cc;)

setup:
	$(foreach var, $(TEST_PROGS), ln -s $(TEST_DIR)/$(var).cc $(var).cc;)

.PHONY: realclean setup

