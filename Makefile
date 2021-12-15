#Copyright 2021 Hewlett Packard Enterprise Development LP.

CC=mpicc
HDF5_DIR=/usr/local/hdf5
HDF5_BUILD_DIR=/home/royann/hdf5-1.13.0
CFLAGS=-I$(HDF5_DIR)/include 
LIBS= -L$(HDF5_DIR)/lib  -lhdf5 
TARGET=libh5dsetsplit.so

# Testcase section
TEST_C_FLAGS = -m64 -DUSE_V4_SSE -DOMPI_SKIP_MPICXX -DPARALLEL_IO
TEST_LIB_FLAGS   = -L$(HDF5_BUILD_DIR)/src/.libs -L$(HDF5_DIR)/lib -L./ -lh5dsetsplit -lhdf5 -lz -lm -ldl
TEST_SRC = vpicio_uni_h5.c
TEST_BIN = vpicio_uni_h5
.PHONY: all test clean
all: makeso test

debug:
	$(CC)  -Wall -Wunused-parameter  -shared  -o $(TARGET) $(CFLAGS) -ggdb -fPIC H5VLdsetsplit.c  $(LIBS)
makeso:
	$(CC)  -Wall -Wunused-parameter  -shared  -o $(TARGET) $(CFLAGS)  -fPIC H5VLdsetsplit.c  $(LIBS)

test:
	$(CC) -Wall -Wunused-parameter  -o $(TEST_BIN) $(CFLAGS) $(TEST_C_FLAGS) $(TEST_SRC)  $(TEST_LIB_FLAGS)
clean:
	rm -rf $(TEST_BIN) $(TARGET) *.h5 *-split
