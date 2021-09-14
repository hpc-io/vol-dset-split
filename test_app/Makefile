#Copyright 2021 Hewlett Packard Enterprise Development LP.

# Edit the following variables as needed

HDF_INSTALL = /usr/local/hdf5
EXTLIB = -L$(HDF_INSTALL)/lib
CC          = mpicc
CFLAGS      = 
LIB         =  -lm

INCLUDE   = -I$(HDF_INSTALL)/include
LIBSHDF   = $(EXTLIB) $(HDF_INSTALL)/lib/libhdf5.so 

all: group_test \
     h5_write 
     

group_test: group_test.c
	$(CC) $(CFLAGS) -o $@ group_test.c $(INCLUDE) $(LIBSHDF) $(LIB)

h5_write: h5_write.c
	$(CC) $(CFLAGS) -o $@ h5_write.c $(INCLUDE) $(LIBSHDF) $(LIB)
clean: 
	rm -f *.h5 *.o *.split\
        group_test \
	h5_write\

.SUFFIXES:.o.c
