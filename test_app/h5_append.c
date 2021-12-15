/*Copyright 2021 Hewlett Packard Enterprise Development LP.*/
/*
 *  This example writes data to the HDF5 file.
 *  Data conversion is performed during write operation.
 */

#include "hdf5.h"

#define H5FILE_NAME "dvc-test.h5"
#define DATASETNAME "IntArray-9"
#define NX          5 /* dataset dimensions */
#define NY          6
#define RANK        2

int
main(void)
{
    hid_t   file, dataset;       /* file and dataset handles */
    hid_t   datatype, dataspace; /* handles */
    hsize_t dimsf[2];            /* dataset dimensions */
    herr_t  status;
    int    read_data[NX][NY]; /* data to read */

    int    write_data[NX][NY]; /* data to write */
    int     i, j;

    /*
     * Data  and output buffer initialization.
     */
    for (j = 0; j < NX; j++)
        for (i = 0; i < NY; i++)
            write_data[j][i] = i * j;
    /*
     * 0 0 0 0  0  0
     * 0 1 2 3  4  5
     * 0 2 4 5  8  10
     * 0 3 6 9  12 15
     * 0 4 8 12 16 20
     */

    /*
     * Create a new file using H5F_ACC_RDWR access,
     * default file creation properties, and default file
     * access properties.
     */
    file = H5Fopen(H5FILE_NAME, H5F_ACC_RDWR, H5P_DEFAULT);

    /*
     * Describe the size of the array and create the data space for fixed
     * size dataset.
     */
    dimsf[0]  = NX;
    dimsf[1]  = NY;
    dataspace = H5Screate_simple(RANK, dimsf, NULL);

    /*
     * Define datatype for the data in the file.
     * We will store little endian INT numbers.
     */
    datatype = H5Tcopy(H5T_NATIVE_INT);
    status   = H5Tset_order(datatype, H5T_ORDER_LE);

    /*
     * Create a new dataset within the file using defined dataspace and
     * datatype and default dataset creation properties.
     */
    dataset = H5Dopen(file, DATASETNAME, H5P_DEFAULT);

    /*
     * Write the data to the dataset using default transfer properties.
     */
    status = H5Dwrite(dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, write_data);

    /*
     * Close/release resources.
     */
    H5Sclose(dataspace);
    H5Tclose(datatype);
    H5Dclose(dataset);
    H5Fclose(file);

    return 0;
}
