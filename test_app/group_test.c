/*Copyright 2021 Hewlett Packard Enterprise Development LP.*/
/*
 * This program creates a group in the file and two datasets in the group.
 * Hard link to the group object is created and one of the datasets is accessed
 * under new name.
 * Iterator functions are used to find information about the objects
 * in the root group and in the created group.
 */


#include "hdf5.h"

#include <stdlib.h>
#define H5FILE_NAME    "master.h5"
#define RANK    2

static herr_t file_info(hid_t loc_id, const char *name, const H5L_info2_t *linfo,
    void *opdata);              /* Link iteration operator function */
static herr_t group_info(hid_t loc_id, const char *name, const H5L_info2_t *linfo,
    void *opdata);              /* Link iteration operator function */
int
main(void)
{

    hid_t    file;
    hid_t    grp, grp2, grp3, grp4;
    hid_t    dataset,dataset2,dataset3, dataspace, fspace, fset;
    hid_t    plist;

    herr_t   status;
    hsize_t  dims[2];
    hsize_t  cdims[2];

    int      idx_f, idx_g;


    /*
     * Create a file.
     */
    printf("Creating file\n");
    file = H5Fcreate(H5FILE_NAME, H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

    printf("Creating group Data\n");

    /*Scenario 1*/
    /* Path - absolute path. Group created under the file*/

    /*
     * Create a group in the file.
     */

    printf("Scenario 1  Path - absolute path. Group created under the file\n");
    grp = H5Gcreate2(file, "/Data1", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);

    if(grp < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    /*
     * Create dataset "Compressed Data" in the group using absolute
     * name. Dataset creation property list is modified to use
     * GZIP compression with the compression effort set to 6.
     * Note that compression can be used only when dataset is chunked.
     */
    dims[0] = 20000;
    dims[1] = 20;
    cdims[0] = 20;
    cdims[1] = 20;
     int wdata[20000][20];   
     int i,j;

    /*
     * Initialize data.
     */
    for (i=0; i< 20000; i++)
        for (j=0; j<20; j++)
            wdata[i][j] = i * j ;

    dataspace = H5Screate_simple(RANK, dims, NULL);
    plist     = H5Pcreate(H5P_DATASET_CREATE);
                H5Pset_chunk(plist, 2, cdims);
                H5Pset_deflate( plist, 6);

   /*Scenario 1.1*/
   /* Dataset created with file loc_id */		
   /*Path name - absolute path*/

    printf("Scenario 1.1  Path - absolute path. Dataset created with file loc_id\n");
    dataset = H5Dcreate2(file, "/Data1/Compressed_Data", H5T_NATIVE_INT,
                        dataspace, H5P_DEFAULT, plist, H5P_DEFAULT);


    if(dataset < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    printf("Scenario 1.1  Dataset write\n");
    status = H5Dwrite (dataset, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                wdata[0]);


    if(status < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

   /*Scenario 1.2*/
   /*Dataset created with group loc_id */
   /*Path name - Relative path*/

    printf("Scenario 1.2  Dataset created with group loc_id. Path name - Relative path\n");
    dataset2 = H5Dcreate2(grp, "Compressed_Data2", H5T_NATIVE_INT,dataspace, H5P_DEFAULT, plist, H5P_DEFAULT);


    if(dataset2 < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    printf("Scenario 1.2  Dataset write\n");

    status = H5Dwrite (dataset2, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                wdata[0]);



    if(status < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }


   /*Scenario 1.3*/
   /* Dataset created with group loc_id */
   /*Path name - absolute path*/

    printf("Scenario 1.3  Dataset creat with group loc_id. Path name - Absolute path\n");
    dataset3 = H5Dcreate2(grp, "/Data1/Compressed_Data3", H5T_NATIVE_INT,dataspace, H5P_DEFAULT, plist, H5P_DEFAULT);


    if(dataset3 < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    printf("Scenario 1.3  Dataset write\n");
    status = H5Dwrite (dataset3, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                wdata[0]);


    if(status < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    /*
     * Close the first dataset .
     */
    /*
     * Close the first dataset .
     */
    H5Sclose(dataspace);
    H5Dclose(dataset);
    H5Dclose(dataset2);
    H5Dclose(dataset3);
    /*
     * Create the second dataset.
     */
    dims[0] = 20;
    dims[1] = 20;

    float wdata_f[20][20];   

    float rdata_f[20][20];   
    

    for (i=0; i< 20; i++)
        for (j=0; j<20; j++)
            wdata_f[i][j] = i * j * 0.1;
    fspace = H5Screate_simple(RANK, dims, NULL);


   /*Scenario 1.4*/
   /* Dataset created with file loc_id */
   /*Path name - Relative path*/

    printf("Scenario 1.4  Dataset created with file loc_id. Path name - Relative path\n");
    fset = H5Dcreate2(file, "Data1/Float_Data", H5T_NATIVE_FLOAT,
			fspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);


    if(fset < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }


    printf("Scenario 1.4 dataset write\n");
     status = H5Dwrite (fset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                wdata_f[0]);
     

    if(status < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    /*
     *Close the second dataset and file.
     */
    H5Sclose(fspace);
    H5Dclose(fset);
    H5Pclose(plist);
    H5Gclose(grp);



   /*Scenario 2*/
   /*Group created under the file*/
   /*Path name - Relative path*/
   
    printf("Scenario 2  Group create under file with Relative Path name\n");
    grp = H5Gcreate2(file, "Data2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);


    if(grp < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    dims[0] = 100;
    dims[1] = 20;
    cdims[0] = 20;
    cdims[1] = 20;
    dataspace = H5Screate_simple(RANK, dims, NULL);
    plist     = H5Pcreate(H5P_DATASET_CREATE);
                H5Pset_chunk(plist, 2, cdims);
                H5Pset_deflate( plist, 6);

    printf("Scenario 2.1  Dataset create with file loc_id Path - Absolute path\n");
    dataset = H5Dcreate2(file, "/Data2/Compressed_Data", H5T_NATIVE_INT,
                        dataspace, H5P_DEFAULT, plist, H5P_DEFAULT);
    

    if(dataset < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    /*
     * Close the first dataset .
     */
    H5Sclose(dataspace);
    H5Dclose(dataset);

    dims[0] = 100;
    dims[1] = 20;
    dataspace = H5Screate_simple(RANK, dims, NULL);


   /*Scenario 2.2*/
   /*Dataset created under the group with file loc_id*/
   /*Path name - absolute path*/

    printf("Scenario 2.2  Second Dataset create with grp loc_id Path - Absolute path\n");
    dataset = H5Dcreate2(grp, "/Data2/Float_Data", H5T_NATIVE_FLOAT,
                        dataspace, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);


    if(dataset < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    /*
     *Close the second dataset and file.
     */
    H5Sclose(dataspace);
    H5Dclose(dataset);


    H5Gclose(grp);


   /*Scenario 3*/
   /*Create a group inside the group*/
   /*Path name - absolute path*/

    printf("Scenario 3 Create group inside another group\n");
    grp = H5Gopen2(file, "Data1", H5P_DEFAULT);


    if(grp < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    /*
     * Create a group in the file. with relative  name
     */
    grp2 = H5Gcreate2(grp, "grp2", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);


    if(grp2 < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    dataspace = H5Screate_simple(RANK, dims, NULL);
    plist     = H5Pcreate(H5P_DATASET_CREATE);
                H5Pset_chunk(plist, 2, cdims);
                H5Pset_deflate( plist, 6);

    printf("Scenario 3.1 Create dataset  inside nested group\n");
    dataset = H5Dcreate2(grp2, "Compressed_Data2", H5T_NATIVE_INT,
                        dataspace, H5P_DEFAULT, plist, H5P_DEFAULT);


    if(dataset < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    printf("Scenario 3.1 Dataset write\n");
     status = H5Dwrite (dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL, H5P_DEFAULT,
                wdata_f[0]);


     H5Dclose(dataset);
     H5Gclose(grp2);

     grp2 = H5Gopen(grp, "grp2", H5P_DEFAULT );
     dataset = H5Dopen(grp2, "Compressed_Data2", H5P_DEFAULT);
     status = H5Dread(dataset, H5T_NATIVE_FLOAT, H5S_ALL, H5S_ALL,H5P_DEFAULT, rdata_f[0]);
     for (int i = 0; i< 20; i++)
	     for(int j = 0; j<20; j++)
		     printf("%f,", rdata_f[i][j]);

    if(status < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }


    printf("Scenario 3.2 Nested group - Path - Absolute path\n");
    grp3 = H5Gcreate2(grp, "/Data1/grp3", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);


    if(grp3 < 0)
    {
        printf("Error Existing\n");
	exit(0);
    }

    printf("Scenario 3.3 Nested group - Negative testing - The following test should raise exceptions\n");
    printf("Scenario 3.3 Nested group - Path - Absolute path- where part of the path does not exist\n");
    grp4 = H5Gcreate2(file, "/Ann/grp3", H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);


    if(grp4 < 0)
    {
        printf("Error Path\n");
    }

    H5Dclose(dataset);
    H5Fclose(file);

    return 0;
}

/*
 * Operator function.
 */
static herr_t
file_info(hid_t loc_id, const char *name, const H5L_info2_t *linfo, void *opdata)
{
    /* avoid compiler warnings */
    loc_id = loc_id;
    opdata = opdata;
    linfo = linfo;

    /*
     * Display group name. The name is passed to the function by
     * the Library. Some magic :-)
     */
    printf("\nName : %s\n", name);

    return 0;
}


/*
 * Operator function.
 */
static herr_t
group_info(hid_t loc_id, const char *name, const H5L_info2_t *linfo, void *opdata)
{
    hid_t did;  /* dataset identifier  */
    hid_t tid;  /* datatype identifier */
    H5T_class_t t_class;
    hid_t pid;  /* data_property identifier */
    hsize_t chunk_dims_out[2];
    int  rank_chunk;

    /* avoid warnings */
    opdata = opdata;
    linfo = linfo;

    /*
     * Open the datasets using their names.
     */
    did = H5Dopen2(loc_id, name, H5P_DEFAULT);

    /*
     * Display dataset name.
     */
    printf("\nName : %s\n", name);

    /*
     * Display dataset information.
     */
    tid = H5Dget_type(did);  /* get datatype*/
    pid = H5Dget_create_plist(did); /* get creation property list */

    /*
     * Check if dataset is chunked.
     */
    if(H5D_CHUNKED == H5Pget_layout(pid)) {
        /*
         * get chunking information: rank and dimensions.
         */
        rank_chunk = H5Pget_chunk(pid, 2, chunk_dims_out);
        printf("chunk rank %d, dimensions %lu x %lu\n", rank_chunk,
            (unsigned long)(chunk_dims_out[0]),
            (unsigned long)(chunk_dims_out[1]));
    }
    else {
        t_class = H5Tget_class(tid);
        if(t_class < 0) {
            puts(" Invalid datatype.\n");
        }
        else {
            if(t_class == H5T_INTEGER)
                puts(" Datatype is 'H5T_NATIVE_INTEGER'.\n");
            if(t_class == H5T_FLOAT)
                puts(" Datatype is 'H5T_NATIVE_FLOAT'.\n");
            if(t_class == H5T_STRING)
                puts(" Datatype is 'H5T_NATIVE_STRING'.\n");
            if(t_class == H5T_BITFIELD)
                puts(" Datatype is 'H5T_NATIVE_BITFIELD'.\n");
            if(t_class == H5T_OPAQUE)
                puts(" Datatype is 'H5T_NATIVE_OPAQUE'.\n");
            if(t_class == H5T_COMPOUND)
                puts(" Datatype is 'H5T_NATIVE_COMPOUND'.\n");
        }
    }

    H5Dclose(did);
    H5Pclose(pid);
    H5Tclose(tid);
    return 0;
}

