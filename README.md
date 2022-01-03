# dset-split
This Vol connector creates separate sub files for each dataset created and mounts those sub-files as external links in the main file.

## HDF5 Versioning with Data Version Control (DVC).

DVC is a data and ML experiment management tool which allows managing datasets, models and code together. Among other features, it provides reproducibility and data provenance to track the evolution of machine learning models. DVC allows tracking the versions of datasets for full reproducibility, however the tracking provided by DVC is at file level. This creates high overheads in huge files like HDF5 where a single file can host multiple datasets. Changes in any one of the dataset would cause the entire file to be duplicated resulting in suboptimal utilization of storage and network bandwidth.
 
'dset-split' Vol connector creates separate file for each individual dataset created in the main file (the file explicitly created from the application) and mounts it on the main file as external links. The vol connector creates individual files for each dataset created. It enables versioning of HDF5 files at a dataset boundary.
The main file and each individual dataset files can be managed as a single unit with the read and write operations happening transparently using the main file handle. The changes in the underlying datasets are tracked at the individual file boundary. Whenever a dataset changes only the split file containing that dataset and main file needs to be duplicated. The files hosting the other datasets which are part of the main file in unchanged hence not duplicated. This increases the storage and bandwidth efficiency, when versioning large HDF5 files. 

 [More information](docs/README.md)
 
Some configuration parameters used in the instructions:
```bash
    VOL_DIR : Directory of HDF5 dset-split VOL connector repository   
    H5_DIR  : Directory of HDF5 install path
 ```

## Code Download
Download the dset-split VOL connector code (this repository) 
```bash
git clone --recursive https://github.com/hpc-io/vol-dset-split.git
```
Download the HDF5 source code
```bash
git clone https://github.com/HDFGroup/hdf5.git
```
## Compile HDF5
```bash
> cd $H5_DIR
> ./autogen.sh  (may skip this step if the configure file exists)
> ./configure --prefix=$H5_DIR --enable-parallel #(may need to add CC=cc or CC=mpicc)
> make && make install
```
## Compile dset-split VOL connector
```bash
> cd $VOL_DIR
> # Edit "Makefile"
> # Change the path of HDF5_DIR to H5_DIR
> make
```

## Set Environment Variables
Will need to set the following environmental variable before running application, e.g.:

for Linux:
```bash
> export LD_LIBRARY_PATH=$VOL_DIR:$H5_DIR/lib:$LD_LIBRARY_PATH
> export HDF5_PLUGIN_PATH="$VOL_DIR"
> export HDF5_VOL_CONNECTOR="dset-split under_vol=0;under_info={}" 
```
Sample export file is in test_app/sample_export.txt
## Run with dset-split
```bash
> # Set environment variables: HDF5_PLUGIN_PATH and HDF5_VOL_CONNECTOR
> Run your application
```
Each dataset will be created inside a separate folder name "filename-split" in the same directory of the main file and mounted on the main file.
The naming convention of the dataset splitfile is "datasetname-time.split"

## Testing with DVC

Install dvc
```bash
pip install dvc
```

Create a new directory and copy a sample application
```
mkdir dvc-hdf-test

#Init git and dvc
git init
dvc init

#Add a dvc remote.
dvc remote add -d myremote /tmp/remote

cp $VOL_DIR/test_app/h5_write
cp $VOL_DIR/test_app/h5_read
cp $VOL_DIR/test_app/h5_append
```

Create hdf5 files with vol connector
Modify HDF5_PLUGIN_PATH in $VOL_DIR/test_app/sampe_export.txt to point to your $VOL_DIR path
```
source $VOL_DIR/test_app/sampe_export.txt
```
Run the write workload:- This will create a file dvc-test.h5 with 10 datasets. The datasets are created
in a separate file under folder dvc-test-split.
```
./h5_write
```
Read the contents of IntArray-9 dataset
```
./h5_read
```
Add the files to dvc
```
dvc add dvc-test.h5 dvc-test-split
```
Push the files to remote storage:- The main file, the directory metadata file and 10 dataset files get pushed to remote.
After executing this command, you should see a line - 12 files pushed.
```
dvc push
```
Modify one of the datasets
This command will modify IntArray-9 dataset
```
./h5_append
```
To see the modified contents
```
./h5_read
```

To see the changes tracked by dvc, 
```
dvc status
```
Push the changes to remote.

Instead of the entire file, only the main file, and the directory metadata file and IntArray-9 dataset file is pushed to remote.
```
dvc push
```
3 files pushed


## Not Supported Operations 
All References Functions like H5Rcreate, H5Rdereference, H5Rget_object_type are currently not supported  

