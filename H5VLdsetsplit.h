/*Copyright 2021 Hewlett Packard Enterprise Development LP.*/

/*
 * Purpose:	The public header file for the dset_split VOL connector.
 */

#ifndef H5VLdset_split_H
#define H5VLdset_split_H

/* Public headers needed by this file */
#include "H5VLpublic.h" /* Virtual Object Layer                 */

/* Identifier for the dset-split VOL connector */
#define H5VL_DSET_SPLIT (H5VL_dset_split_register())

/* Characteristics of the dset-split VOL connector */
#define H5VL_DSET_SPLIT_NAME    "dset-split"

/*TO DO - TEMPORARY CONNNECTOR ID - NOT YET REGISTERED*/
#define H5VL_DSET_SPLIT_VALUE   909 /* VOL connector ID */
#define H5VL_DSET_SPLIT_VERSION 0

/* Pass-through VOL connector info */
typedef struct H5VL_dset_split_info_t {
    hid_t under_vol_id;   /* VOL ID for under VOL */
    void *under_vol_info; /* VOL info for under VOL */
} H5VL_dset_split_info_t;

#ifdef __cplusplus
extern "C" {
#endif

H5_DLL hid_t H5VL_dset_split_register(void);

#ifdef __cplusplus
}
#endif

#endif /* H5VLdset_split_H */
