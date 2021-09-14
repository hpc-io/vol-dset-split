/*Copyright 2021 Hewlett Packard Enterprise Development LP.*/
/*
 * Purpose:     "dset-split" VOL connector, creates datasets as seperate files,
 *              which are mounted as LINKS inside the main file. This is to enable versioning at a dataset level.
 */

/* Header files needed */
/* Do NOT include private HDF5 files here! */
#include <assert.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

/* Public HDF5 file */
#include "hdf5.h"

/* This connector's header */
#include "H5VLdsetsplit.h"
#include "H5VLerror.h"

/**********/
/* Macros */
/**********/

/* Whether to display log messge when callback is invoked */
/* (Uncomment to enable) */
#define DEBUG 
//

/* Hack for missing va_copy() in old Visual Studio editions
 * (from H5win2_defs.h - used on VS2012 and earlier)
 */
#if defined(_WIN32) && defined(_MSC_VER) && (_MSC_VER < 1800)
#define va_copy(D, S) ((D) = (S))
#endif

/* Version of external link format */
#define H5L_EXT_VERSION         0

/* Valid flags for external links */
#define H5L_EXT_FLAGS_ALL       0

/*Length of filename extention (.h5)*/
const uint8_t H5_EXT_LENGTH = 6;
const char FILE_EXTENTION[] = ".split";
const char ATTRIBUTE_NAME[] = "split_file";

/************/
/* Typedefs */
/************/

/* The dset_split VOL info object */
typedef struct H5VL_dset_split_t {
    hid_t under_vol_id; /* ID for underlying VOL connector */
    void *under_object; /* Info object for underlying VOL connector */
    H5I_type_t type;
    hid_t fid;
    int set;
} H5VL_dset_split_t;

/* The dset_split VOL wrapper context */
typedef struct H5VL_dset_split_wrap_ctx_t {
    hid_t under_vol_id;   /* VOL ID for under VOL */
    void *under_wrap_ctx; /* Object wrapping context for under VOL */
} H5VL_dset_split_wrap_ctx_t;

/********************* */
/* Function prototypes */
/********************* */

/* Helper routines */

static H5VL_dset_split_t *H5VL_dset_split_new_obj(void *under_obj, hid_t under_vol_id);
static herr_t H5VL_dset_split_free_obj(H5VL_dset_split_t *obj);
herr_t dset_split_create_attribute(hid_t file_id);
hid_t dset_split_file_create(const char* name, void* obj, H5I_type_t obj_type, hid_t connector_id);
hid_t get_parent_file_fapl(void* file_obj, hid_t connector_id);


/* Management callbacks */
static herr_t H5VL_dset_split_init(hid_t vipl_id);
static herr_t H5VL_dset_split_term(void);

/* VOL info callbacks */
static void * H5VL_dset_split_info_copy(const void *info);
static herr_t H5VL_dset_split_info_cmp(int *cmp_value, const void *info1, const void *info2);
static herr_t H5VL_dset_split_info_free(void *info);
static herr_t H5VL_dset_split_info_to_str(const void *info, char **str);
static herr_t H5VL_dset_split_str_to_info(const char *str, void **info);

/* VOL object wrap / retrieval callbacks */
static void * H5VL_dset_split_get_object(const void *obj);
static herr_t H5VL_dset_split_get_wrap_ctx(const void *obj, void **wrap_ctx);
static void * H5VL_dset_split_wrap_object(void *obj, H5I_type_t obj_type, void *wrap_ctx);
static void * H5VL_dset_split_unwrap_object(void *obj);
static herr_t H5VL_dset_split_free_wrap_ctx(void *obj);

/* Attribute callbacks */
static void * H5VL_dset_split_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                            hid_t type_id, hid_t space_id, hid_t acpl_id, hid_t aapl_id,
                                            hid_t dxpl_id, void **req);
static void * H5VL_dset_split_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                          hid_t aapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id,
                                          void **req);
static herr_t H5VL_dset_split_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id,
                                           void **req);
static herr_t H5VL_dset_split_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                H5VL_attr_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_attr_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_attr_close(void *attr, hid_t dxpl_id, void **req);

/* Dataset callbacks */
static void * H5VL_dset_split_dataset_create(void *obj, const H5VL_loc_params_t *loc_params,
                                               const char *name, hid_t lcpl_id, hid_t type_id, hid_t space_id,
                                               hid_t dcpl_id, hid_t dapl_id, hid_t dxpl_id, void **req);
static void * H5VL_dset_split_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                             hid_t dapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                             hid_t file_space_id, hid_t plist_id, void *buf, void **req);
static herr_t H5VL_dset_split_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id,
                                              hid_t file_space_id, hid_t plist_id, const void *buf,
                                              void **req);
static herr_t H5VL_dset_split_dataset_get(void *dset, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_dataset_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id,
                                                 void **req);
static herr_t H5VL_dset_split_dataset_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

static herr_t H5VL_dset_split_dataset_close(void *dset, hid_t dxpl_id, void **req);

/* Datatype callbacks */
static void *H5VL_dset_split_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params,
                                               const char *name, hid_t type_id, hid_t lcpl_id, hid_t tcpl_id,
                                               hid_t tapl_id, hid_t dxpl_id, void **req);
static void *H5VL_dset_split_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                             hid_t tapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_datatype_get(void *dt, H5VL_datatype_get_args_t *args, hid_t dxpl_id, void **req);
                                             
static herr_t H5VL_dset_split_datatype_specific(void *obj, H5VL_datatype_specific_args_t *args, hid_t dxpl_id, void **req);

static herr_t H5VL_dset_split_datatype_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

static herr_t H5VL_dset_split_datatype_close(void *dt, hid_t dxpl_id, void **req);

/* File callbacks */
static void * H5VL_dset_split_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id,
                                            hid_t dxpl_id, void **req);
static void * H5VL_dset_split_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id,
                                          void **req);
static herr_t H5VL_dset_split_file_get(void *file, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req);

static herr_t H5VL_dset_split_file_specific(void *file, H5VL_file_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_file_optional(void *file, H5VL_optional_args_t *args, hid_t dxpl_id,
                                              void **req);
static herr_t H5VL_dset_split_file_close(void *file, hid_t dxpl_id, void **req);

/* Group callbacks */
static void * H5VL_dset_split_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                             hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id,
                                             void **req);
static void * H5VL_dset_split_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                           hid_t gapl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_group_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_group_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_group_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_group_close(void *grp, hid_t dxpl_id, void **req);

/* Link callbacks */
static herr_t H5VL_dset_split_link_create(H5VL_link_create_args_t *args, void *obj,
                                            const H5VL_loc_params_t *loc_params, hid_t lcpl_id, hid_t lapl_id,
                                            hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                          const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                          hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                                          const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id,
                                          hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_link_get(void *obj, const H5VL_loc_params_t *loc_params,
                                         H5VL_link_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_link_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_link_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args,
                                hid_t dxpl_id, void **req);


/* Object callbacks */
static void * H5VL_dset_split_object_open(void *obj, const H5VL_loc_params_t *loc_params,
                                            H5I_type_t *opened_type, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params,
                                            const char *src_name, void *dst_obj,
                                            const H5VL_loc_params_t *dst_loc_params, const char *dst_name,
                                            hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_object_get(void *obj, const H5VL_loc_params_t *loc_params,
                                           H5VL_object_get_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                                H5VL_object_specific_args_t *args, hid_t dxpl_id, void **req);
static herr_t H5VL_dset_split_object_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args,
                                  hid_t dxpl_id, void **req);

/* Container/connector introspection callbacks */
static herr_t H5VL_dset_split_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl,
                                                        const H5VL_class_t **conn_cls);
static herr_t H5VL_dset_split_introspect_get_cap_flags(const void *info, unsigned *cap_flags);
static herr_t H5VL_dset_split_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type,
                                                     uint64_t *flags);

/* Async request callbacks */
static herr_t H5VL_dset_split_request_wait(void *req, uint64_t timeout, H5VL_request_status_t *status);
static herr_t H5VL_dset_split_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx);
static herr_t H5VL_dset_split_request_cancel(void *req, H5VL_request_status_t *status);
static herr_t H5VL_dset_split_request_specific(void *req, H5VL_request_specific_args_t *args);
static herr_t H5VL_dset_split_request_optional(void *obj, H5VL_optional_args_t *args);
static herr_t H5VL_dset_split_request_free(void *req);

/* Blob callbacks */
static herr_t H5VL_dset_split_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx);
static herr_t H5VL_dset_split_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx);
static herr_t H5VL_dset_split_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t *args);
static herr_t H5VL_dset_split_blob_optional(void *obj, void *blob_id, H5VL_optional_args_t *args);

/* Token callbacks */
static herr_t H5VL_dset_split_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2,
                                          int *cmp_value);
static herr_t H5VL_dset_split_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token,
                                             char **token_str);
static herr_t H5VL_dset_split_token_from_str(void *obj, H5I_type_t obj_type, const char *token_str,
                                               H5O_token_t *token);

/* Generic optional callback */
static herr_t H5VL_dset_split_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);

/*******************/
/* Local variables */
/*******************/

/* Split file  VOL connector class struct */
static const H5VL_class_t H5VL_dset_split_g = {
    H5VL_VERSION,                            /* VOL class struct version */
    (H5VL_class_value_t)H5VL_DSET_SPLIT_VALUE, /* value        */
    H5VL_DSET_SPLIT_NAME,                      /* name         */
    H5VL_DSET_SPLIT_VERSION,                   /* connector version */
    0,                                       /* capability flags */
    H5VL_dset_split_init,                  /* initialize   */
    H5VL_dset_split_term,                  /* terminate    */
    {
        /* info_cls */
        sizeof(H5VL_dset_split_info_t), /* size    */
        H5VL_dset_split_info_copy,      /* copy    */
        H5VL_dset_split_info_cmp,       /* compare */
        H5VL_dset_split_info_free,      /* free    */
        H5VL_dset_split_info_to_str,    /* to_str  */
        H5VL_dset_split_str_to_info     /* from_str */
    },
    {
        /* wrap_cls */
        H5VL_dset_split_get_object,    /* get_object   */
        H5VL_dset_split_get_wrap_ctx,  /* get_wrap_ctx */
        H5VL_dset_split_wrap_object,   /* wrap_object  */
        H5VL_dset_split_unwrap_object, /* unwrap_object */
        H5VL_dset_split_free_wrap_ctx  /* free_wrap_ctx */
    },
    {
        /* attribute_cls */
        H5VL_dset_split_attr_create,   /* create */
        H5VL_dset_split_attr_open,     /* open */
        H5VL_dset_split_attr_read,     /* read */
        H5VL_dset_split_attr_write,    /* write */
        H5VL_dset_split_attr_get,      /* get */
        H5VL_dset_split_attr_specific, /* specific */
        H5VL_dset_split_attr_optional, /* optional */
        H5VL_dset_split_attr_close     /* close */
    },
    {
        /* dataset_cls */
        H5VL_dset_split_dataset_create,   /* create */
        H5VL_dset_split_dataset_open,     /* open */
        H5VL_dset_split_dataset_read,     /* read */
        H5VL_dset_split_dataset_write,    /* write */
        H5VL_dset_split_dataset_get,      /* get */
        H5VL_dset_split_dataset_specific, /* specific */
        H5VL_dset_split_dataset_optional, /* optional */
        H5VL_dset_split_dataset_close     /* close */
    },
    {
        /* datatype_cls */
        H5VL_dset_split_datatype_commit,   /* commit */
        H5VL_dset_split_datatype_open,     /* open */
        H5VL_dset_split_datatype_get,      /* get_size */
        H5VL_dset_split_datatype_specific, /* specific */
        H5VL_dset_split_datatype_optional, /* optional */
        H5VL_dset_split_datatype_close     /* close */
    },
    {
        /* file_cls */
        H5VL_dset_split_file_create,   /* create */
        H5VL_dset_split_file_open,     /* open */
        H5VL_dset_split_file_get,      /* get */
        H5VL_dset_split_file_specific, /* specific */
        H5VL_dset_split_file_optional, /* optional */
        H5VL_dset_split_file_close     /* close */
    },
    {
        /* group_cls */
        H5VL_dset_split_group_create,   /* create */
        H5VL_dset_split_group_open,     /* open */
        H5VL_dset_split_group_get,      /* get */
        H5VL_dset_split_group_specific, /* specific */
        H5VL_dset_split_group_optional, /* optional */
        H5VL_dset_split_group_close     /* close */
    },
    {
        /* link_cls */
        H5VL_dset_split_link_create,   /* create */
        H5VL_dset_split_link_copy,     /* copy */
        H5VL_dset_split_link_move,     /* move */
        H5VL_dset_split_link_get,      /* get */
        H5VL_dset_split_link_specific, /* specific */
        H5VL_dset_split_link_optional  /* optional */
    },
    {
        /* object_cls */
        H5VL_dset_split_object_open,     /* open */
        H5VL_dset_split_object_copy,     /* copy */
        H5VL_dset_split_object_get,      /* get */
        H5VL_dset_split_object_specific, /* specific */
        H5VL_dset_split_object_optional  /* optional */
    },
    {
        /* introspect_cls */
        H5VL_dset_split_introspect_get_conn_cls, /* get_conn_cls */
	H5VL_dset_split_introspect_get_cap_flags, /* get_cap_flags */
        H5VL_dset_split_introspect_opt_query,    /* opt_query */
    },
    {
        /* request_cls */
        H5VL_dset_split_request_wait,     /* wait */
        H5VL_dset_split_request_notify,   /* notify */
        H5VL_dset_split_request_cancel,   /* cancel */
        H5VL_dset_split_request_specific, /* specific */
        H5VL_dset_split_request_optional, /* optional */
        H5VL_dset_split_request_free      /* free */
    },
    {
        /* blob_cls */
        H5VL_dset_split_blob_put,      /* put */
        H5VL_dset_split_blob_get,      /* get */
        H5VL_dset_split_blob_specific, /* specific */
        H5VL_dset_split_blob_optional  /* optional */
    },
    {
        /* token_cls */
        H5VL_dset_split_token_cmp,     /* cmp */
        H5VL_dset_split_token_to_str,  /* to_str */
        H5VL_dset_split_token_from_str /* from_str */
    },
    H5VL_dset_split_optional /* optional */
};

H5PL_type_t H5PLget_plugin_type(void) {return H5PL_TYPE_VOL;}
const void *H5PLget_plugin_info(void) {return &H5VL_dset_split_g;}


/* The connector identification number, initialized at runtime */
static hid_t H5VL_DSET_SPLIT_g = H5I_INVALID_HID;

hid_t H5VL_ERR_STACK_g = H5I_INVALID_HID;
hid_t H5VL_ERR_CLS_g = H5I_INVALID_HID;

/****Helper Functions*****/

/*-------------------------------------------------------------------------
 * Function:   dset_split_extlink_create
 *
 * Purpose:     Helper funtion to create the external link 
 *
 * Return:      Success:     0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */

static herr_t
dset_split_extlink_create(char* file_name,char* dsetname, const char* obj_name, const H5VL_loc_params_t *loc_params, void *vol_obj,
                                            hid_t connector_id, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void* req)
{
     herr_t ret_value;
     size_t file_name_len;
     size_t obj_name_len;
     size_t buf_size;
     void* ext_link_buf = NULL;
     uint8_t    *p;
     H5VL_loc_params_t link_loc_params;
     H5VL_link_create_args_t link_args;

     file_name_len = strlen(file_name) + 1;
     obj_name_len = strlen("/") + 1 + strlen(dsetname) + 1;
     buf_size = 1 + file_name_len + obj_name_len;
     ext_link_buf = calloc(buf_size, sizeof(char));

     /* Encode the external link information */
     p = (uint8_t *)ext_link_buf;
     *p++ = (H5L_EXT_VERSION << 4) | H5L_EXT_FLAGS_ALL;/* External link version & flags */

     strncpy((char *)p, file_name, buf_size-1); /* Name of file containing external link's object */
     p += strlen(file_name) + 1;

     strcpy((char *)p, "/");
     p += strlen("/");

     strncpy((char *)p, dsetname, strlen(dsetname));/* External link's object */

     link_loc_params.type                         = H5VL_OBJECT_BY_NAME;
     link_loc_params.loc_data.loc_by_name.name    = obj_name;
     link_loc_params.loc_data.loc_by_name.lapl_id = lapl_id;
     link_loc_params.obj_type                     = loc_params->obj_type;

     link_args.op_type = H5VL_LINK_CREATE_UD;
     link_args.args.ud.type = H5L_TYPE_EXTERNAL;
     link_args.args.ud.buf = ext_link_buf;
     link_args.args.ud.buf_size = buf_size;

     ret_value = H5VLlink_create(&link_args, vol_obj, &link_loc_params, connector_id, lcpl_id, lapl_id, dxpl_id, req);

#ifdef DEBUG
     if (ret_value < 0)
        printf("Link creation failed\n");
#endif
     if (ext_link_buf)
        free(ext_link_buf);
     return ret_value;
}

/*-------------------------------------------------------------------------
 * Function:    get_dataset_name
 *
 * Purpose:     Helper funtion to get dataset name
 *              Extracts the last token '/'
 *
 * Return:      Success:    return last token
 *              Failure:    
 *
 *-------------------------------------------------------------------------
 */

char*
get_dataset_name(char* path)
{
    char* p = strtok(path, "/");
    char* temp = p;
    while (p != NULL)
    {
        temp = p;
        p = strtok(NULL, "/");
    }
    return temp;
}

/*-------------------------------------------------------------------------
 * Function:    get_parent_file_fapl
 *
 * Purpose:     Helper funtion to get parent file access property list
 *
 * Return:      Success:    return property id
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */

hid_t
get_parent_file_fapl(void* file_obj, hid_t connector_id)
{
    H5VL_file_get_args_t vol_cb_args;
    hid_t ret_value = H5I_INVALID_HID;
    FUNC_ENTER_VOL( hid_t, H5I_INVALID_HID)

    vol_cb_args.op_type               = H5VL_FILE_GET_FAPL;
    vol_cb_args.args.get_fapl.fapl_id = H5I_INVALID_HID;

    if (H5VLfile_get(file_obj, connector_id,  &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't get file access property list");

    ret_value = vol_cb_args.args.get_fapl.fapl_id;
    FUNC_RETURN_SET(ret_value);
    done:
        FUNC_LEAVE_VOL
}

/*-------------------------------------------------------------------------
 * Function:    get_parent_file_fcpl
 *
 * Purpose:     Helper funtion to get parent file creation property list
 *              
 *
 * Return:      Success:    return property id
 *              Failure:    H5I_INVALID_HID
 *
 *-------------------------------------------------------------------------
 */

hid_t
get_parent_file_fcpl(void* file_obj,  hid_t connector_id)
{
    hid_t ret_value = H5I_INVALID_HID;
    H5VL_file_get_args_t vol_cb_args;
    FUNC_ENTER_VOL( hid_t, H5I_INVALID_HID)

    vol_cb_args.op_type               = H5VL_FILE_GET_FCPL;
    vol_cb_args.args.get_fcpl.fcpl_id = H5I_INVALID_HID;

    if (H5VLfile_get(file_obj, connector_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't get file create property list");

    ret_value = vol_cb_args.args.get_fcpl.fcpl_id;
    FUNC_RETURN_SET(ret_value);

    done:
        FUNC_LEAVE_VOL
}

/*-------------------------------------------------------------------------
 * Function:    get_file_name
 *
 * Purpose:     Helper funtion to get file name
 *              
 *
 * Return:      Success:    Filename length
 *              Failure:    -1
 *-------------------------------------------------------------------------
 */

size_t
get_file_name(void* obj, hid_t connector_id, H5I_type_t type, char* name, size_t size)	
{
    H5VL_file_get_args_t vol_cb_args;	
    size_t               file_name_len = 0;  /* Length of file name */
    size_t              ret_value     = -1; /* Return value */
	
    FUNC_ENTER_VOL( size_t, -1)
    vol_cb_args.op_type                     = H5VL_FILE_GET_NAME;	
    vol_cb_args.args.get_name.type          = type;
    vol_cb_args.args.get_name.buf_size      = size;
    vol_cb_args.args.get_name.buf           = name;
    vol_cb_args.args.get_name.file_name_len = &file_name_len;

    if (H5VLfile_get(obj, connector_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, (-1), "unable to get file name");

    /* Set the return value */
    ret_value = (size_t)file_name_len;

    FUNC_RETURN_SET(ret_value);

    done:
        FUNC_LEAVE_VOL
}

/*-------------------------------------------------------------------------
 * Function:    dset_split_file_create
 *
 * Purpose:     Helper funtion to create file
 *              
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */

hid_t 
dset_split_file_create(const char* name, void* obj, H5I_type_t obj_type, hid_t connector_id)
{
    hid_t  file_id = -1;
    void * vol_obj_file = NULL;
    H5VL_object_get_args_t vol_cb_args;
    H5VL_loc_params_t      loc_params;
    FUNC_ENTER_VOL( hid_t, H5I_INVALID_HID)

    loc_params.type     = H5VL_OBJECT_BY_SELF;
    loc_params.obj_type = obj_type;

    vol_cb_args.op_type            = H5VL_OBJECT_GET_FILE;
    vol_cb_args.args.get_file.file = &vol_obj_file;

    if (H5VLobject_get(obj, &loc_params, connector_id, &vol_cb_args, H5P_DATASET_XFER_DEFAULT, NULL) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_CANTGET, H5I_INVALID_HID, "can't retrieve file from object");

    hid_t pfcpl_id = get_parent_file_fcpl(vol_obj_file, connector_id);
    hid_t fcpl_id = H5Pcopy(pfcpl_id);
    hid_t pfapl_id = get_parent_file_fapl(vol_obj_file, connector_id);
    hid_t fapl_id = H5Pcopy(pfapl_id);

    file_id = H5Fcreate(name, H5F_ACC_TRUNC, fcpl_id, fapl_id);
	
    H5Pclose(fapl_id);
    H5Pclose(pfapl_id);
    H5Pclose(fcpl_id);
    H5Pclose(pfcpl_id);

#ifdef DEBUG
    if( file_id < 0 )
        printf("Failed to create the splite file %s\n", name);
#endif
    FUNC_RETURN_SET(file_id);

    done:
        FUNC_LEAVE_VOL
}

/*-------------------------------------------------------------------------
 * Function:    dset_split_create_attribute
 *
 * Purpose:     Creates a attribute named 'split_file' to identify the 
 *              file as split file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */

herr_t 
dset_split_create_attribute(hid_t file_id)
{
    int value = 1;
    hid_t intType = H5Tcopy(H5T_NATIVE_INT32);
    hid_t valueSpace= H5Screate(H5S_SCALAR);
    hid_t attr= H5Acreate(file_id, ATTRIBUTE_NAME, intType, valueSpace,
                                            H5P_DEFAULT, H5P_DEFAULT);
    herr_t status= H5Awrite(attr, intType, &value);
    H5Sclose(valueSpace);
    H5Aclose(attr);
    return status;
}

/*-------------------------------------------------------------------------
 * Function:    H5VL__dset_split_new_obj
 *
 * Purpose:     Create a new dset_split object for an underlying object
 *
 * Return:      Success:    Pointer to the new dset_split object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5VL_dset_split_t *
H5VL_dset_split_new_dataset_obj(void *under_obj, hid_t under_vol_id, hid_t fid, H5I_type_t type)
{
    H5VL_dset_split_t *new_obj;

    new_obj               = (H5VL_dset_split_t *)calloc(1, sizeof(H5VL_dset_split_t));
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    new_obj->fid          = fid;
    new_obj->type         = type;
    new_obj->set          = 1;
    H5Iinc_ref(new_obj->under_vol_id);

    return new_obj;
} /* end H5VL__dset_split_new_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__dset_split_new_obj
 *
 * Purpose:     Create a new dset_split object for an underlying object
 *
 * Return:      Success:    Pointer to the new dset_split object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static H5VL_dset_split_t *
H5VL_dset_split_new_obj(void *under_obj, hid_t under_vol_id)
{
    H5VL_dset_split_t *new_obj;

    new_obj               = (H5VL_dset_split_t *)calloc(1, sizeof(H5VL_dset_split_t));
    new_obj->under_object = under_obj;
    new_obj->under_vol_id = under_vol_id;
    H5Iinc_ref(new_obj->under_vol_id);

    return new_obj;
} /* end H5VL__dset_split_new_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL__dset_split_free_obj
 *
 * Purpose:     Release a dset_split object
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_free_obj(H5VL_dset_split_t *obj)
{
    hid_t err_id;

    err_id = H5Eget_current_stack();

    H5Idec_ref(obj->under_vol_id);

    H5Eset_current_stack(err_id);

    free(obj);

    return 0;
} /* end H5VL__dset_split_free_obj() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_register
 *
 * Purpose:     Register the dset_split VOL connector and retrieve an ID
 *              for it.
 *
 * Return:      Success:    The ID for the dset_split VOL connector
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
hid_t
H5VL_dset_split_register(void)
{
    /* Singleton register the dset_split VOL connector ID */
    if (H5VL_DSET_SPLIT_g < 0)
        H5VL_DSET_SPLIT_g = H5VLregister_connector(&H5VL_dset_split_g, H5P_DEFAULT);

    return H5VL_DSET_SPLIT_g;
} /* end H5VL_dset_split_register() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_init
 *
 * Purpose:     Initialize this VOL connector, performing any necessary
 *              operations for the connector that will apply to all containers
 *              accessed with the connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_init(hid_t vipl_id)
{
#ifdef DEBUG
    printf("DSET-SPLIT VOL INIT\n");
#endif

    /* Shut compiler up about unused parameter */
    (void)vipl_id;

    return 0;
} /* end H5VL_dset_split_init() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_term
 *
 * Purpose:     Terminate this VOL connector, performing any necessary
 *              operations for the connector that release connector-wide
 *              resources (usually created / initialized with the 'init'
 *              callback).
 *
 * Return:      Success:    0
 *              Failure:    (Can't fail)
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_term(void)
{
#ifdef DEBUG
    printf("DSET-SPLIT VOL TERM\n");
#endif

    /* Reset VOL ID */
    H5VL_DSET_SPLIT_g = H5I_INVALID_HID;

    return 0;
} /* end H5VL_dset_split_term() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_info_copy
 *
 * Purpose:     Duplicate the connector's info object.
 *
 * Returns:     Success:    New connector info object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_info_copy(const void *_info)
{
    const H5VL_dset_split_info_t *info = (const H5VL_dset_split_info_t *)_info;
    H5VL_dset_split_info_t *      new_info;

#ifdef DEBUG
    printf("DSET-SPLIT VOL INFO Copy\n");
#endif

    /* Allocate new VOL info struct for the dset_split connector */
    new_info = (H5VL_dset_split_info_t *)calloc(1, sizeof(H5VL_dset_split_info_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_info->under_vol_id = info->under_vol_id;
    H5Iinc_ref(new_info->under_vol_id);
    if (info->under_vol_info)
        H5VLcopy_connector_info(new_info->under_vol_id, &(new_info->under_vol_info), info->under_vol_info);

    return new_info;
} /* end H5VL_dset_split_info_copy() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_info_cmp
 *
 * Purpose:     Compare two of the connector's info objects, setting *cmp_value,
 *              following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_info_cmp(int *cmp_value, const void *_info1, const void *_info2)
{
    const H5VL_dset_split_info_t *info1 = (const H5VL_dset_split_info_t *)_info1;
    const H5VL_dset_split_info_t *info2 = (const H5VL_dset_split_info_t *)_info2;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL INFO Compare\n");
#endif

    /* Sanity checks */
    assert(info1);
    assert(info2);

    /* Initialize comparison value */
    *cmp_value = 0;

    /* Compare under VOL connector classes */
    H5VLcmp_connector_cls(cmp_value, info1->under_vol_id, info2->under_vol_id);
    if (*cmp_value != 0)
        return 0;

    /* Compare under VOL connector info objects */
    H5VLcmp_connector_info(cmp_value, info1->under_vol_id, info1->under_vol_info, info2->under_vol_info);
    if (*cmp_value != 0)
        return 0;

    return 0;
} /* end H5VL_dset_split_info_cmp() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_info_free
 *
 * Purpose:     Release an info object for the connector.
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_info_free(void *_info)
{
    H5VL_dset_split_info_t *info = (H5VL_dset_split_info_t *)_info;
    hid_t                     err_id;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL INFO Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and info */
    if (info->under_vol_info)
        H5VLfree_connector_info(info->under_vol_id, info->under_vol_info);
    H5Idec_ref(info->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free dset_split info object itself */
    free(info);

    return 0;
} /* end H5VL_dset_split_info_free() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_info_to_str
 *
 * Purpose:     Serialize an info object for this connector into a string
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_info_to_str(const void *_info, char **str)
{
    const H5VL_dset_split_info_t *info              = (const H5VL_dset_split_info_t *)_info;
    H5VL_class_value_t              under_value       = (H5VL_class_value_t)-1;
    char *                          under_vol_string  = NULL;
    size_t                          under_vol_str_len = 0;

#ifdef DEBUG
    printf("DSET-SPLIT VOL INFO To String\n");
#endif

    /* Get value and string for underlying VOL connector */
    H5VLget_value(info->under_vol_id, &under_value);
    H5VLconnector_info_to_str(info->under_vol_info, info->under_vol_id, &under_vol_string);

    /* Determine length of underlying VOL info string */
    if (under_vol_string)
        under_vol_str_len = strlen(under_vol_string);

    /* Allocate space for our info */
    *str = (char *)H5allocate_memory(32 + under_vol_str_len, (hbool_t)0);
    assert(*str);

    /* Encode our info
     * Normally we'd use snprintf() here for a little extra safety, but that
     * call had problems on Windows until recently. So, to be as platform-independent
     * as we can, we're using sprintf() instead.
     */
    sprintf(*str, "under_vol=%u;under_info={%s}", (unsigned)under_value,
            (under_vol_string ? under_vol_string : ""));

    return 0;
} /* end H5VL_dset_split_info_to_str() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_str_to_info
 *
 * Purpose:     Deserialize a string into an info object for this connector.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_str_to_info(const char *str, void **_info)
{
    H5VL_dset_split_info_t *info;
    unsigned                  under_vol_value;
    const char *              under_vol_info_start, *under_vol_info_end;
    hid_t                     under_vol_id;
    void *                    under_vol_info = NULL;

#ifdef DEBUG
    printf("DSET-SPLIT VOL INFO String To Info\n");
#endif

    /* Retrieve the underlying VOL connector value and info */
    sscanf(str, "under_vol=%u;", &under_vol_value);
    under_vol_id         = H5VLregister_connector_by_value((H5VL_class_value_t)under_vol_value, H5P_DEFAULT);
    under_vol_info_start = strchr(str, '{');
    under_vol_info_end   = strrchr(str, '}');
    assert(under_vol_info_end > under_vol_info_start);
    if (under_vol_info_end != (under_vol_info_start + 1)) {
        char *under_vol_info_str;

        under_vol_info_str = (char *)malloc((size_t)(under_vol_info_end - under_vol_info_start));
        memcpy(under_vol_info_str, under_vol_info_start + 1,
               (size_t)((under_vol_info_end - under_vol_info_start) - 1));
        *(under_vol_info_str + (under_vol_info_end - under_vol_info_start)) = '\0';

        H5VLconnector_str_to_info(under_vol_info_str, under_vol_id, &under_vol_info);

        free(under_vol_info_str);
    } /* end else */

    /* Allocate new dset_split VOL connector info and set its fields */
    info                 = (H5VL_dset_split_info_t *)calloc(1, sizeof(H5VL_dset_split_info_t));
    info->under_vol_id   = under_vol_id;
    info->under_vol_info = under_vol_info;

    /* Set return value */
    *_info = info;
   
    return 0;
} /* end H5VL_dset_split_str_to_info() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_get_object
 *
 * Purpose:     Retrieve the 'data' for a VOL object.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_get_object(const void *obj)
{
    const H5VL_dset_split_t *o = (const H5VL_dset_split_t *)obj;

#ifdef DEBUG
    printf("DSET-SPLIT VOL Get object\n");
#endif

    return H5VLget_object(o->under_object, o->under_vol_id);
} /* end H5VL_dset_split_get_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_get_wrap_ctx
 *
 * Purpose:     Retrieve a wrapper context for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_get_wrap_ctx(const void *obj, void **wrap_ctx)
{
    const H5VL_dset_split_t *   o = (const H5VL_dset_split_t *)obj;
    H5VL_dset_split_wrap_ctx_t *new_wrap_ctx;

#ifdef DEBUG
    printf("DSET-SPLIT VOL WRAP CTX Get\n");
#endif

    /* Allocate new VOL object wrapping context for the dset_split connector */
    new_wrap_ctx = (H5VL_dset_split_wrap_ctx_t *)calloc(1, sizeof(H5VL_dset_split_wrap_ctx_t));

    /* Increment reference count on underlying VOL ID, and copy the VOL info */
    new_wrap_ctx->under_vol_id = o->under_vol_id;
    H5Iinc_ref(new_wrap_ctx->under_vol_id);
    H5VLget_wrap_ctx(o->under_object, o->under_vol_id, &new_wrap_ctx->under_wrap_ctx);

    /* Set wrap context to return */
    *wrap_ctx = new_wrap_ctx;

    return 0;
} /* end H5VL_dset_split_get_wrap_ctx() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_wrap_object
 *
 * Purpose:     Use a wrapper context to wrap a data object
 *
 * Return:      Success:    Pointer to wrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_wrap_object(void *obj, H5I_type_t obj_type, void *_wrap_ctx)
{
    H5VL_dset_split_wrap_ctx_t *wrap_ctx = (H5VL_dset_split_wrap_ctx_t *)_wrap_ctx;
    H5VL_dset_split_t *         new_obj;
    void *                        under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL WRAP Object\n");
#endif
    /* Wrap the object with the underlying VOL */
    under = H5VLwrap_object(obj, obj_type, wrap_ctx->under_vol_id, wrap_ctx->under_wrap_ctx);

    if (under)
        new_obj = H5VL_dset_split_new_obj(under, wrap_ctx->under_vol_id);
    else
        new_obj = NULL;

    return new_obj;
} /* end H5VL_dset_split_wrap_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_unwrap_object
 *
 * Purpose:     Unwrap a wrapped object, discarding the wrapper, but returning
 *		underlying object.
 *
 * Return:      Success:    Pointer to unwrapped object
 *              Failure:    NULL
 *
 *---------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_unwrap_object(void *obj)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL UNWRAP Object\n");
#endif

    /* Unrap the object with the underlying VOL */
    under = H5VLunwrap_object(o->under_object, o->under_vol_id);

    if (under)
        H5VL_dset_split_free_obj(o);

    return under;
} /* end H5VL_dset_split_unwrap_object() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_free_wrap_ctx
 *
 * Purpose:     Release a wrapper context for an object
 *
 * Note:	Take care to preserve the current HDF5 error stack
 *		when calling HDF5 API calls.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_free_wrap_ctx(void *_wrap_ctx)
{
    H5VL_dset_split_wrap_ctx_t *wrap_ctx = (H5VL_dset_split_wrap_ctx_t *)_wrap_ctx;
    hid_t                         err_id;

#ifdef DEBUG
    printf("DSET-SPLIT VOL WRAP CTX Free\n");
#endif

    err_id = H5Eget_current_stack();

    /* Release underlying VOL ID and wrap context */
    if (wrap_ctx->under_wrap_ctx)
        H5VLfree_wrap_ctx(wrap_ctx->under_wrap_ctx, wrap_ctx->under_vol_id);
    H5Idec_ref(wrap_ctx->under_vol_id);

    H5Eset_current_stack(err_id);

    /* Free dset_split wrap context object itself */
    free(wrap_ctx);

    return 0;
} /* end H5VL_dset_split_free_wrap_ctx() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_create
 *
 * Purpose:     Creates an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_attr_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t type_id,
                              hid_t space_id, hid_t acpl_id, hid_t aapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *attr;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Create\n");
#endif

    under = H5VLattr_create(o->under_object, loc_params, o->under_vol_id, name, type_id, space_id, acpl_id,
                            aapl_id, dxpl_id, req);
    if (under) {
        attr = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        attr = NULL;

    return (void *)attr;
} /* end H5VL_dset_split_attr_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_open
 *
 * Purpose:     Opens an attribute on an object.
 *
 * Return:      Success:    Pointer to attribute object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_attr_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t aapl_id,
                            hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *attr;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Open\n");
#endif

    under = H5VLattr_open(o->under_object, loc_params, o->under_vol_id, name, aapl_id, dxpl_id, req);
    if (under) {
        attr = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        attr = NULL;

    return (void *)attr;
} /* end H5VL_dset_split_attr_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_read
 *
 * Purpose:     Reads data from attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_attr_read(void *attr, hid_t mem_type_id, void *buf, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)attr;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Read\n");
#endif

    ret_value = H5VLattr_read(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_attr_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_write
 *
 * Purpose:     Writes data to attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_attr_write(void *attr, hid_t mem_type_id, const void *buf, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)attr;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Write\n");
#endif

    ret_value = H5VLattr_write(o->under_object, o->under_vol_id, mem_type_id, buf, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_attr_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_get
 *
 * Purpose:     Gets information about an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_attr_get(void *obj, H5VL_attr_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Get\n");
#endif

    ret_value = H5VLattr_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_attr_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_specific
 *
 * Purpose:     Specific operation on attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_attr_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                H5VL_attr_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Specific\n");
#endif

    ret_value = H5VLattr_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_attr_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_optional
 *
 * Purpose:     Perform a connector-specific operation on an attribute
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_attr_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Optional\n");
#endif

    ret_value = H5VLattr_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_attr_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_attr_close
 *
 * Purpose:     Closes an attribute.
 *
 * Return:      Success:    0
 *              Failure:    -1, attr not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_attr_close(void *attr, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)attr;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL ATTRIBUTE Close\n");
#endif

    ret_value = H5VLattr_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying attribute was closed */
    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_attr_close() */

/*-------------------------------------------------------------------------
 * Function:    dset_get_normalized_name
 *
 * Purpose:     Removes ".h5"  from the parent name
 *
 * Return:      Success:    Removes ".h5" from the parent file name
 *        
 *
 *-------------------------------------------------------------------------
 */
void
dset_get_normalized_name (char* name)
{
    char* temp = strstr(name, ".h5");
    if(temp)
    {
        *temp = '\0';
    }
}

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_create
 *
 * Purpose:     Creates a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_dataset_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                 hid_t lcpl_id, hid_t type_id, hid_t space_id, hid_t dcpl_id, hid_t dapl_id,
                                 hid_t dxpl_id, void **req)
{
    FUNC_ENTER_VOL(void*, NULL)
    H5VL_dset_split_t *dset;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *file_under;
    void *under;
    void *dset_under;
    char* dsetname = NULL;;
    hid_t file_id;
    char file_name[1000] = {'\0'};
    herr_t status;
    char* temp_path = NULL;
    char* parent_name = NULL;
    H5VL_loc_params_t file_loc_params;
    herr_t ret;
    size_t size;
	
#ifdef DEBUG
    printf("DSET-SPLIT VOL DATASET Create\n");
#endif

    /*Get the parent Name*/
    size = get_file_name(o->under_object, o->under_vol_id, loc_params->obj_type, NULL,  0);
    if(size > 0)
    {
        parent_name = (char*)calloc(size + 1, sizeof(char));
        get_file_name(o->under_object, o->under_vol_id, loc_params->obj_type, parent_name,  size+1);
        dset_get_normalized_name(parent_name);
    }
    else
    {
        parent_name = (char*)calloc(6, sizeof(char));
        strcpy(parent_name, "Split");
    }
            
    temp_path = (char*)calloc((strlen(name)+1), sizeof(char));
    if(!temp_path)
        HGOTO_ERROR(H5E_VOL, H5E_INTERNAL, NULL, "Memory allocation failed");

    strcpy(temp_path, name );

    /*Extract the dataset name. This is needed because name can also be an absolue path*/
    dsetname = get_dataset_name(temp_path);
    if(!dsetname)
        HGOTO_ERROR(H5E_VOL, H5E_INTERNAL, NULL, "Dataset name - get_dataset_name returned null value");

    sprintf(file_name , "%s-%s-%ld%s", parent_name, dsetname, time(NULL), FILE_EXTENTION);

    if((file_id = dset_split_file_create(file_name, o->under_object, loc_params->obj_type, o->under_vol_id)) < 0 )
        HGOTO_ERROR(H5E_VOL, H5E_INTERNAL, NULL, "Dataset Splitfile creation failed");

    file_under = H5VLobject(file_id);

    if((status = dset_split_create_attribute(file_id)) < 0 )
        HGOTO_ERROR(H5E_VOL, H5E_INTERNAL, NULL, "Attribute creation failed");

    file_loc_params.type = H5VL_OBJECT_BY_SELF;
    file_loc_params.obj_type = H5I_FILE;

    if(NULL == (dset_under = H5VLdataset_create(file_under, &file_loc_params, o->under_vol_id, dsetname, lcpl_id, type_id, space_id,
                             dcpl_id, dapl_id, dxpl_id, req)))
        HGOTO_ERROR(H5E_VOL, H5E_INTERNAL, NULL, "Dataset creation failed");

    if((ret = dset_split_extlink_create(file_name, dsetname, name, loc_params, o->under_object, o->under_vol_id, H5P_LINK_CREATE_DEFAULT, H5P_LINK_CREATE_DEFAULT, H5P_DATASET_XFER_DEFAULT, NULL)) < 0)
        HGOTO_ERROR(H5E_VOL, H5E_INTERNAL, NULL, "Link creation failed");

    under = dset_under;
    if(temp_path)
        free(temp_path);
    temp_path = NULL;
    if(parent_name)
        free(parent_name);
    parent_name = NULL;
    if (under)
    {
        dset = H5VL_dset_split_new_dataset_obj(under, o->under_vol_id, file_id, H5I_DATASET);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_dataset_obj(*req, o->under_vol_id, file_id, H5I_DATASET);
    } /* end if */
    else
        dset = NULL;
    
    FUNC_RETURN_SET(dset);

    done:
        if(temp_path)
            free(temp_path);

	if(parent_name)
            free(parent_name);

    FUNC_LEAVE_VOL
} /* end H5VL_dset_split_dataset_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_open
 *
 * Purpose:     Opens a dataset in a container
 *
 * Return:      Success:    Pointer to a dataset object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_dataset_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                               hid_t dapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *dset;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATASET Open\n");
#endif

    under = H5VLdataset_open(o->under_object, loc_params, o->under_vol_id, name, dapl_id, dxpl_id, req);
    if (under) {
        dset = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        dset = NULL;

    return (void *)dset;
} /* end H5VL_dset_split_dataset_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_read
 *
 * Purpose:     Reads data elements from a dataset into a buffer.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_dataset_read(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                               hid_t plist_id, void *buf, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)dset;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATASET Read\n");
#endif

    ret_value = H5VLdataset_read(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id,
                                 plist_id, buf, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_dataset_read() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_write
 *
 * Purpose:     Writes data elements from a buffer into a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_dataset_write(void *dset, hid_t mem_type_id, hid_t mem_space_id, hid_t file_space_id,
                                hid_t plist_id, const void *buf, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)dset;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATASET Write\n");
#endif

    ret_value = H5VLdataset_write(o->under_object, o->under_vol_id, mem_type_id, mem_space_id, file_space_id,
                                  plist_id, buf, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_dataset_write() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_get
 *
 * Purpose:     Gets information about a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_dataset_get(void *dset, H5VL_dataset_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)dset;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATASET Get\n");
#endif

    ret_value = H5VLdataset_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_dataset_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_specific
 *
 * Purpose:     Specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_dataset_specific(void *obj, H5VL_dataset_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    hid_t                under_vol_id;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL H5Dspecific\n");
#endif

    under_vol_id = o->under_vol_id;

    ret_value = H5VLdataset_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_dataset_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_optional
 *
 * Purpose:     Perform a connector-specific operation on a dataset
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_dataset_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATASET Optional\n");
#endif

    ret_value = H5VLdataset_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_dataset_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_dataset_close
 *
 * Purpose:     Closes a dataset.
 *
 * Return:      Success:    0
 *              Failure:    -1, dataset not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_dataset_close(void *dset, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)dset;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATASET Close\n");
#endif

    ret_value = H5VLdataset_close(o->under_object, o->under_vol_id, dxpl_id, req);

    if(o->set)
    {
       ret_value = H5Fclose(o->fid);
    }

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying dataset was closed */
    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_dataset_close() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_datatype_commit
 *
 * Purpose:     Commits a datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_datatype_commit(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                  hid_t type_id, hid_t lcpl_id, hid_t tcpl_id, hid_t tapl_id, hid_t dxpl_id,
                                  void **req)
{
    H5VL_dset_split_t *dt;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATATYPE Commit\n");
#endif

    under = H5VLdatatype_commit(o->under_object, loc_params, o->under_vol_id, name, type_id, lcpl_id, tcpl_id,
                                tapl_id, dxpl_id, req);
    if (under) {
        dt = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        dt = NULL;

    return (void *)dt;
} /* end H5VL_dset_split_datatype_commit() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_datatype_open
 *
 * Purpose:     Opens a named datatype inside a container.
 *
 * Return:      Success:    Pointer to datatype object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_datatype_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                                hid_t tapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *dt;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATATYPE Open\n");
#endif

    under = H5VLdatatype_open(o->under_object, loc_params, o->under_vol_id, name, tapl_id, dxpl_id, req);
    if (under) {
        dt = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        dt = NULL;

    return (void *)dt;
} /* end H5VL_dset_split_datatype_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_datatype_get
 *
 * Purpose:     Get information about a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_datatype_get(void *dt, H5VL_datatype_get_args_t *args, hid_t dxpl_id, void **req)
                               
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)dt;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATATYPE Get\n");
#endif

    ret_value = H5VLdatatype_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_datatype_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_datatype_specific
 *
 * Purpose:     Specific operations for datatypes
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_datatype_specific(void *obj, H5VL_datatype_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    hid_t                under_vol_id;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATATYPE Specific\n");
#endif

    under_vol_id = o->under_vol_id;

    ret_value = H5VLdatatype_specific(o->under_object, o->under_vol_id, args, dxpl_id, req);
               
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_datatype_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_datatype_optional
 *
 * Purpose:     Perform a connector-specific operation on a datatype
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_datatype_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATATYPE Optional\n");
#endif

    ret_value = H5VLdatatype_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_datatype_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_datatype_close
 *
 * Purpose:     Closes a datatype.
 *
 * Return:      Success:    0
 *              Failure:    -1, datatype not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_datatype_close(void *dt, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)dt;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL DATATYPE Close\n");
#endif

    assert(o->under_object);

    ret_value = H5VLdatatype_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying datatype was closed */
    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_datatype_close() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_file_create
 *
 * Purpose:     Creates a container using this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_file_create(const char *name, unsigned flags, hid_t fcpl_id, hid_t fapl_id, hid_t dxpl_id,
                              void **req)
{
    H5VL_dset_split_info_t *info;
    H5VL_dset_split_t *     file;
    hid_t                     under_fapl_id;
    void *                    under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL FILE Create\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Make sure we have info about the underlying VOL to be used */
    if (!info)
        return NULL;

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = H5VLfile_create(name, flags, fcpl_id, under_fapl_id, dxpl_id, req);
    if (under) {

        file = H5VL_dset_split_new_obj(under, info->under_vol_id);
        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, info->under_vol_id);
    } /* end if */
    else
        file = NULL;

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    H5VL_dset_split_info_free(info);
    return (void *)file;
} /* end H5VL_dset_split_file_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_file_open
 *
 * Purpose:     Opens a container created with this connector
 *
 * Return:      Success:    Pointer to a file object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_file_open(const char *name, unsigned flags, hid_t fapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_info_t *info;
    H5VL_dset_split_t *     file;
    hid_t                     under_fapl_id;
    void *                    under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL FILE Open\n");
#endif

    /* Get copy of our VOL info from FAPL */
    H5Pget_vol_info(fapl_id, (void **)&info);

    /* Make sure we have info about the underlying VOL to be used */
    if (!info)
        return NULL;

    /* Copy the FAPL */
    under_fapl_id = H5Pcopy(fapl_id);

    /* Set the VOL ID and info for the underlying FAPL */
    H5Pset_vol(under_fapl_id, info->under_vol_id, info->under_vol_info);

    /* Open the file with the underlying VOL connector */
    under = H5VLfile_open(name, flags, under_fapl_id, dxpl_id, req);
    if (under) {
        file = H5VL_dset_split_new_obj(under, info->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, info->under_vol_id);
    } /* end if */
    else
        file = NULL;

    /* Close underlying FAPL */
    H5Pclose(under_fapl_id);

    /* Release copy of our VOL info */
    H5VL_dset_split_info_free(info);

    return (void *)file;
} /* end H5VL_dset_split_file_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_file_get
 *
 * Purpose:     Get info about a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_file_get(void *file, H5VL_file_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)file;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL FILE Get\n");
#endif

    ret_value = H5VLfile_get(o->under_object, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_file_get() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_file_specific
 *
 * Purpose:     Specific operation on file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_file_specific(void *file, H5VL_file_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o            = (H5VL_dset_split_t *)file;
    H5VL_dset_split_t *new_o;
    H5VL_file_specific_args_t  my_args;
    H5VL_file_specific_args_t *new_args;
    H5VL_dset_split_info_t * info;
    hid_t                      under_vol_id = -1;
    herr_t                     ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL FILE Specific\n");
#endif

    if (args->op_type == H5VL_FILE_IS_ACCESSIBLE) {
        /* Shallow copy the args */
        memcpy(&my_args, args, sizeof(my_args));

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(args->args.is_accessible.fapl_id, (void **)&info);

        /* Make sure we have info about the underlying VOL to be used */
        if (!info)
            return (-1);

        /* Keep the correct underlying VOL ID for later */
        under_vol_id = info->under_vol_id;

        /* Copy the FAPL */
        my_args.args.is_accessible.fapl_id = H5Pcopy(args->args.is_accessible.fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(my_args.args.is_accessible.fapl_id, info->under_vol_id, info->under_vol_info);

        /* Set argument pointer to new arguments */
        new_args = &my_args;

        /* Set object pointer for operation */
        new_o = NULL;
    } /* end else-if */
    else if (args->op_type == H5VL_FILE_DELETE) {
        /* Shallow copy the args */
        memcpy(&my_args, args, sizeof(my_args));

        /* Get copy of our VOL info from FAPL */
        H5Pget_vol_info(args->args.del.fapl_id, (void **)&info);

        /* Make sure we have info about the underlying VOL to be used */
        if (!info)
            return (-1);

        /* Keep the correct underlying VOL ID for later */
        under_vol_id = info->under_vol_id;

        /* Copy the FAPL */
        my_args.args.del.fapl_id = H5Pcopy(args->args.del.fapl_id);

        /* Set the VOL ID and info for the underlying FAPL */
        H5Pset_vol(my_args.args.del.fapl_id, info->under_vol_id, info->under_vol_info);

        /* Set argument pointer to new arguments */
        new_args = &my_args;

        /* Set object pointer for operation */
        new_o = NULL;
    } /* end else-if */
    else {
        /* Keep the correct underlying VOL ID for later */
        under_vol_id = o->under_vol_id;

        /* Set argument pointer to current arguments */
        new_args = args;

        /* Set object pointer for operation */
        new_o = o->under_object;
    } /* end else */

    ret_value = H5VLfile_specific(new_o, under_vol_id, new_args, dxpl_id, req);


    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    if (args->op_type == H5VL_FILE_IS_ACCESSIBLE) {
        /* Close underlying FAPL */
        H5Pclose(my_args.args.is_accessible.fapl_id);

        /* Release copy of our VOL info */
        H5VL_dset_split_info_free(info);
    } /* end else-if */
    else if (args->op_type == H5VL_FILE_DELETE) {
        /* Close underlying FAPL */
        H5Pclose(my_args.args.del.fapl_id);

        /* Release copy of our VOL info */
        H5VL_dset_split_info_free(info);
    } /* end else-if */
    else if (args->op_type == H5VL_FILE_REOPEN) {
        /* Wrap file struct pointer for 'reopen' operation, if we reopened one */
        if (ret_value >= 0 && *args->args.reopen.file)
            *args->args.reopen.file = H5VL_dset_split_new_obj(*args->args.reopen.file, under_vol_id);
    } /* end else */


    return ret_value;
} /* end H5VL_dset_split_file_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_file_optional
 *
 * Purpose:     Perform a connector-specific operation on a file
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_file_optional(void *file, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)file;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL File Optional\n");
#endif

    ret_value = H5VLfile_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_file_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_file_close
 *
 * Purpose:     Closes a file.
 *
 * Return:      Success:    0
 *              Failure:    -1, file not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_file_close(void *file, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)file;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL FILE Close\n");
#endif

    ret_value = H5VLfile_close(o->under_object, o->under_vol_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying file was closed */
    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_file_close() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_group_create
 *
 * Purpose:     Creates a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_group_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name,
                               hid_t lcpl_id, hid_t gcpl_id, hid_t gapl_id, hid_t dxpl_id, void **req)
{

    H5VL_dset_split_t *group;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *     under = NULL;

#ifdef DEBUG
    printf("DSET-SPLIT VOL GROUP Create : %s\n", name);
#endif

    under = H5VLgroup_create(o->under_object, loc_params, o->under_vol_id, name, lcpl_id, gcpl_id,  gapl_id, dxpl_id, req);

    if (under) {
        group = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        group = NULL;

    return group;
   
} /* end H5VL_dset_split_group_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_group_open
 *
 * Purpose:     Opens a group inside a container
 *
 * Return:      Success:    Pointer to a group object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_group_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id,
                             hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *group;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL GROUP Open\n");
#endif

    under = H5VLgroup_open(o->under_object, loc_params, o->under_vol_id, name, gapl_id, dxpl_id, req);
    if (under) {
        group = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        group = NULL;

    return (void *)group;
}
/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_group_get
 *
 * Purpose:     Get info about a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_group_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL GROUP Get\n");
#endif

    ret_value = H5VLgroup_get(o->under_object, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_group_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_group_specific
 *
 * Purpose:     Specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_group_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    hid_t                under_vol_id;
    herr_t               ret_value;

#ifdef ENABLE_PASSTHRU_LOGGING
    printf("DSET_SPLIT VOL GROUP Specific\n");
#endif

    /* Save copy of underlying VOL connector ID, in case of
     * 'refresh' operation destroying the current object
     */
    under_vol_id = o->under_vol_id;

    /* Unpack arguments to get at the child file pointer when mounting a file */
    if (args->op_type == H5VL_GROUP_MOUNT) {
        H5VL_group_specific_args_t vol_cb_args; /* New group specific arg struct */

        /* Set up new VOL callback arguments */
        vol_cb_args.op_type         = H5VL_GROUP_MOUNT;
        vol_cb_args.args.mount.name = args->args.mount.name;
        vol_cb_args.args.mount.child_file =
            ((H5VL_dset_split_t *)args->args.mount.child_file)->under_object;
        vol_cb_args.args.mount.fmpl_id = args->args.mount.fmpl_id;

        /* Re-issue 'group specific' call, using the unwrapped pieces */
        ret_value = H5VLgroup_specific(o->under_object, under_vol_id, &vol_cb_args, dxpl_id, req);
    } /* end if */
    else
        ret_value = H5VLgroup_specific(o->under_object, under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_group_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_group_optional
 *
 * Purpose:     Perform a connector-specific operation on a group
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_group_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL GROUP Optional\n");
#endif
    ret_value = H5VLgroup_optional(o->under_object, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_group_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_group_close
 *
 * Purpose:     Closes a group.
 *
 * Return:      Success:    0
 *              Failure:    -1, group not closed.
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_group_close(void *grp, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)grp;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL H5Gclose\n");
#endif
    {
        ret_value = H5VLgroup_close(o->under_object, o->under_vol_id, dxpl_id, req);
    }

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    /* Release our wrapper, if underlying file was closed */
    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_group_close() */



/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_link_create
 *
 * Purpose:     Creates a hard / soft / UD / external link.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_link_create(H5VL_link_create_args_t *args, void *obj, const H5VL_loc_params_t *loc_params,
                              hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o            = (H5VL_dset_split_t *)obj;
    hid_t                under_vol_id = -1;
    herr_t               ret_value = -1;

#ifdef DEBUG
    printf("DSET-SPLIT VOL LINK Create\n");
#endif
    /* Try to retrieve the under VOL id */
    if (o)
        under_vol_id = o->under_vol_id;

    /* Fix up the link target object for hard link creation */
    if (H5VL_LINK_CREATE_HARD == args->op_type) {
	        void *cur_obj = args->args.hard.curr_obj;

        /* If cur_obj is a non-NULL pointer, find its 'under object' and update the pointer */
        if (cur_obj) {
            /* Check if we still haven't set the under VOL ID */
            if (under_vol_id < 0)
                under_vol_id = ((H5VL_dset_split_t *)cur_obj)->under_vol_id;

            /* Update the object for the link target */
            args->args.hard.curr_obj = ((H5VL_dset_split_t *)cur_obj)->under_object;
        } /* end if */
    }     /* end if */

    ret_value = H5VLlink_create(args, (o ? o->under_object : NULL), loc_params, under_vol_id, lcpl_id,
                                lapl_id, dxpl_id, req);
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_link_create() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_link_copy
 *
 * Purpose:     Renames an object within an HDF5 container and copies it to a new
 *              group.  The original name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_link_copy(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                            const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id,
                            void **req)
{
    H5VL_dset_split_t *o_src        = (H5VL_dset_split_t *)src_obj;
    H5VL_dset_split_t *o_dst        = (H5VL_dset_split_t *)dst_obj;
    hid_t                under_vol_id = -1;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL LINK Copy\n");
#endif

    /* Retrieve the under VOL id */
    if (o_src)
        under_vol_id = o_src->under_vol_id;
    else if (o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value =
        H5VLlink_copy((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL),
                      loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_link_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_link_move
 *
 * Purpose:     Moves a link within an HDF5 file to a new group.  The original
 *              name SRC is unlinked from the group graph
 *              and then inserted with the new name DST (which can specify a
 *              new path for the object) as an atomic operation. The names
 *              are interpreted relative to SRC_LOC_ID and
 *              DST_LOC_ID, which are either file IDs or group ID.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_link_move(void *src_obj, const H5VL_loc_params_t *loc_params1, void *dst_obj,
                            const H5VL_loc_params_t *loc_params2, hid_t lcpl_id, hid_t lapl_id, hid_t dxpl_id,
                            void **req)
{
    H5VL_dset_split_t *o_src        = (H5VL_dset_split_t *)src_obj;
    H5VL_dset_split_t *o_dst        = (H5VL_dset_split_t *)dst_obj;
    hid_t                under_vol_id = -1;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL LINK Move\n");
#endif

    /* Retrieve the under VOL id */
    if (o_src)
        under_vol_id = o_src->under_vol_id;
    else if (o_dst)
        under_vol_id = o_dst->under_vol_id;
    assert(under_vol_id > 0);

    ret_value =
        H5VLlink_move((o_src ? o_src->under_object : NULL), loc_params1, (o_dst ? o_dst->under_object : NULL),
                      loc_params2, under_vol_id, lcpl_id, lapl_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_link_move() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_link_get
 *
 * Purpose:     Get info about a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_link_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_link_get_args_t *args,
                           hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL LINK Get\n");
#endif

    ret_value = H5VLlink_get(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_link_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_link_specific
 *
 * Purpose:     Specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_link_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                H5VL_link_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL LINK Specific\n");
#endif

    ret_value = H5VLlink_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_link_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_link_optional
 *
 * Purpose:     Perform a connector-specific operation on a link
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_link_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args,
                                hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL LINK Optional\n");
#endif

    ret_value = H5VLlink_optional(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_link_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_object_open
 *
 * Purpose:     Opens an object inside a container.
 *
 * Return:      Success:    Pointer to object
 *              Failure:    NULL
 *
 *-------------------------------------------------------------------------
 */
static void *
H5VL_dset_split_object_open(void *obj, const H5VL_loc_params_t *loc_params, H5I_type_t *opened_type,
                              hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *new_obj;
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    void *               under;

#ifdef DEBUG
    printf("DSET-SPLIT VOL OBJECT Open\n");
#endif

    under = H5VLobject_open(o->under_object, loc_params, o->under_vol_id, opened_type, dxpl_id, req);
    if (under) {
        new_obj = H5VL_dset_split_new_obj(under, o->under_vol_id);

        /* Check for async request */
        if (req && *req)
            *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);
    } /* end if */
    else
        new_obj = NULL;

    return (void *)new_obj;
} /* end H5VL_dset_split_object_open() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_object_copy
 *
 * Purpose:     Copies an object inside a container.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_object_copy(void *src_obj, const H5VL_loc_params_t *src_loc_params, const char *src_name,
                              void *dst_obj, const H5VL_loc_params_t *dst_loc_params, const char *dst_name,
                              hid_t ocpypl_id, hid_t lcpl_id, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o_src = (H5VL_dset_split_t *)src_obj;
    H5VL_dset_split_t *o_dst = (H5VL_dset_split_t *)dst_obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL OBJECT Copy\n");
#endif

    ret_value =
        H5VLobject_copy(o_src->under_object, src_loc_params, src_name, o_dst->under_object, dst_loc_params,
                        dst_name, o_src->under_vol_id, ocpypl_id, lcpl_id, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o_src->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_object_copy() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_object_get
 *
 * Purpose:     Get info about an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_object_get(void *obj, const H5VL_loc_params_t *loc_params, H5VL_object_get_args_t *args,
                             hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL OBJECT Get\n");
#endif

    ret_value = H5VLobject_get(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_object_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_object_specific
 *
 * Purpose:     Specific operation on an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_object_specific(void *obj, const H5VL_loc_params_t *loc_params,
                                  H5VL_object_specific_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    hid_t                under_vol_id;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL OBJECT Specific\n");
#endif

    under_vol_id = o->under_vol_id;

    ret_value = H5VLobject_specific(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);
    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_object_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_object_optional
 *
 * Purpose:     Perform a connector-specific operation for an object
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_object_optional(void *obj, const H5VL_loc_params_t *loc_params, H5VL_optional_args_t *args,
                                  hid_t dxpl_id, void **req)

{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL OBJECT Optional\n");
#endif

    ret_value = H5VLobject_optional(o->under_object, loc_params, o->under_vol_id, args, dxpl_id, req);

    /* Check for async request */
    if (req && *req)
        *req = H5VL_dset_split_new_obj(*req, o->under_vol_id);

    return ret_value;
} /* end H5VL_dset_split_object_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_introspect_get_conn_clss
 *
 * Purpose:     Query the connector class.
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_introspect_get_conn_cls(void *obj, H5VL_get_conn_lvl_t lvl, const H5VL_class_t **conn_cls)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL INTROSPECT GetConnCls\n");
#endif

    /* Check for querying this connector's class */
    if (H5VL_GET_CONN_LVL_CURR == lvl) {
        *conn_cls = &H5VL_dset_split_g;
        ret_value = 0;
    } /* end if */
    else
        ret_value = H5VLintrospect_get_conn_cls(o->under_object, o->under_vol_id, lvl, conn_cls);

    return ret_value;
} /* end H5VL_dset_split_introspect_get_conn_cls() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_introspect_opt_query
 *
 * Purpose:     Query if an optional operation is supported by this connector
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_introspect_opt_query(void *obj, H5VL_subclass_t cls, int opt_type, uint64_t *flags)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL INTROSPECT OptQuery\n");
#endif

    ret_value = H5VLintrospect_opt_query(o->under_object, o->under_vol_id, cls, opt_type, flags);

    return ret_value;
} /* end H5VL_dset_split_introspect_opt_query() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_introspect_get_cap_flags
 *
 * Purpose:     Query the capability flags for this connector and any
 *              underlying connector(s).
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_introspect_get_cap_flags(const void *_info, unsigned *cap_flags)
{
    const H5VL_dset_split_info_t *info = (const H5VL_dset_split_info_t *)_info;
    herr_t                          ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL INTROSPECT GetCapFlags\n");
#endif

    /* Invoke the query on the underlying VOL connector */
    ret_value = H5VLintrospect_get_cap_flags(info->under_vol_info, info->under_vol_id, cap_flags);

    /* Bitwise OR our capability flags in */
    if (ret_value >= 0)
        *cap_flags |= H5VL_dset_split_g.cap_flags;

    return ret_value;
} /* end H5VL_dset_split_introspect_get_cap_flags() */




/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_request_wait
 *
 * Purpose:     Wait (with a timeout) for an async operation to complete
 *
 * Note:        Releases the request if the operation has completed and the
 *              connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_request_wait(void *obj, uint64_t timeout, H5VL_request_status_t *status)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL REQUEST Wait\n");
#endif

    ret_value = H5VLrequest_wait(o->under_object, o->under_vol_id, timeout, status);

    if (ret_value >= 0 && *status != H5VL_REQUEST_STATUS_IN_PROGRESS)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_request_wait() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_request_notify
 *
 * Purpose:     Registers a user callback to be invoked when an asynchronous
 *              operation completes
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_request_notify(void *obj, H5VL_request_notify_t cb, void *ctx)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL REQUEST Notify\n");
#endif

    ret_value = H5VLrequest_notify(o->under_object, o->under_vol_id, cb, ctx);

    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_request_notify() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_request_cancel
 *
 * Purpose:     Cancels an asynchronous operation
 *
 * Note:        Releases the request, if connector callback succeeds
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_request_cancel(void *obj, H5VL_request_status_t *status)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL REQUEST Cancel\n");
#endif

    ret_value = H5VLrequest_cancel(o->under_object, o->under_vol_id, status);

    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_request_cancel() */


/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_request_specific
 *
 * Purpose:     Specific operation on a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_request_specific(void *obj, H5VL_request_specific_args_t *args)
{
     H5VL_dset_split_t *o         = (H5VL_dset_split_t *)obj;
#ifdef DEBUG 
    printf("DSET-SPLIT VOL REQUEST Specific\n");
#endif
    herr_t               ret_value = -1;


    ret_value = H5VLrequest_specific(o->under_object, o->under_vol_id, args);

    return ret_value;

} /* end H5VL_dset_split_request_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_request_optional
 *
 * Purpose:     Perform a connector-specific operation for a request
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_request_optional(void *obj, H5VL_optional_args_t *args)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL REQUEST Optional\n");
#endif

    ret_value = H5VLrequest_optional(o->under_object, o->under_vol_id, args);
    return ret_value;
} /* end H5VL_dset_split_request_optional() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_request_free
 *
 * Purpose:     Releases a request, allowing the operation to complete without
 *              application tracking
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *-------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_request_free(void *obj)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL REQUEST Free\n");
#endif

    ret_value = H5VLrequest_free(o->under_object, o->under_vol_id);

    if (ret_value >= 0)
        H5VL_dset_split_free_obj(o);

    return ret_value;
} /* end H5VL_dset_split_request_free() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_blob_put
 *
 * Purpose:     Handles the blob 'put' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_blob_put(void *obj, const void *buf, size_t size, void *blob_id, void *ctx)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL BLOB Put\n");
#endif

    ret_value = H5VLblob_put(o->under_object, o->under_vol_id, buf, size, blob_id, ctx);

    return ret_value;
} /* end H5VL_dset_split_blob_put() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_blob_get
 *
 * Purpose:     Handles the blob 'get' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_blob_get(void *obj, const void *blob_id, void *buf, size_t size, void *ctx)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL BLOB Get\n");
#endif

    ret_value = H5VLblob_get(o->under_object, o->under_vol_id, blob_id, buf, size, ctx);

    return ret_value;
} /* end H5VL_dset_split_blob_get() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_blob_specific
 *
 * Purpose:     Handles the blob 'specific' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_blob_specific(void *obj, void *blob_id, H5VL_blob_specific_args_t *args)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL BLOB Specific\n");
#endif

    ret_value = H5VLblob_specific(o->under_object, o->under_vol_id, blob_id, args);
    return ret_value;
} /* end H5VL_dset_split_blob_specific() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_blob_optional
 *
 * Purpose:     Handles the blob 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_blob_optional(void *obj, void *blob_id, H5VL_optional_args_t *args)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL BLOB Optional\n");
#endif

    ret_value = H5VLblob_optional(o->under_object, o->under_vol_id, blob_id, args);
    return ret_value;
} /* end H5VL_dset_split_blob_optional() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_token_cmp
 *
 * Purpose:     Compare two of the connector's object tokens, setting
 *              *cmp_value, following the same rules as strcmp().
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_token_cmp(void *obj, const H5O_token_t *token1, const H5O_token_t *token2, int *cmp_value)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL TOKEN Compare\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token1);
    assert(token2);
    assert(cmp_value);

    ret_value = H5VLtoken_cmp(o->under_object, o->under_vol_id, token1, token2, cmp_value);

    return ret_value;
} /* end H5VL_dset_split_token_cmp() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_token_to_str
 *
 * Purpose:     Serialize the connector's object token into a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_token_to_str(void *obj, H5I_type_t obj_type, const H5O_token_t *token, char **token_str)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL TOKEN To string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_to_str(o->under_object, obj_type, o->under_vol_id, token, token_str);

    return ret_value;
} /* end H5VL_dset_split_token_to_str() */

/*---------------------------------------------------------------------------
 * Function:    H5VL_dset_split_token_from_str
 *
 * Purpose:     Deserialize the connector's object token from a string.
 *
 * Return:      Success:    0
 *              Failure:    -1
 *
 *---------------------------------------------------------------------------
 */
static herr_t
H5VL_dset_split_token_from_str(void *obj, H5I_type_t obj_type, const char *token_str, H5O_token_t *token)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG
    printf("DSET-SPLIT VOL TOKEN From string\n");
#endif

    /* Sanity checks */
    assert(obj);
    assert(token);
    assert(token_str);

    ret_value = H5VLtoken_from_str(o->under_object, obj_type, o->under_vol_id, token_str, token);

    return ret_value;
} /* end H5VL_dset_split_token_from_str() */

/*-------------------------------------------------------------------------
 * Function:    H5VL_dset_split_optional
 *
 * Purpose:     Handles the generic 'optional' callback
 *
 * Return:      SUCCEED / FAIL
 *
 *-------------------------------------------------------------------------
 */
herr_t
H5VL_dset_split_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req)
{
    H5VL_dset_split_t *o = (H5VL_dset_split_t *)obj;
    herr_t               ret_value;

#ifdef DEBUG 
    printf("DSET-SPLIT VOL generic Optional\n");
#endif

    ret_value = H5VLoptional(o->under_object, o->under_vol_id, args, dxpl_id, req);

    return ret_value;
} /* end H5VL_dset_split_optional() */

