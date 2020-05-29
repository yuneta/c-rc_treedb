/***********************************************************************
 *          RC_TREEDB.C
 *
 *          Resource Driver for Treedb
 *
 *          Copyright (c) 2020 Niyamaka.
 *          All Rights Reserved.
***********************************************************************/
#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include "rc_treedb.h"

/***************************************************************
 *              Constants
 ***************************************************************/

/***************************************************************
 *              Structures
 ***************************************************************/

/***************************************************************
 *              DBA persistent functions
 ***************************************************************/
PRIVATE void * dba_open(
    hgobj gobj,
    const char *database,
    json_t *jn_properties   // owned
);
PRIVATE int dba_close(hgobj gobj, void *pDb);
PRIVATE int dba_create_table(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    const char *key,
    json_t *kw_fields // owned
);
PRIVATE int dba_drop_table(
    hgobj gobj,
    void *pDb,
    const char *tablename
);
PRIVATE uint64_t dba_create_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_record // owned
);

PRIVATE int dba_update_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro,  // owned
    json_t *kw_record   // owned
);

PRIVATE int dba_delete_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro // owned
);

PRIVATE json_t *dba_load_table(
    hgobj gobj,
    void *pDb,
    const char* tablename,
    const char* resource,
    void *user_data,    // To use as parameter in dba_record_cb() callback.
    json_t *kw_filtro,  // owned. Filter the records if you don't full table. See kw_record keys.
    dba_record_cb dba_filter,
    json_t *jn_record_list
);

/***************************************************************
 *              Prototypes
 ***************************************************************/


/***************************************************************
 *              Data
 ***************************************************************/

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE dba_persistent_t dba = {
    dba_open,
    dba_close,
    dba_create_table,
    dba_drop_table,
    dba_create_record,
    dba_update_record,
    dba_delete_record,
    dba_load_table
};
PUBLIC dba_persistent_t *dba_rc_treedb(void)
{
    return &dba;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE void * dba_open(
    hgobj gobj,
    const char *path_, // Viene el path completo incluyendo el nombre de la database
    json_t *jn_properties   // owned
)
{
    char path[NAME_MAX];
    snprintf(path, sizeof(path), "%s", path_);

    char *database = strrchr(path, '/');
    if(!database) {
        log_critical(LOG_OPT_EXIT_ZERO,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_INTERNAL_ERROR,
            "msg",          "%s", "path without /",
            NULL
        );
    }
    *database = 0;
    database++;

    json_t *jn_tranger = json_pack("{s:s, s:s, s:s, s:b}",
        "path", path,
        "database", database,
        "filename_mask", "%Y",
        "master", 1
    );

    json_t *tranger = tranger_startup(
        jn_tranger // owned
    );

    if(!tranger) {
        log_critical(LOG_OPT_EXIT_ZERO,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_INTERNAL_ERROR,
            "msg",          "%s", "tranger_startup() FAILED",
            NULL
        );
    }

    json_t *jn_schema = kw_get_dict(jn_properties, "treedb_schema", 0, 0);
    JSON_INCREF(jn_schema);
    treedb_open_db( // Return IS NOT YOURS!
        tranger,
        database,
        jn_schema,  // owned
        "persistent"
    );

    JSON_DECREF(jn_properties);
    return tranger;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_close(hgobj gobj, void *pDb)
{
    json_t *treedbs = treedb_list_treedb(pDb);

    int idx; json_t *jn_treedb;
    json_array_foreach(treedbs, idx, jn_treedb) {
        treedb_close_db(pDb, json_string_value(jn_treedb));
    }
    JSON_DECREF(treedbs);
    EXEC_AND_RESET(tranger_shutdown, pDb);
    return 0;
}

/***************************************************************************
 *  HACK function idempotent!
 ***************************************************************************/
PRIVATE int dba_create_table(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    const char *key,    // primary key
    json_t *kw_fields_   // owned
)
{
    if(strcmp(key, "id")!=0 || !kw_has_key(kw_fields_, "id")) {
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_INTERNAL_ERROR,
            "msg",          "%s", "pkey must be 'id'",
            "tablename",    "%s", tablename,
            NULL
        );
        log_debug_json(0, kw_fields_, "pkey must be 'id'");
        KW_DECREF(kw_fields_);
        return -1;
    }

    json_t *kw_fields = kw_duplicate(kw_fields_);
    KW_DECREF(kw_fields_);
    json_t *cols = json_object();

    // HACK resource trabaja con integer keys, treedb con string keys
    if(kw_has_key(kw_fields, "id")) {
        json_object_set_new(kw_fields, "id", json_string(""));
    }

    const char *col_name;
    json_t *jn_value;
    json_object_foreach(kw_fields, col_name, jn_value) {
        if(json_is_string(jn_value)) {
            // "TEXT"
            if(strcmp(col_name, "id")==0) {
                json_t *col = json_pack("{s:s, s:s, s:[s,s,s]}",
                    "header", col_name,
                    "type", "string",
                    "flag", "persistent", "required","rowid"
                );
                json_object_set_new(cols, col_name, col);
            } else {
                json_t *col = json_pack("{s:s, s:s, s:[s,s]}",
                    "header", col_name,
                    "type", "string",
                    "flag", "persistent", "required"
                );
                json_object_set_new(cols, col_name, col);
            }

        } else if(json_is_integer(jn_value)) {
            // "INTEGER"
            json_t *col = json_pack("{s:s, s:s, s:[s]}",
                "header", col_name,
                "type", "integer",
                "flag", "persistent"
            );
            json_object_set_new(cols, col_name, col);

        } else if(json_is_real(jn_value)) {
            // "REAL"
            json_t *col = json_pack("{s:s, s:s, s:[s]}",
                "header", col_name,
                "type", "real",
                "flag", "persistent"
            );
            json_object_set_new(cols, col_name, col);

        } else if(json_is_boolean(jn_value)) {
            // "BOOLEAN"
            json_t *col = json_pack("{s:s, s:s, s:[s]}",
                "header", col_name,
                "type", "boolean",
                "flag", "persistent"
            );
            json_object_set_new(cols, col_name, col);

        } else {
            // "BLOB"
            json_t *col = json_pack("{s:s, s:s, s:[s]}",
                "header", col_name,
                "type", "blob",
                "flag", "persistent"
            );
            json_object_set_new(cols, col_name, col);
        }
    }

    treedb_create_topic(
        pDb,
        kw_get_str(pDb, "database", "", KW_REQUIRED), // treedb_name
        tablename, // topic_name
        "1",       // topic_version
        "",        // topic_options
        cols, // owned
        0,
        0
    );

    KW_DECREF(kw_fields);
    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_drop_table(
    hgobj gobj,
    void *pDb,
    const char *tablename
)
{
    return treedb_delete_topic(
        pDb,
        kw_get_str(pDb, "database", "", KW_REQUIRED), // treedb_name
        tablename // topic_name
    );
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE uint64_t dba_create_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_record_  // owned
)
{
    json_t *kw_record = kw_duplicate(kw_record_);
    KW_DECREF(kw_record_);

    // HACK resource trabaja con integer keys, treedb con string keys
    if(kw_has_key(kw_record, "id")) {
        json_int_t id = kw_get_int(kw_record, "id", 0, 0);
        if(id) {
            json_object_set_new(kw_record, "id", json_sprintf("%"JSON_INTEGER_FORMAT, id));
        } else {
            json_object_del(kw_record, "id");
        }
    }

    KW_INCREF(kw_record);
    json_t *record = treedb_update_node( // Return is NOT YOURS
        pDb,
        kw_get_str(pDb, "database", "", KW_REQUIRED), // treedb_name
        tablename, // topic_name
        kw_record, // owned
        "create" // options // "permissive"
    );
    if(!record) {
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_SERVICE_ERROR,
            "msg",          "%s", "treedb_create_node() FAILED",
            "tablename",    "%s", tablename,
            NULL
        );
        log_debug_json(0, kw_record, "treedb_create_node() FAILED");
    }

    uint64_t id = (uint64_t)kw_get_int(record, "id", 0, KW_WILD_NUMBER);
    if(!id) {
        id = kw_get_int(record, "__md_treedb__`__rowid__", 0, KW_REQUIRED);
    }

    KW_DECREF(kw_record);
    return id;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_update_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro_,  // owned
    json_t *kw_record_   // owned
)
{
    json_t *kw_filtro = kw_duplicate(kw_filtro_);
    KW_DECREF(kw_filtro_);

    json_t *kw_record = kw_duplicate(kw_record_);
    KW_DECREF(kw_record_);

    // HACK resource trabaja con integer keys, treedb con string keys
    if(kw_has_key(kw_record, "id")) {
        json_int_t id = kw_get_int(kw_record, "id", 0, 0);
        if(id) {
            json_object_set_new(kw_record, "id", json_sprintf("%"JSON_INTEGER_FORMAT, id));
        } else {
            json_object_del(kw_record, "id");
        }
    }

    KW_INCREF(kw_record);
    json_t *record = treedb_update_node( // Return is NOT YOURS
        pDb,
        kw_get_str(pDb, "database", "", KW_REQUIRED), // treedb_name
        tablename, // topic_name
        kw_record, // owned
        "" // options // "permissive"
    );
    if(!record) {
        log_error(0,
            "gobj",         "%s", gobj_full_name(gobj),
            "function",     "%s", __FUNCTION__,
            "msgset",       "%s", MSGSET_SERVICE_ERROR,
            "msg",          "%s", "treedb_create_node() FAILED",
            "tablename",    "%s", tablename,
            NULL
        );
        log_debug_json(0, kw_record, "treedb_create_node() FAILED");
    }

    KW_DECREF(kw_filtro);
    KW_DECREF(kw_record);
    return 0;
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE int dba_delete_record(
    hgobj gobj,
    void *pDb,
    const char *tablename,
    json_t *kw_filtro_ // owned
)
{
    json_t *kw_filtro = kw_duplicate(kw_filtro_);
    KW_DECREF(kw_filtro_);

    // HACK resource trabaja con integer keys, treedb con string keys
    if(kw_has_key(kw_filtro, "id")) {
        json_int_t id = kw_get_int(kw_filtro, "id", 0, 0);
        if(id) {
            json_object_set_new(kw_filtro, "id", json_sprintf("%"JSON_INTEGER_FORMAT, id));
        } else {
            json_object_del(kw_filtro, "id");
        }
    }

    return treedb_delete_node(
        pDb,
        kw_get_str(pDb, "database", "", KW_REQUIRED), // treedb_name
        tablename, // topic_name
        kw_filtro,    // owned
        "force"
    );
}

/***************************************************************************
 *
 ***************************************************************************/
PRIVATE json_t *dba_load_table(
    hgobj gobj,
    void *pDb,
    const char* tablename,
    const char* resource,
    void *user_data,    // To use as parameter in dba_record_cb() callback.
    json_t *kw_filtro_,  // owned. Filter the records if you don't full table. See kw_record keys.
    dba_record_cb dba_filter,
    json_t *jn_record_list
)
{
    json_t *kw_filtro = kw_duplicate(kw_filtro_);
    KW_DECREF(kw_filtro_);

    if(!jn_record_list) {
        jn_record_list = json_array();
    }

    // HACK resource trabaja con integer keys, treedb con string keys
    json_int_t id = kw_get_int(kw_filtro, "id", 0, 0);
    if(id) {
        json_object_set_new(kw_filtro, "id", json_sprintf("%"JSON_INTEGER_FORMAT, id));
    }

    json_t *records = treedb_list_nodes( // Return MUST be decref
        pDb,
        kw_get_str(pDb, "database", "", KW_REQUIRED), // treedb_name
        tablename,  // topic_name
        kw_filtro,  // jn_filter,  // owned
        0,          // jn_options, // owned "collapsed"
        0           // match_fn
    );

    int idx; json_t *kw_record;
    json_array_foreach(records, idx, kw_record) {
        json_t *record = kw_duplicate(kw_record);
        json_int_t id = kw_get_int(record, "id", 0, KW_REQUIRED|KW_WILD_NUMBER);
        json_object_set_new(record, "id", json_integer(id));
        JSON_INCREF(record);
        int ret = dba_filter(gobj, resource, user_data, record);
        // Return 1 append, 0 ignore, -1 break the load.
        if(ret < 0) {
            JSON_DECREF(record);
            break;
        } else if(ret==0) {
            JSON_DECREF(record);
            continue;
        }
        json_array_append_new(jn_record_list, record);
    }

    JSON_DECREF(records);

    return jn_record_list;
}
