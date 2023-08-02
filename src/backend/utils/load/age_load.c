/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "postgres.h"

#include "access/heapam.h"
#include "access/xact.h"
#include "parser/parse_node.h"
#include "storage/lockdefs.h"
#include "tcop/dest.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"

#include "catalog/ag_graph.h"
#include "catalog/ag_label.h"
#include "utils/agtype.h"
#include "utils/graphid.h"

#include "utils/load/ag_load_edges.h"
#include "utils/load/ag_load_labels.h"
#include "utils/load/age_load.h"

agtype *create_empty_agtype(void)
{
    agtype_in_state result;

    memset(&result, 0, sizeof(agtype_in_state));

    result.res = push_agtype_value(&result.parse_state, WAGT_BEGIN_OBJECT,
                                   NULL);
    result.res = push_agtype_value(&result.parse_state, WAGT_END_OBJECT, NULL);

    return agtype_value_to_agtype(result.res);
}

agtype *create_agtype_from_list(char **header, char **fields, size_t fields_len,
                                int64 vertex_id)
{
    agtype_in_state result;
    int i;

    memset(&result, 0, sizeof(agtype_in_state));

    result.res = push_agtype_value(&result.parse_state, WAGT_BEGIN_OBJECT,
                                   NULL);

    result.res = push_agtype_value(&result.parse_state,
                                   WAGT_KEY,
                                   string_to_agtype_value("__id__"));
    result.res = push_agtype_value(&result.parse_state,
                                   WAGT_VALUE,
                                   integer_to_agtype_value(vertex_id));

    for (i = 0; i<fields_len; i++)
    {
        result.res = push_agtype_value(&result.parse_state,
                                       WAGT_KEY,
                                       string_to_agtype_value(header[i]));
        result.res = push_agtype_value(&result.parse_state,
                                       WAGT_VALUE,
                                       string_to_agtype_value(fields[i]));
    }

    result.res = push_agtype_value(&result.parse_state,
                                   WAGT_END_OBJECT, NULL);

    return agtype_value_to_agtype(result.res);
}

agtype* create_agtype_from_list_i(char **header, char **fields,
                                  size_t fields_len, size_t start_index)
{

    agtype_in_state result;
    size_t i;

    if (start_index + 1 == fields_len)
    {
        return create_empty_agtype();
    }

    memset(&result, 0, sizeof(agtype_in_state));

    result.res = push_agtype_value(&result.parse_state, WAGT_BEGIN_OBJECT,
                                   NULL);

    for (i = start_index; i < fields_len; i++)
    {
        result.res = push_agtype_value(&result.parse_state,
                                       WAGT_KEY,
                                       string_to_agtype_value(header[i]));
        result.res = push_agtype_value(&result.parse_state,
                                       WAGT_VALUE,
                                       string_to_agtype_value(fields[i]));
    }

    result.res = push_agtype_value(&result.parse_state,
                                   WAGT_END_OBJECT, NULL);

    return agtype_value_to_agtype(result.res);
}

void insert_edge_simple(Oid graph_oid, char *label_name, graphid edge_id,
                        graphid start_id, graphid end_id,
                        agtype *edge_properties)
{

    Datum values[5];
    bool nulls[5] = {false, false, false, false, false};
    Relation label_relation;
    HeapTuple tuple;
    int32 label_id;

    // Check if label provided exists as vertex label, then throw error
    if (get_label_kind(label_name, graph_oid) == LABEL_KIND_VERTEX)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("label %s already exists as vertex label", label_name)));
    }

    label_id = get_label_id(label_name, graph_oid);

    values[0] = GRAPHID_GET_DATUM(edge_id);
    values[1] = GRAPHID_GET_DATUM(start_id);
    values[2] = GRAPHID_GET_DATUM(end_id);
    values[3] = AGTYPE_P_GET_DATUM((edge_properties));
    values[4] = Int32GetDatum(label_id);

    label_relation = table_open(get_label_relation(label_name, graph_oid),
                                RowExclusiveLock);

    tuple = heap_form_tuple(RelationGetDescr(label_relation),
                            values, nulls);
    heap_insert(label_relation, tuple,
                GetCurrentCommandId(true), 0, NULL);
    table_close(label_relation, RowExclusiveLock);
    CommandCounterIncrement();
}

void insert_vertex_simple(Oid graph_oid, char *label_name, graphid vertex_id,
                          agtype *vertex_properties)
{

    Datum values[3];
    bool nulls[3] = {false, false, false};
    Relation label_relation;
    HeapTuple tuple;
    int32 label_id;

    // Check if label provided exists as edge label, then throw error
    if (get_label_kind(label_name, graph_oid) == LABEL_KIND_EDGE)
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                        errmsg("label %s already exists as edge label", label_name)));
    }

    label_id = get_label_id(label_name, graph_oid);

    values[0] = GRAPHID_GET_DATUM(vertex_id);
    values[1] = AGTYPE_P_GET_DATUM((vertex_properties));
    values[2] = Int32GetDatum(label_id);

    label_relation = table_open(get_label_relation(label_name, graph_oid),
                                RowExclusiveLock);
    tuple = heap_form_tuple(RelationGetDescr(label_relation),
                            values, nulls);
    heap_insert(label_relation, tuple,
                GetCurrentCommandId(true), 0, NULL);
    table_close(label_relation, RowExclusiveLock);
    CommandCounterIncrement();
}

PG_FUNCTION_INFO_V1(load_labels_from_file);
Datum load_labels_from_file(PG_FUNCTION_ARGS)
{
    Name graph_name;
    Name label_name;
    text* file_path;
    char* graph_name_str;
    char* label_name_str;
    char* file_path_str;
    Oid graph_oid;
    int32 label_id;
    bool id_field_exists;

    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("graph name must not be NULL")));
    }

    if (PG_ARGISNULL(1))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("label name must not be NULL")));
    }

    if (PG_ARGISNULL(2))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("file path must not be NULL")));
    }

    graph_name = PG_GETARG_NAME(0);
    label_name = PG_GETARG_NAME(1);
    file_path = PG_GETARG_TEXT_P(2);
    id_field_exists = PG_GETARG_BOOL(3);


    graph_name_str = NameStr(*graph_name);
    label_name_str = NameStr(*label_name);
    file_path_str = text_to_cstring(file_path);

    graph_oid = get_graph_oid(graph_name_str);
    label_id = get_label_id(label_name_str, graph_oid);

    create_labels_from_csv_file(file_path_str, graph_name_str, graph_oid,
                                label_name_str, label_id, id_field_exists);
    PG_RETURN_VOID();

}

PG_FUNCTION_INFO_V1(load_edges_from_file);
Datum load_edges_from_file(PG_FUNCTION_ARGS)
{

    Name graph_name;
    Name label_name;
    text* file_path;
    char* graph_name_str;
    char* label_name_str;
    char* file_path_str;
    Oid graph_oid;
    int32 label_id;

    if (PG_ARGISNULL(0))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("graph name must not be NULL")));
    }

    if (PG_ARGISNULL(1))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("label name must not be NULL")));
    }

    if (PG_ARGISNULL(2))
    {
        ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                errmsg("file path must not be NULL")));
    }

    graph_name = PG_GETARG_NAME(0);
    label_name = PG_GETARG_NAME(1);
    file_path = PG_GETARG_TEXT_P(2);

    graph_name_str = NameStr(*graph_name);
    label_name_str = NameStr(*label_name);
    file_path_str = text_to_cstring(file_path);

    graph_oid = get_graph_oid(graph_name_str);
    label_id = get_label_id(label_name_str, graph_oid);

    create_edges_from_csv_file(file_path_str, graph_name_str, graph_oid,
                               label_name_str, label_id);
    PG_RETURN_VOID();

}
