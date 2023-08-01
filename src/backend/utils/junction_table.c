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

#include "access/xact.h"
#include "commands/tablecmds.h"
#include "nodes/makefuncs.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/lsyscache.h"
#include "parser/parse_relation.h"
#include "utils/rel.h"
#include "rewrite/rewriteHandler.h"
#include "access/table.h"

#include "commands/label_commands.h"
#include "utils/ag_cache.h"
#include "utils/agtype.h"
#include "utils/junction_table.h"
#include "catalog/ag_label.h"

#define gen_label_relation_name(label_name);

void create_junction_table(char *graph_name);
static void create_table_for_junc_table(char *graph_name, char *label_name,
					char *schema_name, char *rel_name);

static List
    *create_junc_table_table_elements(char *graph_name, char *label_name,
				      char *schema_name, char *rel_name);

int32 junction_table_label_id(const char *graph_name, Oid graph_oid,
			      graphid element_graphid);
Oid ag_junction_id(const char *graph_name, const char* table_name,
		   Oid graph_oid, char *table_kind);
static Constraint *build_fk_constraint(char *schema_name);
void init_junc_node(cypher_node *node);
cypher_target_node *
transform_junction_table_node(cypher_parsestate *cpstate, List **target_list,
			      cypher_node *node);

void create_junction_table(char *graph_name)
{
    graph_cache_data *cache_data;
    Oid nsp_id;
    char *schema_name;
    char *rel_name;
    char *table_name = AG_JUNCTION_TABLE;

    cache_data = search_graph_name_cache(graph_name);
    if (!cache_data)
    {
        ereport(ERROR, (errcode(ERRCODE_UNDEFINED_SCHEMA),
                        errmsg("graph \"%s\" does not exist", graph_name)));
    }
    nsp_id = cache_data->namespace;
    schema_name = get_namespace_name(nsp_id);
    rel_name = table_name;

    // create a table for the junction table
    create_table_for_junc_table(graph_name, table_name, schema_name, rel_name);


    CommandCounterIncrement();
};

static void create_table_for_junc_table(char *graph_name, char *table_name,
				    char *schema_name, char *rel_name)
{
    CreateStmt *create_stmt;
    PlannedStmt *wrapper;

    create_stmt = makeNode(CreateStmt);

    // relpersistence is set to RELPERSISTENCE_PERMANENT by makeRangeVar()
    create_stmt->relation = makeRangeVar(schema_name, rel_name, -1);

    create_stmt->tableElts =
        create_junc_table_table_elements(graph_name, table_name,
					 schema_name, rel_name);


    create_stmt->inhRelations = NIL;
    create_stmt->partbound = NULL;
    create_stmt->ofTypename = NULL;
    create_stmt->constraints = NIL;
    create_stmt->options = NIL;
    create_stmt->oncommit = ONCOMMIT_NOOP;
    create_stmt->tablespacename = NULL;
    create_stmt->if_not_exists = false;

    wrapper = makeNode(PlannedStmt);
    wrapper->commandType = CMD_UTILITY;
    wrapper->canSetTag = false;
    wrapper->utilityStmt = (Node *)create_stmt;
    wrapper->stmt_location = -1;
    wrapper->stmt_len = 0;

    ProcessUtility(wrapper, "(generated CREATE TABLE command)",
                   PROCESS_UTILITY_SUBCOMMAND, NULL, NULL, None_Receiver,
                   NULL);
    // CommandCounterIncrement() is called in ProcessUtility()
}

static List *create_junc_table_table_elements(char *graph_name, char *table_name,
					      char *schema_name, char *rel_name)
{
    ColumnDef *id;
    ColumnDef *label_id;
    Constraint *not_null;
    Constraint *fk;

    fk = build_fk_constraint(schema_name);

    not_null = makeNode(Constraint);
    not_null->contype = CONSTR_NOTNULL;
    not_null->location = -1;

    id = makeColumnDef(AG_VERTEX_COLNAME_ID, EIDOID, -1, InvalidOid);
    id->constraints = list_make2(fk, not_null);
    
    label_id = makeColumnDef(AG_VERTEX_COLNAME_LABEL_ID, INT4OID, -1,
                             InvalidOid);
    label_id->constraints = list_make1(not_null); 

    return list_make2(id, label_id);
}

// Build the foreign key constraint for the "id" column of the junction table
static Constraint *build_fk_constraint(char *schema_name)
{
    Constraint *fk;
    RangeVar *rv = makeRangeVar(schema_name, AG_DEFAULT_LABEL_VERTEX, -1);
  
    fk = makeNode(Constraint);
    fk->contype = CONSTR_FOREIGN;
    fk->location = -1;
    fk->keys = NULL;
    fk->options = NIL;
    fk->indexname = NULL;
    fk->indexspace = NULL;
    fk->pktable = rv;
    fk->fk_matchtype = 's';
    fk->fk_upd_action = 'c';
    fk->fk_del_action = 'c';

    return fk;
}

void init_junc_node(cypher_node *node)
{
    node->extensible.type = T_ExtensibleNode;
    node->extensible.extnodename = "cypher_node";
    node->label = AG_JUNCTION_TABLE;
    node->parsed_label = NULL;
    node->name = NULL;
    node->location = -1;
    node->props = NULL;
}

/*
 * This function is a simplified version of the function
 * transform_create_cypher_existing_node(). The necessary checks have already
 * happened in the transform_create_cypher_node().
 */
cypher_target_node *
    transform_junction_table_node(cypher_parsestate *cpstate, List **target_list,
				 cypher_node *node)
{
    cypher_target_node *rel = make_ag_node(cypher_target_node);
    Relation label_relation;
    RangeVar *rv;
    RangeTblEntry *rte;
    ParseNamespaceItem *pnsi;

    rel->type = LABEL_KIND_VERTEX;
    rel->tuple_position = InvalidAttrNumber;
    rel->variable_name = NULL;
    rel->resultRelInfo = NULL;
    rel->label_name = node->label;
    rel->flags = CYPHER_TARGET_NODE_FLAG_INSERT;

    rv = makeRangeVar(cpstate->graph_name, node->label, -1);
    label_relation = parserOpenTable(&cpstate->pstate, rv, RowExclusiveLock);

    // Store the relid
    rel->relid = RelationGetRelid(label_relation);

    pnsi = addRangeTableEntryForRelation((ParseState *)cpstate, label_relation,
                                        AccessShareLock, NULL, false, false);
    rte = pnsi->p_rte;
    rte->requiredPerms = ACL_INSERT;

    // id
    rel->id_expr = (Expr *)build_column_default(label_relation,
                                                Anum_ag_label_vertex_table_id);
    // label_id
    rel->label_id_expr = (Expr *)build_column_default(
        label_relation, Anum_ag_label_vertex_table_label_id - 1);
    table_close(label_relation, NoLock);
    node->name = get_next_default_alias(cpstate);

    return rel;
}


