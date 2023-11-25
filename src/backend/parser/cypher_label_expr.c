#include "catalog/ag_label.h"
#include "commands/label_commands.h"
#include "nodes/cypher_nodes.h"
#include "utils/ag_cache.h"

#include "parser/cypher_label_expr.h"
#include "parser/cypher_transform_entity.h"

/*
 * Returns the first label in label_expr that exists in the cache and is not
 * of type label_kind.
 *
 * This is a helper function to check if all labels in label_expr are valid in
 * kind, during node\edge creation.
 */
char *find_first_invalid_label(cypher_label_expr *label_expr,
                               char label_expr_kind, Oid graph_oid)
{
    ListCell *lc;

    foreach (lc, label_expr->label_names)
    {
        char *label_name;
        label_cache_data *cache_data;

        label_name = strVal(lfirst(lc));
        cache_data = search_label_name_graph_cache(label_name, graph_oid);

        if (cache_data && cache_data->kind != label_expr_kind)
        {
            return label_name;
        }
    }

    return NULL;
}

// TODO: can be moved to utils?
/*
 * List comparator for String nodes. The ListCells a and b must
 * contain node pointer of type T_String.
 */
int list_string_cmp(const ListCell *a, const ListCell *b)
{
    Node *na = lfirst(a);
    Node *nb = lfirst(b);

    Assert(IsA(na, String));
    Assert(IsA(nb, String));

    return strcmp(strVal(na), strVal(nb));
}

/*
 * Generates relation name for label_expr. It does not check if the relation
 * exists for that name. The caller must do existence check before using the
 * table. This function is not applicable for LABEL_EXPR_TYPE_OR.
 */
char *label_expr_relname(cypher_label_expr *label_expr, char label_expr_kind)
{
    switch (label_expr->type)
    {
    case LABEL_EXPR_TYPE_EMPTY:
        return label_expr_kind == LABEL_KIND_VERTEX ? AG_DEFAULT_LABEL_VERTEX :
                                                      AG_DEFAULT_LABEL_EDGE;

    case LABEL_EXPR_TYPE_SINGLE:
        Assert(list_length(label_expr->label_names) == 1);
        return (char *)strVal(linitial(label_expr->label_names));

    case LABEL_EXPR_TYPE_AND:
        StringInfo relname_strinfo;
        ListCell *lc;
        char *relname;

        Assert(list_length(label_expr->label_names) > 1);

        relname_strinfo = makeStringInfo();
        appendStringInfoString(relname_strinfo, INTR_REL_PREFIX);

        foreach (lc, label_expr->label_names)
        {
            char *label_name = strVal(lfirst(lc));
            appendStringInfoString(relname_strinfo, label_name);
        }

        relname = relname_strinfo->data;
        pfree(relname_strinfo);

        return relname;

    case LABEL_EXPR_TYPE_OR:
        elog(ERROR, "label expression type OR cannot have a table");
        return NULL;

    default:
        elog(ERROR, "invalid cypher_label_expr type");
        return NULL;
    }
}

// TODO: whenever a new field is added, update this one.
bool label_expr_are_equal(cypher_label_expr *le1, cypher_label_expr *le2)
{
    ListCell *lc1;
    ListCell *lc2;

    if (le1 == le2)
    {
        return true;
    }

    /**
     * If exactly one is null, return false.
     * TODO: should null be allowed?
     */
    if ((le1 == NULL && le2 != NULL) || (le2 == NULL && le1 != NULL))
    {
        return false;
    }

    if (le1->type != le2->type)
    {
        return false;
    }

    if (LABEL_EXPR_LENGTH(le1) != LABEL_EXPR_LENGTH(le2))
    {
        return false;
    }

    /*
     * Assuming both lists are sorted and have same length.
     */
    forboth(lc1, le1->label_names, lc2, le2->label_names)
    {
        char *le1_label = strVal(lfirst(lc1));
        char *le2_label = strVal(lfirst(lc2));

        if (strcmp(le1_label, le2_label) != 0)
        {
            return false;
        }
    }

    return true;
}

/*
 * This code is borrowed from Postgres' list_intersection since it does not
 * implement an intersection function for OIDs. Use of non-public functions and
 * macros are commented out.
 */
List *list_intersection_oid(List *list1, const List *list2)
{
    List *result;
    const ListCell *cell;

    if (list1 == NIL || list2 == NIL)
    {
        return NIL;
    }

    // Assert(IsOidList(list1));
    // Assert(IsOidList(list2));

    result = NIL;
    foreach (cell, list1)
    {
        if (list_member_oid(list2, lfirst_oid(cell)))
        {
            result = lappend_oid(result, lfirst_oid(cell));
        }
    }

    // check_list_invariants(result);
    return result;
}

/*
 * Returns a list of relation OIDs to be scanned by the MATCH clause. May
 * return empty list as `NIL`.
 */
List *get_reloids_to_scan(cypher_label_expr *label_expr, char label_expr_kind,
                          Oid graph_oid)
{
    cypher_label_expr_type label_expr_type = LABEL_EXPR_TYPE(label_expr);
    char *label_name;
    label_cache_data *lcd;

    switch (label_expr_type)
    {
    case LABEL_EXPR_TYPE_EMPTY:
        label_name = label_expr_kind == LABEL_KIND_VERTEX ?
                               AG_DEFAULT_LABEL_VERTEX :
                               AG_DEFAULT_LABEL_EDGE;
        lcd = search_label_name_graph_cache(label_name, graph_oid);
        Assert(lcd); /* default labels should always exist */

        return list_copy(lcd->clusters);

    case LABEL_EXPR_TYPE_SINGLE:
        label_name = strVal(linitial(label_expr->label_names));
        lcd = search_label_name_graph_cache(label_name, graph_oid);

        if (!lcd || lcd->kind != label_expr_kind)
        {
            return NIL;
        }

        return list_copy(lcd->clusters);

    case LABEL_EXPR_TYPE_AND:
    case LABEL_EXPR_TYPE_OR:
        List *reloids;
        List *(*merge_lists)(List *, const List *);
        ListCell *lc;

        reloids = NIL;

        /*
         * This function pointer describes how to combine two lists of relation
         * oids.
         *
         * For AND, uses intersection.
         *      MATCH (:A:B) -> intersection of allrelations of A and B.
         *      To scan only common relations of A and B.
         *
         * For OR, uses concat (similar to union).
         *      MATCH (:A|B) -> union of allrelations of A and B.
         *      To scan all relations of A and B.
         */
        merge_lists = label_expr_type == LABEL_EXPR_TYPE_AND ?
                          &list_intersection_oid :
                          &list_concat_unique_oid;

        foreach (lc, label_expr->label_names)
        {
            label_name = strVal(lfirst(lc));
            lcd = search_label_name_graph_cache(label_name, graph_oid);

            /* if some label_name does not exist in cache */
            if (!lcd || lcd->kind != label_expr_kind)
            {
                if (label_expr_type == LABEL_EXPR_TYPE_AND)
                {
                    /* for AND, no relation to scan */
                    return NIL;
                }
                else
                {
                    /* for OR, skip that label */
                    continue;
                }
            }

            /* if first iteration */
            if (list_length(reloids) == 0)
            {
                reloids = list_copy(lcd->clusters);
                continue;
            }
            else
            {
                /*
                 * "Merges" lcd->clusters into reloids.
                 *
                 * At the end of the loop, reloids is a result of all labels'
                 * clusters merged together using the merge_list funciton.
                 * For example, for OR, reloids is a result of a chain of
                 * union.
                 */
                reloids = merge_lists(reloids, lcd->clusters);
            }
        }
        return reloids;

    default:
        elog(ERROR, "invalid cypher_label_expr type");
        return NIL;
    }
}

/*
 * Checks invalid label expression type in CREATE\MERGE clause and reports
 * error if invalid.
 *
 * This is used in CREATE and MERGE's transformation.
 */
void check_label_expr_type_for_create(ParseState *pstate, Node *entity)
{
    cypher_label_expr *label_expr;
    int location;
    char label_expr_kind;
    char *variable_name;

    if (is_ag_node(entity, cypher_node))
    {
        cypher_node *node = (cypher_node *)entity;
        label_expr = node->label_expr;
        location = node->location;
        label_expr_kind = LABEL_KIND_VERTEX;
        variable_name = node->name;
    }
    else if (is_ag_node(entity, cypher_relationship))
    {
        cypher_relationship *rel = (cypher_relationship *)entity;
        label_expr = rel->label_expr;
        location = rel->location;
        label_expr_kind = LABEL_KIND_EDGE;
        variable_name = rel->name;
    }
    else
    {
        ereport(ERROR,
                (errmsg_internal("unrecognized node in create pattern")));
    }

    /*
     * If entity has a variable which was declared in a previous clause,
     * then skip the check since the check has been done already.
     */
    if (variable_name &&
        find_variable((cypher_parsestate *)pstate, variable_name))
    {
        return;
    }

    /*
     * In a CREATE\MERGE clause, following types are allowed-
     *      for vertices: empty, single, and
     *      for edges   : single
     */
    if (LABEL_EXPR_TYPE(label_expr) == LABEL_EXPR_TYPE_OR)
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg(
                 "label expression type OR is not allowed in a CREATE\\MERGE clause"),
             parser_errposition(pstate, location)));
    }

    if (label_expr_kind == LABEL_KIND_EDGE &&
        LABEL_EXPR_TYPE(label_expr) != LABEL_EXPR_TYPE_SINGLE)
    {
        ereport(
            ERROR,
            (errcode(ERRCODE_SYNTAX_ERROR),
             errmsg(
                 "relationships must have exactly one label in a CREATE\\MERGE clause"),
             parser_errposition(pstate, location)));
    }

    return;
}
