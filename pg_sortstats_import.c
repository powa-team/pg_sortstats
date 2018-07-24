/*-------------------------------------------------------------------------
 *
 * pg_sortstats_import.c
 *		Imported needed function.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#if PG_VERSION_NUM >= 110000
#include "catalog/pg_collation_d.h"
#else
#include "catalog/pg_collation.h"
#endif
#include "nodes/execnodes.h"
#include "nodes/nodeFuncs.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/tuplesort.h"
#include "utils/typcache.h"

#include "include/pg_sortstats_import.h"

/*
 * Imported from ExplainPreScanNode
 */
bool
pgsrt_PreScanNode(PlanState *planstate, Bitmapset **rels_used)
{
	Plan	   *plan = planstate->plan;

	switch (nodeTag(plan))
	{
		case T_SeqScan:
#if PG_VERSION_NUM >= 90500
		case T_SampleScan:
#endif
		case T_IndexScan:
		case T_IndexOnlyScan:
		case T_BitmapHeapScan:
		case T_TidScan:
		case T_SubqueryScan:
		case T_FunctionScan:
#if PG_VERSION_NUM >= 100000
		case T_TableFuncScan:
#endif
		case T_ValuesScan:
		case T_CteScan:
#if PG_VERSION_NUM >= 100000
		case T_NamedTuplestoreScan:
#endif
		case T_WorkTableScan:
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
			break;
		case T_ForeignScan:
#if PG_VERSION_NUM >= 90500
			*rels_used = bms_add_members(*rels_used,
										 ((ForeignScan *) plan)->fs_relids);
#else
			*rels_used = bms_add_member(*rels_used,
										((Scan *) plan)->scanrelid);
#endif
			break;
#if PG_VERSION_NUM >= 90500
		case T_CustomScan:
			*rels_used = bms_add_members(*rels_used,
										 ((CustomScan *) plan)->custom_relids);
			break;
#endif
		case T_ModifyTable:
#if PG_VERSION_NUM >= 90500
			*rels_used = bms_add_member(*rels_used,
										((ModifyTable *) plan)->nominalRelation);
			if (((ModifyTable *) plan)->exclRelRTI)
				*rels_used = bms_add_member(*rels_used,
											((ModifyTable *) plan)->exclRelRTI);
#else
			/* cf ExplainModifyTarget */
			*rels_used = bms_add_member(*rels_used,
					  linitial_int(((ModifyTable *) plan)->resultRelations));
#endif
			break;
		default:
			break;
	}

	return planstate_tree_walker(planstate, pgsrt_PreScanNode, rels_used);
}

/* Imported from show_sortorder_options */
void
pgsrt_show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst)
{
	Oid			sortcoltype = exprType(sortexpr);
	bool		reverse = false;
	TypeCacheEntry *typentry;

	typentry = lookup_type_cache(sortcoltype,
								 TYPECACHE_LT_OPR | TYPECACHE_GT_OPR);

	/*
	 * Print COLLATE if it's not default.  There are some cases where this is
	 * redundant, eg if expression is a column whose declared collation is
	 * that collation, but it's hard to distinguish that here.
	 */
	if (OidIsValid(collation) && collation != DEFAULT_COLLATION_OID)
	{
		char	   *collname = get_collation_name(collation);

		if (collname == NULL)
			elog(ERROR, "cache lookup failed for collation %u", collation);
		appendStringInfo(buf, " COLLATE %s", quote_identifier(collname));
	}

	/* Print direction if not ASC, or USING if non-default sort operator */
	if (sortOperator == typentry->gt_opr)
	{
		appendStringInfoString(buf, " DESC");
		reverse = true;
	}
	else if (sortOperator != typentry->lt_opr)
	{
		char	   *opname = get_opname(sortOperator);

		if (opname == NULL)
			elog(ERROR, "cache lookup failed for operator %u", sortOperator);
		appendStringInfo(buf, " USING %s", opname);
		/* Determine whether operator would be considered ASC or DESC */
		(void) get_equality_op_for_ordering_op(sortOperator, &reverse);
	}

	/* Add NULLS FIRST/LAST only if it wouldn't be default */
	if (nullsFirst && !reverse)
	{
		appendStringInfoString(buf, " NULLS FIRST");
	}
	else if (!nullsFirst && reverse)
	{
		appendStringInfoString(buf, " NULLS LAST");
	}
}

#if PG_VERSION_NUM < 90600

#include "nodes/nodes.h"
#include "nodes/pg_list.h"
#include "utils/logtape.h"
#include "utils/tuplesort.h"

#include "include/pg_sortstats_import.h"

bool
planstate_tree_walker(PlanState *planstate,
					  bool (*walker) (),
					  void *context)
{
	Plan	   *plan = planstate->plan;
#if PG_VERSION_NUM >= 90500
	ListCell   *lc;
#endif

	/* initPlan-s */
	if (planstate_walk_subplans(planstate->initPlan, walker, context))
		return true;

	/* lefttree */
	if (outerPlanState(planstate))
	{
		if (walker(outerPlanState(planstate), context))
			return true;
	}

	/* righttree */
	if (innerPlanState(planstate))
	{
		if (walker(innerPlanState(planstate), context))
			return true;
	}

	/* special child plans */
	switch (nodeTag(plan))
	{
		case T_ModifyTable:
			if (planstate_walk_members(((ModifyTableState *) planstate)->mt_plans,
									   ((ModifyTableState *) planstate)->mt_nplans,
									   walker, context))
				return true;
			break;
		case T_Append:
			if (planstate_walk_members(((AppendState *) planstate)->appendplans,
									   ((AppendState *) planstate)->as_nplans,
									   walker, context))
				return true;
			break;
		case T_MergeAppend:
			if (planstate_walk_members(((MergeAppendState *) planstate)->mergeplans,
									   ((MergeAppendState *) planstate)->ms_nplans,
									   walker, context))
				return true;
			break;
		case T_BitmapAnd:
			if (planstate_walk_members(((BitmapAndState *) planstate)->bitmapplans,
									   ((BitmapAndState *) planstate)->nplans,
									   walker, context))
				return true;
			break;
		case T_BitmapOr:
			if (planstate_walk_members(((BitmapOrState *) planstate)->bitmapplans,
									   ((BitmapOrState *) planstate)->nplans,
									   walker, context))
				return true;
			break;
		case T_SubqueryScan:
			if (walker(((SubqueryScanState *) planstate)->subplan, context))
				return true;
			break;
#if PG_VERSION_NUM >= 90500
		case T_CustomScan:
			foreach(lc, ((CustomScanState *) planstate)->custom_ps)
			{
				if (walker((PlanState *) lfirst(lc), context))
					return true;
			}
			break;
#endif
		default:
			break;
	}

	/* subPlan-s */
	if (planstate_walk_subplans(planstate->subPlan, walker, context))
		return true;

	return false;
}

bool
planstate_walk_subplans(List *plans,
						bool (*walker) (),
						void *context)
{
	ListCell   *lc;

	foreach(lc, plans)
	{
		SubPlanState *sps = lfirst_node(SubPlanState, lc);

		if (walker(sps->planstate, context))
			return true;
	}

	return false;
}

bool
planstate_walk_members(PlanState **planstates, int nplans,
					   bool (*walker) (), void *context)
{
	int			j;

	for (j = 0; j < nplans; j++)
	{
		if (walker(planstates[j], context))
			return true;
	}

	return false;
}
#endif
