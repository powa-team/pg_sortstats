/*-------------------------------------------------------------------------
 *
 * pg_sortstats_import.c
 *		Imported needed function.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

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
