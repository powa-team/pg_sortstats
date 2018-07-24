/*-------------------------------------------------------------------------
 *
 * pg_sortstats.c
 *		Track statistics about sorts performs, and also estimate how much
 *		work_mem would have been needed to sort data in memory.
 *
 * Copyright (c) 2018, Julien Rouhaud
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#if PG_VERSION_NUM >= 110000
#include "catalog/pg_collation_d.h"
#else
#include "catalog/pg_collation.h"
#endif
#if PG_VERSION_NUM < 90500
#include "lib/stringinfo.h"
#endif
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/logtape.h"
#include "utils/lsyscache.h"
#if PG_VERSION_NUM >= 90500
#include "utils/ruleutils.h"
#endif
#include "utils/tuplesort.h"
#include "utils/typcache.h"

#include "include/pg_sortstats_import.h"

PG_MODULE_MAGIC;

/*--- Macros and structs ---*/
#define PGSRT_COLUMNS 3			/* number of columns in pg_sortstats  SRF */

typedef struct pgsrtWalkerContext
{
	QueryDesc *queryDesc;
	List	  *ancestors;
	List	  *rtable;
	List	  *rtable_names;
	List	  *deparse_cxt;
} pgsrtWalkerContext;

/*--- Function declarations ---*/

void		_PG_init(void);
void		_PG_fini(void);


extern Datum pg_sortstats(PG_FUNCTION_ARGS);
PG_FUNCTION_INFO_V1(pg_sortstats);

static void pgsrt_shmem_startup(void);
static void pgsrt_ExecutorStart(QueryDesc *queryDesc, int eflags);
static void pgsrt_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
#if PG_VERSION_NUM >= 90600
				 uint64 count
#else
				 long count
#endif
#if PG_VERSION_NUM >= 100000
				 ,bool execute_once
#endif
);
static void pgsrt_ExecutorFinish(QueryDesc *queryDesc);
static void pgsrt_ExecutorEnd(QueryDesc *queryDesc);

static shmem_startup_hook_type prev_shmem_startup_hook = NULL;
static ExecutorStart_hook_type prev_ExecutorStart = NULL;
static ExecutorRun_hook_type prev_ExecutorRun = NULL;
static ExecutorFinish_hook_type prev_ExecutorFinish = NULL;
static ExecutorEnd_hook_type prev_ExecutorEnd = NULL;

static bool pgsrt_planstate_walker(PlanState *ps, pgsrtWalkerContext *context);
static char * pgsrt_get_sort_group_keys(PlanState *planstate,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 pgsrtWalkerContext *context);
static bool pgsrt_PreScanNode(PlanState *planstate, Bitmapset **rels_used);
static void pgsrt_setup_deparse_cxt(pgsrtWalkerContext *context);
static void pgsrt_show_sortorder_options(StringInfo buf, Node *sortexpr,
		Oid sortOperator, Oid collation, bool nullsFirst);

/*--- Local variables ---*/
static int nesting_level = 0;
static bool pgsrt_enabled;
//static int pgsrt_max;

/* Global Hash */
//static HTAB *pgsrt_hash = NULL;
//static pgsrtSharedState *pgsrt = NULL;


void
_PG_init(void)
{
	//if (!process_shared_preload_libraries_in_progress)
	//{
	//	elog(ERROR, "This module can only be loaded via shared_preload_libraries");
	//	return;
	//}

	prev_ExecutorStart = ExecutorStart_hook;
	ExecutorStart_hook = pgsrt_ExecutorStart;
	prev_ExecutorRun = ExecutorRun_hook;
	ExecutorRun_hook = pgsrt_ExecutorRun;
	prev_ExecutorFinish = ExecutorFinish_hook;
	ExecutorFinish_hook = pgsrt_ExecutorFinish;
	prev_ExecutorEnd = ExecutorEnd_hook;
	ExecutorEnd_hook = pgsrt_ExecutorEnd;
	prev_shmem_startup_hook = shmem_startup_hook;
	shmem_startup_hook = pgsrt_shmem_startup;

	DefineCustomBoolVariable("pg_sortstats.enabled",
							 "Enable / Disable pg_sortstats",
							 NULL,
							 &pgsrt_enabled,
							 true,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	//DefineCustomIntVariable("pg_sortstats.max",
	//						"Sets the maximum number of statements tracked by pg_sortstats.",
	//						NULL,
	//						&pgsrt_max,
	//						1000,
	//						100,
	//						INT_MAX,
	//						PGC_POSTMASTER,
	//						0,
	//						NULL,
	//						NULL,
	//						NULL);
}

static void
pgsrt_shmem_startup(void)
{
}

void
_PG_fini(void)
{
	/* Uninstall hooks. */
	shmem_startup_hook = prev_shmem_startup_hook;
	ExecutorStart_hook = prev_ExecutorStart;
	ExecutorRun_hook = prev_ExecutorRun;
	ExecutorFinish_hook = prev_ExecutorFinish;
	ExecutorEnd_hook = prev_ExecutorEnd;
}

/*
 * Nothing to do?
 */
static void
pgsrt_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
	if (pgsrt_enabled)
	{
	}

	if (prev_ExecutorStart)
		prev_ExecutorStart(queryDesc, eflags);
	else
		standard_ExecutorStart(queryDesc, eflags);

}

/*
 * ExecutorRun hook: all we need do is track nesting depth
 */
static void
pgsrt_ExecutorRun(QueryDesc *queryDesc,
				 ScanDirection direction,
#if PG_VERSION_NUM >= 90600
				 uint64 count
#else
				 long count
#endif
#if PG_VERSION_NUM >= 100000
				 ,bool execute_once
#endif
)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorRun)
#if PG_VERSION_NUM >= 100000
			prev_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			prev_ExecutorRun(queryDesc, direction, count);
#endif
		else
#if PG_VERSION_NUM >= 100000
			standard_ExecutorRun(queryDesc, direction, count, execute_once);
#else
			standard_ExecutorRun(queryDesc, direction, count);
#endif
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * ExecutorFinish hook: all we need do is track nesting depth
 */
static void
pgsrt_ExecutorFinish(QueryDesc *queryDesc)
{
	nesting_level++;
	PG_TRY();
	{
		if (prev_ExecutorFinish)
			prev_ExecutorFinish(queryDesc);
		else
			standard_ExecutorFinish(queryDesc);
		nesting_level--;
	}
	PG_CATCH();
	{
		nesting_level--;
		PG_RE_THROW();
	}
	PG_END_TRY();
}

/*
 * Walk the planstates, find any sorts and gather their statistics.
 */
static void
pgsrt_ExecutorEnd(QueryDesc *queryDesc)
{
	pgsrtWalkerContext context;

	context.queryDesc = queryDesc;
	context.ancestors = NIL;

	/* retrieve sorts informations, main work starts from here */
	pgsrt_planstate_walker(queryDesc->planstate, &context);

	if (prev_ExecutorEnd)
		prev_ExecutorEnd(queryDesc);
	else
		standard_ExecutorEnd(queryDesc);
}

static bool pgsrt_planstate_walker(PlanState *ps, pgsrtWalkerContext *context)
{
	if (IsA(ps, SortState))
	{
		Plan *plan = ps->plan;
		SortState *srtstate = (SortState *) ps;
		Tuplesortstate *state = (Tuplesortstate *) srtstate->tuplesortstate;
#if PG_VERSION_NUM >= 110000
		TuplesortInstrumentation stats;
#endif
		Sort *sort = (Sort *) plan;
		char *deparsed;
		int nbtapes = 0;
		const char *sortMethod;
		const char *spaceType;
		long		spaceUsed;
		int mem_per_row;
		int64 lines;
		int64 lines_to_sort;
		int tuple_palloc;
		int i = 1;
		long w_m;

		if (state)
		{
			tuple_palloc = sort->plan.plan_width + 16;

			/*
			 * the tuple is palloced, and a palloc chunk always uses a 2^N
			 * size
			 */
			while (tuple_palloc > i)
				i *=2;
			tuple_palloc = i;

			lines = 0;
			/* get effective number of lines */
			if (ps->instrument)
				lines = ps->instrument->ntuples;

			/* fallback to estimated # of lines if no value */
			if (lines == 0)
				lines = sort->plan.plan_rows;

			/*
			 * If the sort is bounded, set the number of lines to sort
			 * accordingly.
			 */
			if (srtstate->bounded)
				lines_to_sort = srtstate->bound;
			else
				lines_to_sort = lines;

			/*
			 * compute the per-row space needed. The involved struct aren't
			 * exported, so just use raw number instead. FTR, the formula:
			 * sizeof(SortTuple) + sizeof(MinimalTuple) + sizeof(AllocChunkData) + palloced;
			 * */
			mem_per_row = 24 + 8 + 16 + tuple_palloc;
			w_m = lines_to_sort * mem_per_row;

			/*
			 * If a bounded sort was asked, we'll try to sort the bound limit
			 * number of line, but a Top-N heapsort requires twice the amount
			 * of memory.
			 */
			if (srtstate->bounded)
				w_m *= 2;

			/* convert in kB, and add 1 kB as a quick round up */
			w_m /= 1024;
			w_m += 1;

			/* deparse the sort keys */
			deparsed = pgsrt_get_sort_group_keys(ps, sort->numCols,
					sort->sortColIdx, sort->sortOperators, sort->collations,
					sort->nullsFirst, context);

#if PG_VERSION_NUM >= 110000
			tuplesort_get_stats(state, &stats);
			sortMethod = tuplesort_method_name(stats.sortMethod);
			spaceType = tuplesort_space_type_name(stats.spaceType);
			spaceUsed = stats.spaceUsed;
#else
			tuplesort_get_stats(state, &sortMethod, &spaceType, &spaceUsed);
#endif

			if (strcmp(sortMethod, "external merge") == 0)
			{
				nbtapes = ((struct pgsrt_Tuplesortstate *) state)->currentRun + 1;
			}

			elog(WARNING, "sort info:\n"
					"keys: %s\n"
					"type: %s\n"
					"space type: %s\n"
					"space: %ld kB\n"
					"w_m estimated: %ld kB\n"
					"nbTapes: %d\n"
#if PG_VERSION_NUM >= 110000
					"parallel: %s (%d)\n"
#endif
					"bounded? %s - bound %ld - %ld",
					deparsed,
					sortMethod,
					spaceType,
					spaceUsed,
					w_m,
					nbtapes,
#if PG_VERSION_NUM >= 110000
					(srtstate->shared_info ? "yes" : "no"),(srtstate->shared_info ? srtstate->shared_info->num_workers : -1),
#endif
					(srtstate->bounded ? "yes":"no"), srtstate->bound, srtstate->bound_Done);
		}
	}

	context->ancestors = lcons(ps, context->ancestors);

	return planstate_tree_walker(ps, pgsrt_planstate_walker, context);
}

Datum
pg_sortstats(PG_FUNCTION_ARGS)
{
	return (Datum) 0;
}

/* Adapted from ExplainPrintPlan */
static void
pgsrt_setup_deparse_cxt(pgsrtWalkerContext *context)
{
	Bitmapset  *rels_used = NULL;

	/* Set up ExplainState fields associated with this plan tree */
	Assert(context->queryDesc->plannedstmt != NULL);

	context->rtable = context->queryDesc->plannedstmt->rtable;
	pgsrt_PreScanNode(context->queryDesc->planstate, &rels_used);
	context->rtable_names = select_rtable_names_for_explain(context->rtable,
			rels_used);
	context->deparse_cxt = deparse_context_for_plan_rtable(context->rtable,
			context->rtable_names);
}

/* Adapted from show_sort_group_keys */
static char *
pgsrt_get_sort_group_keys(PlanState *planstate,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 pgsrtWalkerContext *context)
{
	Plan	   *plan = planstate->plan;
	List	   *dp_context = NIL;
	StringInfoData sortkeybuf;
	bool		useprefix;
	int			keyno;

	if (nkeys <= 0)
		return "nothing?";

	pgsrt_setup_deparse_cxt(context);

	initStringInfo(&sortkeybuf);

	/* Set up deparsing context */
	dp_context = set_deparse_context_planstate(context->deparse_cxt,
											(Node *) planstate,
											context->ancestors);
	useprefix = (list_length(context->rtable) > 1);

	for (keyno = 0; keyno < nkeys; keyno++)
	{
		/* find key expression in tlist */
		AttrNumber	keyresno = keycols[keyno];
		TargetEntry *target = get_tle_by_resno(plan->targetlist,
											   keyresno);
		char	   *exprstr;

		if (keyno != 0)
			appendStringInfoString(&sortkeybuf, ", ");

		if (!target)
			elog(ERROR, "no tlist entry for key %d", keyresno);
		/* Deparse the expression, showing any top-level cast */
		exprstr = deparse_expression((Node *) target->expr, dp_context,
									 useprefix, true);
		appendStringInfoString(&sortkeybuf, exprstr);

		/* Append sort order information, if relevant */
		if (sortOperators != NULL)
			pgsrt_show_sortorder_options(&sortkeybuf,
								   (Node *) target->expr,
								   sortOperators[keyno],
								   collations[keyno],
								   nullsFirst[keyno]);
	}

	return sortkeybuf.data;
}

/*
 * Imported from ExplainPreScanNode
 */
static bool
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

/* copied from show_sortorder_options */
static void
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
