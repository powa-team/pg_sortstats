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
#include "access/hash.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#if PG_VERSION_NUM >= 90500
#include "utils/ruleutils.h"
#endif
#include "utils/tuplesort.h"

#include "include/pg_sortstats_import.h"

PG_MODULE_MAGIC;

/*--- Macros and structs ---*/
#define PGSRT_COLUMNS		14			/* number of columns in pg_sortstats  SRF */
#define PGSRT_KEYS_SIZE		80
#define USAGE_DECREASE_FACTOR	(0.99)	/* decreased every pgsrt_entry_dealloc */
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */
#define USAGE_INIT	(1.0)

/* In PostgreSQL 11, queryid becomes a uint64 internally.
 */
#if PG_VERSION_NUM >= 110000
typedef uint64 pgsrt_queryid;
#else
typedef uint32 pgsrt_queryid;
#endif

typedef struct pgsrtSharedState
{
	LWLockId	lock;					/* protects hashtable search/modification */
	double		cur_median_usage;		/* current median usage in hashtable */
} pgsrtSharedState;

typedef struct pgsrtHashKey
{
	Oid				userid;			/* user OID */
	Oid				dbid;			/* database OID */
	pgsrt_queryid	queryid;		/* query identifier */
	uint32			sortid;			/* sort identifier withing a query */
} pgsrtHashKey;

typedef struct pgsrtCounters
{
	double			usage;					/* usage factor */
	int64			lines;					/* total number of lines in input */
	int64			lines_to_sort;			/* total number of lines sorted */
	int64			work_mems;				/* total size of estimated work_mem */
	int64			topn_sorts;				/* number of top-N heapsorts */
	int64			quicksorts;				/* number of quicksorts */
	int64			external_sorts;			/* number of external sorts */
	int64			external_merges;		/* number of external merges */
	int64			nbtapes;
	int64			space_disk;
	int64			space_memory;
	char			keys[PGSRT_KEYS_SIZE];	/* deparsed sort key */
} pgsrtCounters;

typedef struct pgsrtEntry
{
	pgsrtHashKey	key;
	pgsrtCounters	counters;	/* statistics for this sort */
	slock_t			mutex;				/* protects the counters only */
} pgsrtEntry;

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


extern PGDLLEXPORT Datum	pg_sortstats(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum	pg_sortstats_reset(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(pg_sortstats);
PG_FUNCTION_INFO_V1(pg_sortstats_reset);

static void pgsrt_shmem_startup(void);
static void pgsrt_shmem_shutdown(int code, Datum arg);
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

static Size pgsrt_memsize(void);

static pgsrtEntry *pgsrt_entry_alloc(pgsrtHashKey *key, char *keys);
static void pgsrt_entry_dealloc(void);
static void pgsrt_entry_reset(void);
static void pgsrt_entry_store(pgsrt_queryid queryId, pgsrtCounters *counters);
static uint32 pgsrt_hash_fn(const void *key, Size keysize);
static int	pgsrt_match_fn(const void *key1, const void *key2, Size keysize);

static bool pgsrt_planstate_walker(PlanState *ps, pgsrtWalkerContext *context);
static char * pgsrt_get_sort_group_keys(PlanState *planstate,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 pgsrtWalkerContext *context);
static void pgsrt_setup_walker_context(pgsrtWalkerContext *context);

/*--- Local variables ---*/
static int nesting_level = 0;
static bool pgsrt_enabled;
static int pgsrt_max;

static HTAB *pgsrt_hash = NULL;
static pgsrtSharedState *pgsrt = NULL;


void
_PG_init(void)
{
	if (!process_shared_preload_libraries_in_progress)
	{
		elog(ERROR, "This module can only be loaded via shared_preload_libraries");
		return;
	}

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

	DefineCustomIntVariable("pg_sortstats.max",
							"Sets the maximum number of statements tracked by pg_sortstats.",
							NULL,
							&pgsrt_max,
							1000,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	RequestAddinShmemSpace(pgsrt_memsize());
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("pg_sortstats", 1);
#else
	RequestAddinLWLocks(1);
#endif

	/* install hooks */
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
}

static void
pgsrt_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgsrt = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	pgsrt = ShmemInitStruct("pg_sortstats",
					sizeof(pgsrtSharedState),
					&found);

	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM >= 90600
		pgsrt->lock = &(GetNamedLWLockTranche("pg_sortstats"))->lock;
#else
		pgsrt->lock = LWLockAssign();
#endif
	}

	memset(&info, 0, sizeof(info));
	info.keysize = sizeof(pgsrtHashKey);
	info.entrysize = sizeof(pgsrtEntry);
	info.hash = pgsrt_hash_fn;
	info.match = pgsrt_match_fn;

	/* allocate stats shared memory hash */
	pgsrt_hash = ShmemInitHash("pg_sortstats hash",
							  pgsrt_max, pgsrt_max,
							  &info,
							  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE);

	LWLockRelease(AddinShmemInitLock);

	if (!IsUnderPostmaster)
		on_shmem_exit(pgsrt_shmem_shutdown, (Datum) 0);

	/*
	 * Done if some other process already completed our initialization.
	 */
	if (found)
		return;
}

/* Save the statistics into a file at shutdown */
static void
pgsrt_shmem_shutdown(int code, Datum arg)
{
	/* TODO */
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

static Size
pgsrt_memsize(void)
{
	Size	size;

	size = MAXALIGN(sizeof(pgsrtSharedState));
	size = add_size(size, hash_estimate_size(pgsrt_max, sizeof(pgsrtEntry)));

	return size;
}

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgsrt->lock
 */
static pgsrtEntry *
pgsrt_entry_alloc(pgsrtHashKey *key, char *keys)
{
	pgsrtEntry *entry;
	bool		found;

	/* Make space if needed */
	while (hash_get_num_entries(pgsrt_hash) >= pgsrt_max)
		pgsrt_entry_dealloc();

	/* Find or create an entry with desired hash code */
	entry = (pgsrtEntry *) hash_search(pgsrt_hash, key, HASH_ENTER, &found);

	if (!found)
	{
		/* New entry, initialize it */

		/* reset the statistics */
		memset(&entry->counters, 0, sizeof(pgsrtCounters));
		/* set the appropriate initial usage count */
		entry->counters.usage = USAGE_INIT;
		memcpy(entry->counters.keys, keys, PGSRT_KEYS_SIZE - 1);
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
	}

	return entry;
}

/*
 * qsort comparator for sorting into increasing usage order
 */
static int
entry_cmp(const void *lhs, const void *rhs)
{
	double		l_usage = (*(pgsrtEntry *const *) lhs)->counters.usage;
	double		r_usage = (*(pgsrtEntry *const *) rhs)->counters.usage;

	if (l_usage < r_usage)
		return -1;
	else if (l_usage > r_usage)
		return +1;
	else
		return 0;
}

/*
 * Deallocate least used entries.
 * Caller must hold an exclusive lock on pgsrt->lock.
 */
static void
pgsrt_entry_dealloc(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgsrtEntry **entries;
	pgsrtEntry  *entry;
	int			nvictims;
	int			i;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 */
	entries = palloc(hash_get_num_entries(pgsrt_hash) * sizeof(pgsrtEntry *));

	i = 0;
	hash_seq_init(&hash_seq, pgsrt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		entry->counters.usage *= USAGE_DECREASE_FACTOR;
	}

	qsort(entries, i, sizeof(pgsrtEntry *), entry_cmp);

	if (i > 0)
	{
		/* Record the (approximate) median usage */
		pgsrt->cur_median_usage = entries[i / 2]->counters.usage;
	}

	nvictims = Max(10, i * USAGE_DEALLOC_PERCENT / 100);
	nvictims = Min(nvictims, i);

	for (i = 0; i < nvictims; i++)
	{
		hash_search(pgsrt_hash, &entries[i]->key, HASH_REMOVE, NULL);
	}

	pfree(entries);
}

/* Remove all saved entries in shmem */
static void
pgsrt_entry_reset(void)
{
	HASH_SEQ_STATUS hash_seq;
	pgsrtEntry  *entry;

	LWLockAcquire(pgsrt->lock, LW_EXCLUSIVE);

	hash_seq_init(&hash_seq, pgsrt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		hash_search(pgsrt_hash, &entry->key, HASH_REMOVE, NULL);
	}

	LWLockRelease(pgsrt->lock);
}

static void
pgsrt_entry_store(pgsrt_queryid queryId, pgsrtCounters *counters)
{
	volatile pgsrtEntry *e;

	pgsrtHashKey key;
	pgsrtEntry  *entry;

	/* Safety check... */
	if (!pgsrt || !pgsrt_hash)
		return;

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;
	key.sortid = (uint32) hash_any((unsigned char *) counters->keys,
			strlen(counters->keys));

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgsrt->lock, LW_SHARED);

	entry = (pgsrtEntry *) hash_search(pgsrt_hash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(pgsrt->lock);
		LWLockAcquire(pgsrt->lock, LW_EXCLUSIVE);

		/* OK to create a new hashtable entry */
		entry = pgsrt_entry_alloc(&key, counters->keys);
	}

	/*
	 * Grab the spinlock while updating the counters */
	e = (volatile pgsrtEntry *) entry;

	SpinLockAcquire(&e->mutex);

	e->counters.usage += 1;
	e->counters.lines += counters->lines;
	e->counters.lines_to_sort += counters->lines_to_sort;
	e->counters.work_mems += counters->work_mems;
	e->counters.topn_sorts += counters->topn_sorts;
	e->counters.quicksorts += counters->quicksorts;
	e->counters.external_sorts += counters->external_sorts;
	e->counters.external_merges += counters->external_merges;
	e->counters.nbtapes += counters->nbtapes;
	e->counters.space_disk += counters->space_disk;
	e->counters.space_memory += counters->space_memory;

	SpinLockRelease(&e->mutex);

	LWLockRelease(pgsrt->lock);
}

/* Compute hash value for a pgsrtHashKey.  sortid is already hashed */
static uint32
pgsrt_hash_fn(const void *key, Size keysize)
{
	const pgsrtHashKey *k = (const pgsrtHashKey *) key;

	return hash_uint32((uint32) k->userid) ^
		hash_uint32((uint32) k->dbid) ^
		hash_uint32((uint32) k->queryid) ^
		k->sortid;
}

/* Compare two pgsrtHashKey keys.  Zero means match */
static int
pgsrt_match_fn(const void *key1, const void *key2, Size keysize)
{
	const pgsrtHashKey *k1 = (const pgsrtHashKey *) key1;
	const pgsrtHashKey *k2 = (const pgsrtHashKey *) key2;

	if (k1->userid == k2->userid &&
		k1->dbid == k2->dbid &&
		k1->queryid == k2->queryid &&
		k1->sortid == k2->sortid)
		return 0;
	else
		return 1;
}

static bool pgsrt_planstate_walker(PlanState *ps, pgsrtWalkerContext *context)
{
	if (IsA(ps, SortState))
	{
		pgsrtCounters counters;
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
		int64 w_m;

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

			counters.lines = lines;
			counters.lines_to_sort = lines_to_sort;
			counters.work_mems = w_m;
			if (stats.sortMethod == SORT_TYPE_TOP_N_HEAPSORT)
				counters.topn_sorts = 1;
			else
				counters.topn_sorts = 0;

			if (stats.sortMethod == SORT_TYPE_QUICKSORT)
				counters.quicksorts = 1;
			else
				counters.quicksorts = 0;

			if (stats.sortMethod == SORT_TYPE_EXTERNAL_SORT)
				counters.external_sorts = 1;
			else
				counters.external_sorts = 0;

			if (stats.sortMethod == SORT_TYPE_EXTERNAL_MERGE)
				counters.external_merges = 1;
			else
				counters.external_merges = 0;

			counters.nbtapes = nbtapes;

			if (stats.spaceType == SORT_SPACE_TYPE_DISK)
				counters.space_disk = spaceUsed;
			else
				counters.space_disk = 0;

			if (stats.spaceType == SORT_SPACE_TYPE_MEMORY)
				counters.space_memory = spaceUsed;
			else
				counters.space_memory = 0;

			memset(counters.keys, 0, PGSRT_KEYS_SIZE);
			memcpy(counters.keys, deparsed, PGSRT_KEYS_SIZE - 1);

			pgsrt_entry_store(context->queryDesc->plannedstmt->queryId, &counters);

			//elog(WARNING, "sort info:\n"
			//		"keys: %s\n"
			//		"type: %s\n"
			//		"space type: %s\n"
			//		"space: %ld kB\n"
			//		"lines to sort: %ld\n"
			//		"w_m estimated: %ld kB\n"
			//		"nbTapes: %d\n"
#if PG_VERSI//ON_NUM >= 110000
			//		"parallel: %s (%d)\n"
#endif
			//		"bounded? %s - %s , bound %ld - %ld",
			//		deparsed,
			//		sortMethod,
			//		spaceType,
			//		spaceUsed,
			//		lines_to_sort,
			//		w_m,
			//		nbtapes,
#if PG_VERSI//ON_NUM >= 110000
			//		(srtstate->shared_info ? "yes" : "no"),(srtstate->shared_info ? srtstate->shared_info->num_workers : -1),
#endif
			//		(srtstate->bounded ? "yes":"no"),(srtstate->bounded_Done ? "yes":"no"), srtstate->bound, srtstate->bound_Done);
		}
	}

	context->ancestors = lcons(ps, context->ancestors);

	return planstate_tree_walker(ps, pgsrt_planstate_walker, context);
}

/* Adapted from ExplainPrintPlan */
static void
pgsrt_setup_walker_context(pgsrtWalkerContext *context)
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

	pgsrt_setup_walker_context(context);

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
 * Reset statistics.
 */
PGDLLEXPORT Datum
pg_sortstats_reset(PG_FUNCTION_ARGS)
{
	if (!pgsrt)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_sortstats must be loaded via shared_preload_libraries")));

	pgsrt_entry_reset();
	PG_RETURN_VOID();
}

Datum
pg_sortstats(PG_FUNCTION_ARGS)
{
	ReturnSetInfo	*rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	TupleDesc		tupdesc;
	Tuplestorestate	*tupstore;
	HASH_SEQ_STATUS hash_seq;
	pgsrtEntry		*entry;


	if (!pgsrt)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("pg_stat_kcache must be loaded via shared_preload_libraries")));
	/* check to see if caller supports us returning a tuplestore */
	if (rsinfo == NULL || !IsA(rsinfo, ReturnSetInfo))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("set-valued function called in context that cannot accept a set")));
	if (!(rsinfo->allowedModes & SFRM_Materialize))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("materialize mode required, but it is not " \
							"allowed in this context")));

	/* Switch into long-lived context to construct returned data structures */
	per_query_ctx = rsinfo->econtext->ecxt_per_query_memory;
	oldcontext = MemoryContextSwitchTo(per_query_ctx);

	/* Build a tuple descriptor for our result type */
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		elog(ERROR, "return type must be a row type");

	tupstore = tuplestore_begin_heap(true, false, work_mem);
	rsinfo->returnMode = SFRM_Materialize;
	rsinfo->setResult = tupstore;
	rsinfo->setDesc = tupdesc;

	MemoryContextSwitchTo(oldcontext);

	LWLockAcquire(pgsrt->lock, LW_SHARED);

	hash_seq_init(&hash_seq, pgsrt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		Datum			values[PGSRT_COLUMNS];
		bool			nulls[PGSRT_COLUMNS];
		pgsrtCounters	tmp;
		int				i = 0;

		memset(values, 0, sizeof(values));
		memset(nulls, 0, sizeof(nulls));

		/* copy counters to a local variable to keep locking time short */
		{
			volatile pgsrtEntry *e = (volatile pgsrtEntry *) entry;

			SpinLockAcquire(&e->mutex);
			tmp = e->counters;
			SpinLockRelease(&e->mutex);
		}

		values[i++] = Int64GetDatum(entry->key.queryid);
		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);
		values[i++] = CStringGetTextDatum(tmp.keys);
		values[i++] = Int64GetDatumFast(tmp.lines);
		values[i++] = Int64GetDatumFast(tmp.lines_to_sort);
		values[i++] = Int64GetDatumFast(tmp.work_mems);
		values[i++] = Int64GetDatumFast(tmp.topn_sorts);
		values[i++] = Int64GetDatumFast(tmp.quicksorts);
		values[i++] = Int64GetDatumFast(tmp.external_sorts);
		values[i++] = Int64GetDatumFast(tmp.external_merges);
		values[i++] = Int64GetDatumFast(tmp.nbtapes);
		values[i++] = Int64GetDatumFast(tmp.space_disk);
		values[i++] = Int64GetDatumFast(tmp.space_memory);

		Assert(i == PGSRT_COLUMNS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	LWLockRelease(pgsrt->lock);

	/* clean up and return the tuplestore */
	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}
