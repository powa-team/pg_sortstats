#ifndef PG_SORTSTATS_IMPORT_h
#define PG_SORTSTATS_IMPORT_h

#include "nodes/execnodes.h"
#include "utils/logtape.h"
#if PG_VERSION_NUM < 90500
#include "lib/stringinfo.h"
#endif

#if PG_VERSION_NUM >= 90400 && PG_VERSION_NUM < 90500
#include "include/pg_sortstats_import_pg9_4.h"
#elif PG_VERSION_NUM >= 90500 && PG_VERSION_NUM < 90600
#include "include/pg_sortstats_import_pg9_5.h"
#elif PG_VERSION_NUM >= 90600 && PG_VERSION_NUM < 100000
#include "include/pg_sortstats_import_pg9_6.h"
#elif PG_VERSION_NUM >= 100000 && PG_VERSION_NUM < 110000
#include "include/pg_sortstats_import_pg10.h"
#elif PG_VERSION_NUM >= 110000 && PG_VERSION_NUM < 120000
#include "include/pg_sortstats_import_pg11.h"
#elif PG_VERSION_NUM >= 120000 && PG_VERSION_NUM < 130000
#include "include/pg_sortstats_import_pg12.h"
#elif PG_VERSION_NUM >= 130000 && PG_VERSION_NUM < 140000
#include "include/pg_sortstats_import_pg13.h"
#else
#error "PostgreSQL version not supported"
#endif

/*
 * Import some define that are stable enough so that we don't need a
 * per-major-version definition
 */
#define PGSRT_ALLOC_MINBITS		3	/* smallest chunk size is 8 bytes */
#define PGSRT_ALLOCSET_NUM_FREELISTS	11
#define PGSRT_ALLOC_CHUNK_LIMIT	(1 << (PGSRT_ALLOCSET_NUM_FREELISTS-1+PGSRT_ALLOC_MINBITS))

bool pgsrt_PreScanNode(PlanState *planstate, Bitmapset **rels_used);
void pgsrt_show_sortorder_options(StringInfo buf, Node *sortexpr,
					   Oid sortOperator, Oid collation, bool nullsFirst);

#if PG_VERSION_NUM < 90600

bool planstate_tree_walker(PlanState *planstate,
					  bool (*walker) (),
					  void *context);

bool planstate_walk_subplans(List *plans,
						bool (*walker) (),
						void *context);


bool planstate_walk_members(PlanState **planstates, int nplans,
					   bool (*walker) (), void *context);

#endif		/* PG_VERSION_NUM < 90600 */

#endif
