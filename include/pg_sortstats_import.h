#ifndef PG_SORTSTATS_IMPORT_h
#define PG_SORTSTATS_IMPORT_h

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
#else
#error "PostgreSQL version not supported"
#endif

#if PG_VERSION_NUM < 90600

#include "nodes/execnodes.h"

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
