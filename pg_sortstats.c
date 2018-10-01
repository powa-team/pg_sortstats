/*-------------------------------------------------------------------------
 *
 * pg_sortstats.c
 *		Track statistics about sorts performs, and also estimate how much
 *		work_mem would have been needed to sort data in memory.
 *
 * This module is heavily inspired on the great pg_stat_statements official
 * contrib.  The same locking rules are used, which for reference are:
 *
 * Note about locking issues: to create or delete an entry in the shared
 * hashtable, one must hold pgsrt->lock exclusively.  Modifying any field
 * in an entry except the counters requires the same.  To look up an entry,
 * one must hold the lock shared.  To read or update the counters within
 * an entry, one must hold the lock shared or exclusive (so the entry doesn't
 * disappear!) and also take the entry's mutex spinlock.
 * The shared state variable pgsrt->extent (the next free spot in the external
 * keys-text file) should be accessed only while holding either the
 * pgsrt->mutex spinlock, or exclusive lock on pgsrt->lock.  We use the mutex
 * to allow reserving file space while holding only shared lock on pgsrt->lock.
 * Rewriting the entire external keys-text file, eg for garbage collection,
 * requires holding pgsrt->lock exclusively; this allows individual entries
 * in the file to be read or written while holding only shared lock.
 *
 * Copyright (c) 2018, The PoWA-team
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <sys/stat.h>
#include <unistd.h>

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "access/hash.h"
#include "access/htup_details.h"
#if PG_VERSION_NUM >= 90600
#include "access/parallel.h"
#endif
#include "mb/pg_wchar.h"
#include "nodes/nodeFuncs.h"
#include "parser/parsetree.h"
#if PG_VERSION_NUM >= 90600
#include "postmaster/autovacuum.h"
#endif
#if PG_VERSION_NUM < 100000
#include "storage/fd.h"
#endif
#include "storage/ipc.h"
#include "storage/lwlock.h"
#include "storage/shmem.h"
#if PG_VERSION_NUM < 110000
#include "storage/spin.h"
#endif
#include "utils/builtins.h"
#include "utils/guc.h"
#if PG_VERSION_NUM < 110000
#include "utils/memutils.h"
#endif
#if PG_VERSION_NUM >= 90500
#include "utils/ruleutils.h"
#endif
#include "utils/tuplesort.h"

#include "include/pg_sortstats_import.h"

PG_MODULE_MAGIC;

/*--- Macros and structs ---*/

/* Location of permanent stats file (valid when database is shut down) */
#define PGSRT_DUMP_FILE	PGSTAT_STAT_PERMANENT_DIRECTORY "/pg_sortstats.stat"

/*
 * Location of external keys text file.  We don't keep it in the core
 * system's stats_temp_directory.  The core system can safely use that GUC
 * setting, because the statistics collector temp file paths are set only once
 * as part of changing the GUC, but pg_sortstats has no way of avoiding
 * race conditions.  Besides, we only expect modest, infrequent I/O for keys
 * strings, so placing the file on a faster filesystem is not compelling.
 */
#define PGSRT_TEXT_FILE	PG_STAT_TMP_DIR "/pgsrt_sortkey_texts.stat"

/* Magic number identifying the stats file format */
static const uint32 PGSRT_FILE_HEADER = 0x20180804;

/* PostgreSQL major version number, changes in which invalidate all entries */
static const uint32 PGSRT_PG_MAJOR_VERSION = PG_VERSION_NUM / 100;

#define PGSRT_COLUMNS		17			/* number of columns in pg_sortstats  SRF */
#define USAGE_DECREASE_FACTOR	(0.99)	/* decreased every pgsrt_entry_dealloc */
#define USAGE_DEALLOC_PERCENT	5		/* free this % of entries at once */
#define USAGE_INIT	(1.0)
#define ASSUMED_MEDIAN_INIT		(10.0)	/* initial assumed median usage */
#define ASSUMED_LENGTH_INIT		128		/* initial assumed mean keys length */

#define record_gc_ktexts() \
	do { \
		volatile pgsrtSharedState *s = (volatile pgsrtSharedState *) pgsrt; \
		SpinLockAcquire(&s->mutex); \
		s->gc_count++; \
		SpinLockRelease(&s->mutex); \
	} while(0)



/* In PostgreSQL 11, queryid becomes a uint64 internally.
 */
#if PG_VERSION_NUM >= 110000
typedef uint64 pgsrt_queryid;
#else
typedef uint32 pgsrt_queryid;
#endif

typedef struct pgsrtSharedState
{
	LWLockId		lock;				/* protects hashtable search/modification */
	double			cur_median_usage;	/* current median usage in hashtable */
	Size			mean_keys_len;		/* current mean keys text length */
	slock_t			mutex;				/* protects following fields only: */
	Size			extent;				/* current extent of keys file */
	int				n_writers;			/* number of active writers to keys file */
	int				gc_count;			/* keys file garbage collection cycle count */
#if PG_VERSION_NUM >= 90600
	LWLockId		queryids_lock;		/* protects following array */
	pgsrt_queryid	queryids[FLEXIBLE_ARRAY_MEMBER]; /* queryid of non-worker processes */
#endif
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
	int64			nbtapes;				/* total number of tapes used */
	int64			space_disk;				/* total disk space consumed */
	int64			space_memory;			/* total memory space consumed */
	int64			non_parallels;			/* number of non parallel sorts */
	int64			nb_workers;				/* total number of parallel workers (including gather node) */
} pgsrtCounters;

typedef struct pgsrtEntry
{
	pgsrtHashKey	key;
	pgsrtCounters	counters;		/* statistics for this sort */
	int				nbkeys;			/* # of columns in the sort */
	Size			keys_offset;	/* deparsed keys text offset in external file */
	int				keys_len;		/* # of valid bytes in deparsed keys string, or -1 */
	int				encoding;		/* deparsed keys text encoding */
	slock_t			mutex;			/* protects the counters only */
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
#if PG_VERSION_NUM >= 90600
static Size pgsrt_queryids_size(void);
static pgsrt_queryid pgsrt_get_queryid(void);
static void pgsrt_set_queryid(pgsrt_queryid);
#endif

static pgsrtEntry *pgsrt_entry_alloc(pgsrtHashKey *key, Size keys_offset,
		int keys_len, int encoding, int nbkeys);
static void pgsrt_entry_dealloc(void);
static void pgsrt_entry_reset(void);
static void pgsrt_store(pgsrt_queryid queryId, int nbkeys, char *keys,
		pgsrtCounters *counters);
static uint32 pgsrt_hash_fn(const void *key, Size keysize);
static int	pgsrt_match_fn(const void *key1, const void *key2, Size keysize);

static bool ktext_store(const char *keys, int keys_len, Size *keys_offset,
		int *gc_count);
static char *ktext_load_file(Size *buffer_size);
static char *ktext_fetch(Size keys_offset, int keys_len, char *buffer,
		Size buffer_size);
static bool need_gc_ktexts(void);
static void gc_ktexts(void);

static void pgsrt_process_sortstate(SortState *srtstate, pgsrtWalkerContext *context);
static bool pgsrt_planstate_walker(PlanState *ps, pgsrtWalkerContext *context);
static char * pgsrt_get_sort_group_keys(SortState *srtstate,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 pgsrtWalkerContext *context);
static void pgsrt_setup_walker_context(pgsrtWalkerContext *context);

static unsigned long round_up_pow2(int64 val);

/*--- Local variables ---*/
static int nesting_level = 0;

static bool pgsrt_enabled;
static int pgsrt_max;			/* max #of sorts to track */
static bool pgsrt_save;			/* whether to save stats across shutdown */

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
							10000,
							100,
							INT_MAX,
							PGC_POSTMASTER,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomBoolVariable("pg_sortstats.save",
							 "Save pg_sortstats statistics across server shutdowns.",
							 NULL,
							 &pgsrt_save,
							 true,
							 PGC_SIGHUP,
							 0,
							 NULL,
							 NULL,
							 NULL);

	EmitWarningsOnPlaceholders("pg_sortstats");

	/*
	 * Request additional shared resources.  (These are no-ops if we're not in
	 * the postmaster process.)  We'll allocate or attach to the shared
	 * resources in pgsrt_shmem_startup().
	 */
	RequestAddinShmemSpace(pgsrt_memsize());
#if PG_VERSION_NUM >= 90600
	RequestNamedLWLockTranche("pg_sortstats", 2);
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

static void
pgsrt_shmem_startup(void)
{
	bool		found;
	HASHCTL		info;
	FILE	   *file = NULL;
	FILE	   *kfile = NULL;
	uint32		header;
	int32		num;
	int32		pgver;
	int32		i;
	int			buffer_size;
	char	   *buffer = NULL;
	Size		tottextlen;
	int			nvalidtexts;

	if (prev_shmem_startup_hook)
		prev_shmem_startup_hook();

	/* reset in case this is a restart within the postmaster */
	pgsrt = NULL;

	/* Create or attach to the shared memory state */
	LWLockAcquire(AddinShmemInitLock, LW_EXCLUSIVE);

	/* global access lock */
	pgsrt = ShmemInitStruct("pg_sortstats",
					(sizeof(pgsrtSharedState)
#if PG_VERSION_NUM >= 90600
					+ pgsrt_queryids_size()
#endif
					),
					&found);

	if (!found)
	{
		/* First time through ... */
#if PG_VERSION_NUM >= 90600
		LWLockPadded *locks = GetNamedLWLockTranche("pg_sortstats");
		pgsrt->lock = &(locks[0]).lock;
		pgsrt->queryids_lock = &(locks[1]).lock;
		memset(pgsrt->queryids, 0, pgsrt_queryids_size());
#else
		pgsrt->lock = LWLockAssign();
#endif
		pgsrt->cur_median_usage = ASSUMED_MEDIAN_INIT;
		pgsrt->mean_keys_len = ASSUMED_LENGTH_INIT;
		SpinLockInit(&pgsrt->mutex);
		pgsrt->extent = 0;
		pgsrt->n_writers = 0;
		pgsrt->gc_count = 0;
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

	/*
	 * Note: we don't bother with locks here, because there should be no other
	 * processes running when this code is reached.
	 */

	/* Unlink keys text file possibly left over from crash */
	unlink(PGSRT_TEXT_FILE);

	/* Allocate new keys text temp file */
	kfile = AllocateFile(PGSRT_TEXT_FILE, PG_BINARY_W);
	if (kfile == NULL)
		goto write_error;

	/*
	 * If we were told not to load old statistics, we're done.  (Note we do
	 * not try to unlink any old dump file in this case.  This seems a bit
	 * questionable but it's the historical behavior.)
	 */
	if (!pgsrt_save)
	{
		FreeFile(kfile);
		return;
	}

	/*
	 * Attempt to load old statistics from the dump file.
	 */
	file = AllocateFile(PGSRT_DUMP_FILE, PG_BINARY_R);
	if (file == NULL)
	{
		if (errno != ENOENT)
			goto read_error;
		/* No existing persisted stats file, so we're done */
		FreeFile(kfile);
		return;
	}

	buffer_size = 2048;
	buffer = (char *) palloc(buffer_size);

	if (fread(&header, sizeof(uint32), 1, file) != 1 ||
		fread(&pgver, sizeof(uint32), 1, file) != 1 ||
		fread(&num, sizeof(int32), 1, file) != 1)
		goto read_error;

	if (header != PGSRT_FILE_HEADER ||
		pgver != PGSRT_PG_MAJOR_VERSION)
		goto data_error;

	tottextlen = 0;
	nvalidtexts = 0;

	for (i = 0; i < num; i++)
	{
		pgsrtEntry	temp;
		pgsrtEntry  *entry;
		Size		keys_offset;

		if (fread(&temp, sizeof(pgsrtEntry), 1, file) != 1)
			goto read_error;

		/* Encoding is the only field we can easily sanity-check */
		if (!PG_VALID_BE_ENCODING(temp.encoding))
			goto data_error;

		/* Resize buffer as needed */
		if (temp.keys_len >= buffer_size)
		{
			buffer_size = Max(buffer_size * 2, temp.keys_len + 1);
			buffer = repalloc(buffer, buffer_size);
		}

		if (fread(buffer, 1, temp.keys_len + 1, file) != temp.keys_len + 1)
			goto read_error;

		/* Should have a trailing null, but let's make sure */
		buffer[temp.keys_len] = '\0';

		/* Store the keys text */
		keys_offset = pgsrt->extent;
		if (fwrite(buffer, 1, temp.keys_len + 1, kfile) != temp.keys_len + 1)
			goto write_error;
		pgsrt->extent += temp.keys_len + 1;

		/* make the hashtable entry (discards old entries if too many) */
		entry = pgsrt_entry_alloc(&temp.key, keys_offset, temp.keys_len,
				temp.encoding, temp.nbkeys);

		/* In the mean length computation, ignore dropped texts. */
		if (entry->keys_len >= 0)
		{
			tottextlen += entry->keys_len + 1;
			nvalidtexts++;
		}

		/* copy in the actual stats */
		entry->counters = temp.counters;
	}

	if (nvalidtexts > 0)
		pgsrt->mean_keys_len = tottextlen / nvalidtexts;
	else
		pgsrt->mean_keys_len = ASSUMED_LENGTH_INIT;

	pfree(buffer);
	FreeFile(file);
	FreeFile(kfile);

	/*
	 * Remove the persisted stats file so it's not included in
	 * backups/replication slaves, etc.  A new file will be written on next
	 * shutdown.
	 *
	 * Note: it's okay if the PGSRT_TEXT_FILE is included in a basebackup,
	 * because we remove that file on startup; it acts inversely to
	 * PGSRT_DUMP_FILE, in that it is only supposed to be around when the
	 * server is running, whereas PGSRT_DUMP_FILE is only supposed to be around
	 * when the server is not running.  Leaving the file creates no danger of
	 * a newly restored database having a spurious record of execution costs,
	 * which is what we're really concerned about here.
	 */
	unlink(PGSRT_DUMP_FILE);

	return;

read_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not read file \"%s\": %m",
					PGSRT_DUMP_FILE)));
	goto fail;
data_error:
	ereport(LOG,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("ignoring invalid data in file \"%s\"",
					PGSRT_DUMP_FILE)));
	goto fail;
write_error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					PGSRT_TEXT_FILE)));
fail:
	if (buffer)
		pfree(buffer);
	if (file)
		FreeFile(file);
	if (kfile)
		FreeFile(kfile);
	/* If possible, throw away the bogus file; ignore any error */
	unlink(PGSRT_DUMP_FILE);

	/*
	 * Don't unlink PGSRT_TEXT_FILE here; it should always be around while the
	 * server is running with pg_sortstats enabled
	 */
}

/* Save the statistics into a file at shutdown */
static void
pgsrt_shmem_shutdown(int code, Datum arg)
{
	FILE	   *file;
	char	   *kbuffer = NULL;
	Size		kbuffer_size = 0;
	HASH_SEQ_STATUS hash_seq;
	int32		num_entries;
	pgsrtEntry  *entry;

	/* Don't try to dump during a crash. */
	if (code)
		return;

	/* Safety check ... shouldn't get here unless shmem is set up. */
	if (!pgsrt || !pgsrt_hash)
		return;

	/* Don't dump if told not to. */
	if (!pgsrt_save)
		return;

	file = AllocateFile(PGSRT_DUMP_FILE ".tmp", PG_BINARY_W);
	if (file == NULL)
		goto error;

	if (fwrite(&PGSRT_FILE_HEADER, sizeof(uint32), 1, file) != 1)
		goto error;
	if (fwrite(&PGSRT_PG_MAJOR_VERSION, sizeof(uint32), 1, file) != 1)
		goto error;
	num_entries = hash_get_num_entries(pgsrt_hash);
	if (fwrite(&num_entries, sizeof(int32), 1, file) != 1)
		goto error;

	kbuffer = ktext_load_file(&kbuffer_size);
	if (kbuffer == NULL)
		goto error;

	/*
	 * When serializing to disk, we store keys texts immediately after their
	 * entry data.  Any orphaned keys texts are thereby excluded.
	 */
	hash_seq_init(&hash_seq, pgsrt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			len = entry->keys_len;
		char	   *kstr = ktext_fetch(entry->keys_offset, len,
									   kbuffer, kbuffer_size);

		if (kstr == NULL)
			continue;			/* Ignore any entries with bogus texts */

		if (fwrite(entry, sizeof(pgsrtEntry), 1, file) != 1 ||
			fwrite(kstr, 1, len + 1, file) != len + 1)
		{
			/* note: we assume hash_seq_term won't change errno */
			hash_seq_term(&hash_seq);
			goto error;
		}
	}

	free(kbuffer);
	kbuffer = NULL;

	if (FreeFile(file))
	{
		file = NULL;
		goto error;
	}

	/*
	 * Rename file into place, so we atomically replace any old one.
	 */
	(void) durable_rename(PGSRT_DUMP_FILE ".tmp", PGSRT_DUMP_FILE, LOG);

	/* Unlink keys-texts file; it's not needed while shutdown */
	unlink(PGSRT_TEXT_FILE);

	return;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					PGSRT_DUMP_FILE ".tmp")));
	if (kbuffer)
		free(kbuffer);
	if (file)
		FreeFile(file);
	unlink(PGSRT_DUMP_FILE ".tmp");
	unlink(PGSRT_TEXT_FILE);
}

/*
 * Save this query's queryId if it's not a parallel worker
 */
static void
pgsrt_ExecutorStart(QueryDesc *queryDesc, int eflags)
{
#if PG_VERSION_NUM >= 90600
	if (pgsrt_enabled && !IsParallelWorker())
		pgsrt_set_queryid(queryDesc->plannedstmt->queryId);
#endif

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
	/* retrieve sorts informations, main work starts from here */
	if (pgsrt_enabled)
	{
		pgsrtWalkerContext context;

		context.queryDesc = queryDesc;
		context.ancestors = NIL;

		pgsrt_planstate_walker(queryDesc->planstate, &context);

#if PG_VERSION_NUM >= 90600
		/* Remove the saved queryid for safety */
		if (!IsParallelWorker())
			pgsrt_set_queryid(0);
#endif
	}

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
#if PG_VERSION_NUM >= 90600
	size = add_size(size, pgsrt_queryids_size());
#endif

	return size;
}

#if PG_VERSION_NUM >= 90600
/* Parallel workers won't have their queryid setup.  We store the leader
 * process' queryid in shared memory so that workers can find which queryid
 * they're actually executing.
 */
static Size
pgsrt_queryids_size(void)
{
	/* We need frrom for all possible backends, plus the autovacuum launcher
	 * and workers, plus the background workers, and an extra one since
	 * BackendId numerotation starts at 1.
	 */
#define PGSRT_NB_BACKEND_SLOT (MaxConnections \
			 + autovacuum_max_workers + 1 \
			 + max_worker_processes + 1)

	return MAXALIGN(sizeof(pgsrt_queryid) * PGSRT_NB_BACKEND_SLOT);
}

static pgsrt_queryid
pgsrt_get_queryid(void)
{
	pgsrt_queryid queryId;

	Assert(IsParallelWorker());
	Assert(MyBackendId <= PGSRT_NB_BACKEND_SLOT);

	LWLockAcquire(pgsrt->queryids_lock, LW_SHARED);
	queryId = pgsrt->queryids[ParallelMasterBackendId];
	LWLockRelease(pgsrt->queryids_lock);

	return queryId;
}

static void
pgsrt_set_queryid(pgsrt_queryid queryId)
{
	Assert(!IsParallelWorker());
	Assert(MyBackendId <= PGSRT_NB_BACKEND_SLOT);

	LWLockAcquire(pgsrt->queryids_lock, LW_EXCLUSIVE);
	pgsrt->queryids[MyBackendId] = queryId;
	LWLockRelease(pgsrt->queryids_lock);
}
#endif

/*
 * Allocate a new hashtable entry.
 * caller must hold an exclusive lock on pgsrt->lock
 */
static pgsrtEntry *
pgsrt_entry_alloc(pgsrtHashKey *key, Size keys_offset, int keys_len,
		int encoding, int nbkeys)
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
		/* re-initialize the mutex each time ... we assume no one using it */
		SpinLockInit(&entry->mutex);
		/* set non counters fields */
		Assert(keys_len >= 0);
		entry->nbkeys = nbkeys;
		entry->keys_offset = keys_offset;
		entry->keys_len = keys_len;
		entry->encoding = encoding;
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
 *
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
	Size		tottextlen;
	int			nvalidtexts;

	/*
	 * Sort entries by usage and deallocate USAGE_DEALLOC_PERCENT of them.
	 * While we're scanning the table, apply the decay factor to the usage
	 * values.
	 *
	 * Note that the mean query length is almost immediately obsolete, since
	 * we compute it before not after discarding the least-used entries.
	 * Hopefully, that doesn't affect the mean too much; it doesn't seem worth
	 * making two passes to get a more current result.  Likewise, the new
	 * cur_median_usage includes the entries we're about to zap.
	 */
	entries = palloc(hash_get_num_entries(pgsrt_hash) * sizeof(pgsrtEntry *));

	i = 0;
	tottextlen = 0;
	nvalidtexts = 0;

	hash_seq_init(&hash_seq, pgsrt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entries[i++] = entry;
		entry->counters.usage *= USAGE_DECREASE_FACTOR;
		/* In the mean length computation, ignore dropped texts. */
		if (entry->keys_len >= 0)
		{
			tottextlen += entry->keys_len + 1;
			nvalidtexts++;
		}
	}

	/* Sort into increasing order by usage */
	qsort(entries, i, sizeof(pgsrtEntry *), entry_cmp);

	/* Record the (approximate) median usage */
	if (i > 0)
		pgsrt->cur_median_usage = entries[i / 2]->counters.usage;
	/* Record the mean query length */
	if (nvalidtexts > 0)
		pgsrt->mean_keys_len = tottextlen / nvalidtexts;
	else
		pgsrt->mean_keys_len = ASSUMED_LENGTH_INIT;

	/* Now zap an appropriate fraction of lowest-usage entries */
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


/*
 * Store some statistics for a sort.
 */
static void
pgsrt_store(pgsrt_queryid queryId, int nbkeys, char *keys,
		pgsrtCounters *counters)
{
	volatile pgsrtEntry *e;
	pgsrtHashKey key;
	pgsrtEntry  *entry;

	Assert(keys != NULL);

	/* Safety check... */
	if (!pgsrt || !pgsrt_hash)
		return;

	/* Set up key for hashtable search */
	key.userid = GetUserId();
	key.dbid = MyDatabaseId;
	key.queryid = queryId;
	key.sortid = (uint32) hash_any((unsigned char *) keys,
			strlen(keys));

	/* Lookup the hash table entry with shared lock. */
	LWLockAcquire(pgsrt->lock, LW_SHARED);

	entry = (pgsrtEntry *) hash_search(pgsrt_hash, &key, HASH_FIND, NULL);

	/* Create new entry, if not present */
	if (!entry)
	{
		Size		keys_offset;
		int			keys_len = strlen(keys);
		int			gc_count;
		bool		stored;
		bool		do_gc;

		/* Append new keys text to file with only shared lock held */
		stored = ktext_store(keys, keys_len, &keys_offset, &gc_count);

		/*
		 * Determine whether we need to garbage collect external keys texts
		 * while the shared lock is still held.  This micro-optimization
		 * avoids taking the time to decide this while holding exclusive lock.
		 */
		do_gc = need_gc_ktexts();

		/* Need exclusive lock to make a new hashtable entry - promote */
		LWLockRelease(pgsrt->lock);
		LWLockAcquire(pgsrt->lock, LW_EXCLUSIVE);

		/*
		 * A garbage collection may have occurred while we weren't holding the
		 * lock.  In the unlikely event that this happens, the keys text we
		 * stored above will have been garbage collected, so write it again.
		 * This should be infrequent enough that doing it while holding
		 * exclusive lock isn't a performance problem.
		 */
		if (!stored || pgsrt->gc_count != gc_count)
			stored = ktext_store(keys, keys_len, &keys_offset, NULL);

		/* If we failed to write to the text file, give up */
		if (!stored)
			goto done;

		/* OK to create a new hashtable entry */
		entry = pgsrt_entry_alloc(&key, keys_offset, keys_len,
				GetDatabaseEncoding(), nbkeys);

		/* If needed, perform garbage collection while exclusive lock held */
		if (do_gc)
			gc_ktexts();
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
	e->counters.non_parallels += counters->non_parallels;
	e->counters.nb_workers += counters->nb_workers;

	SpinLockRelease(&e->mutex);

done:
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

/*
 * Given a keys string (not necessarily null-terminated), allocate a new
 * entry in the external keys text file and store the string there.
 *
 * If successful, returns true, and stores the new entry's offset in the file
 * into *keys_offset.  Also, if gc_count isn't NULL, *gc_count is set to the
 * number of garbage collections that have occurred so far.
 *
 * On failure, returns false.
 *
 * At least a shared lock on pgsrt->lock must be held by the caller, so as
 * to prevent a concurrent garbage collection.  Share-lock-holding callers
 * should pass a gc_count pointer to obtain the number of garbage collections,
 * so that they can recheck the count after obtaining exclusive lock to
 * detect whether a garbage collection occurred (and removed this entry).
 */
static bool
ktext_store(const char *keys, int keys_len,
			Size *keys_offset, int *gc_count)
{
	Size		off;
	int			fd;

	/*
	 * We use a spinlock to protect extent/n_writers/gc_count, so that
	 * multiple processes may execute this function concurrently.
	 */
	{
		volatile pgsrtSharedState *s = (volatile pgsrtSharedState *) pgsrt;

		SpinLockAcquire(&s->mutex);
		off = s->extent;
		s->extent += keys_len + 1;
		s->n_writers++;
		if (gc_count)
			*gc_count = s->gc_count;
		SpinLockRelease(&s->mutex);
	}

	*keys_offset = off;

	/* Now write the data into the successfully-reserved part of the file */
#if PG_VERSION_NUM < 110000
	fd = OpenTransientFile(PGSRT_TEXT_FILE, O_RDWR | O_CREAT | PG_BINARY,
			S_IRUSR | S_IWUSR);
#else
	fd = OpenTransientFile(PGSRT_TEXT_FILE, O_RDWR | O_CREAT | PG_BINARY);
#endif
	if (fd < 0)
		goto error;

	if (lseek(fd, off, SEEK_SET) != off)
		goto error;

	if (write(fd, keys, keys_len) != keys_len)
		goto error;
	if (write(fd, "\0", 1) != 1)
		goto error;

	CloseTransientFile(fd);

	/* Mark our write complete */
	{
		volatile pgsrtSharedState *s = (volatile pgsrtSharedState *) pgsrt;

		SpinLockAcquire(&s->mutex);
		s->n_writers--;
		SpinLockRelease(&s->mutex);
	}

	return true;

error:
	ereport(LOG,
			(errcode_for_file_access(),
			 errmsg("could not write file \"%s\": %m",
					PGSRT_TEXT_FILE)));

	if (fd >= 0)
		CloseTransientFile(fd);

	/* Mark our write complete */
	{
		volatile pgsrtSharedState *s = (volatile pgsrtSharedState *) pgsrt;

		SpinLockAcquire(&s->mutex);
		s->n_writers--;
		SpinLockRelease(&s->mutex);
	}

	return false;
}

/*
 * Read the external keys text file into a malloc'd buffer.
 *
 * Returns NULL (without throwing an error) if unable to read, eg
 * file not there or insufficient memory.
 *
 * On success, the buffer size is also returned into *buffer_size.
 *
 * This can be called without any lock on pgsrt->lock, but in that case
 * the caller is responsible for verifying that the result is sane.
 */
static char *
ktext_load_file(Size *buffer_size)
{
	char	   *buf;
	int			fd;
	struct stat stat;

#if PG_VERSION_NUM < 110000
	fd = OpenTransientFile(PGSRT_TEXT_FILE, O_RDONLY | PG_BINARY, 0);
#else
	fd = OpenTransientFile(PGSRT_TEXT_FILE, O_RDONLY | PG_BINARY);
#endif
	if (fd < 0)
	{
		if (errno != ENOENT)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							PGSRT_TEXT_FILE)));
		return NULL;
	}

	/* Get file length */
	if (fstat(fd, &stat))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not stat file \"%s\": %m",
						PGSRT_TEXT_FILE)));
		CloseTransientFile(fd);
		return NULL;
	}

	/* Allocate buffer; beware that off_t might be wider than size_t */
	if (stat.st_size <= MaxAllocHugeSize)
		buf = (char *) malloc(stat.st_size);
	else
		buf = NULL;
	if (buf == NULL)
	{
		ereport(LOG,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				 errmsg("out of memory"),
				 errdetail("Could not allocate enough memory to read file \"%s\".",
						   PGSRT_TEXT_FILE)));
		CloseTransientFile(fd);
		return NULL;
	}

	/*
	 * OK, slurp in the file.  If we get a short read and errno doesn't get
	 * set, the reason is probably that garbage collection truncated the file
	 * since we did the fstat(), so we don't log a complaint --- but we don't
	 * return the data, either, since it's most likely corrupt due to
	 * concurrent writes from garbage collection.
	 */
	errno = 0;
	if (read(fd, buf, stat.st_size) != stat.st_size)
	{
		if (errno)
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m",
							PGSRT_TEXT_FILE)));
		free(buf);
		CloseTransientFile(fd);
		return NULL;
	}

	CloseTransientFile(fd);

	*buffer_size = stat.st_size;
	return buf;
}

/*
 * Locate a keys text in the file image previously read by ktext_load_file().
 *
 * We validate the given offset/length, and return NULL if bogus.  Otherwise,
 * the result points to a null-terminated string within the buffer.
 */
static char *
ktext_fetch(Size keys_offset, int keys_len,
			char *buffer, Size buffer_size)
{
	/* File read failed? */
	if (buffer == NULL)
		return NULL;
	/* Bogus offset/length? */
	if (keys_len < 0 ||
		keys_offset + keys_len >= buffer_size)
		return NULL;
	/* As a further sanity check, make sure there's a trailing null */
	if (buffer[keys_offset + keys_len] != '\0')
		return NULL;
	/* Looks OK */
	return buffer + keys_offset;
}

/*
 * Do we need to garbage-collect the external keys text file?
 *
 * Caller should hold at least a shared lock on pgsrt->lock.
 */
static bool
need_gc_ktexts(void)
{
	Size		extent;

	/* Read shared extent pointer */
	{
		volatile pgsrtSharedState *s = (volatile pgsrtSharedState *) pgsrt;

		SpinLockAcquire(&s->mutex);
		extent = s->extent;
		SpinLockRelease(&s->mutex);
	}

	/* Don't proceed if file does not exceed 100 bytes per possible entry */
	if (extent < 100 * pgsrt_max)
		return false;

	/*
	 * Don't proceed if file is less than about 50% bloat.  Nothing can or
	 * should be done in the event of unusually large keys texts accounting
	 * for file's large size.  We go to the trouble of maintaining the mean
	 * keys length in order to prevent garbage collection from thrashing
	 * uselessly.
	 */
	if (extent < pgsrt->mean_keys_len * pgsrt_max * 2)
		return false;

	return true;
}

/*
 * Garbage-collect orphaned keys texts in external file.
 *
 * This won't be called often in the typical case, since it's likely that
 * there won't be too much churn, and besides, a similar compaction process
 * occurs when serializing to disk at shutdown or as part of resetting.
 * Despite this, it seems prudent to plan for the edge case where the file
 * becomes unreasonably large, with no other method of compaction likely to
 * occur in the foreseeable future.
 *
 * The caller must hold an exclusive lock on pgsrt->lock.
 *
 * At the first sign of trouble we unlink the keys text file to get a clean
 * slate (although existing statistics are retained), rather than risk
 * thrashing by allowing the same problem case to recur indefinitely.
 */
static void
gc_ktexts(void)
{
	char	   *kbuffer;
	Size		kbuffer_size;
	FILE	   *kfile = NULL;
	HASH_SEQ_STATUS hash_seq;
	pgsrtEntry  *entry;
	Size		extent;
	int			nentries;

	/*
	 * When called from pgsrt_store, some other session might have proceeded
	 * with garbage collection in the no-lock-held interim of lock strength
	 * escalation.  Check once more that this is actually necessary.
	 */
	if (!need_gc_ktexts())
		return;

	/*
	 * Load the old texts file.  If we fail (out of memory, for instance),
	 * invalidate keys texts.  Hopefully this is rare.  It might seem better
	 * to leave things alone on an OOM failure, but the problem is that the
	 * file is only going to get bigger; hoping for a future non-OOM result is
	 * risky and can easily lead to complete denial of service.
	 */
	kbuffer = ktext_load_file(&kbuffer_size);
	if (kbuffer == NULL)
		goto gc_fail;

	/*
	 * We overwrite the keys texts file in place, so as to reduce the risk of
	 * an out-of-disk-space failure.  Since the file is guaranteed not to get
	 * larger, this should always work on traditional filesystems; though we
	 * could still lose on copy-on-write filesystems.
	 */
	kfile = AllocateFile(PGSRT_TEXT_FILE, PG_BINARY_W);
	if (kfile == NULL)
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						PGSRT_TEXT_FILE)));
		goto gc_fail;
	}

	extent = 0;
	nentries = 0;

	hash_seq_init(&hash_seq, pgsrt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		int			keys_len = entry->keys_len;
		char	   *qry = ktext_fetch(entry->keys_offset,
									  keys_len,
									  kbuffer,
									  kbuffer_size);

		if (qry == NULL)
		{
			/* Trouble ... drop the text */
			entry->keys_offset = 0;
			entry->keys_len = -1;
			/* entry will not be counted in mean keys length computation */
			continue;
		}

		if (fwrite(qry, 1, keys_len + 1, kfile) != keys_len + 1)
		{
			ereport(LOG,
					(errcode_for_file_access(),
					 errmsg("could not write file \"%s\": %m",
							PGSRT_TEXT_FILE)));
			hash_seq_term(&hash_seq);
			goto gc_fail;
		}

		entry->keys_offset = extent;
		extent += keys_len + 1;
		nentries++;
	}

	/*
	 * Truncate away any now-unused space.  If this fails for some odd reason,
	 * we log it, but there's no need to fail.
	 */
	if (ftruncate(fileno(kfile), extent) != 0)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not truncate file \"%s\": %m",
						PGSRT_TEXT_FILE)));

	if (FreeFile(kfile))
	{
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m",
						PGSRT_TEXT_FILE)));
		kfile = NULL;
		goto gc_fail;
	}

	elog(DEBUG1, "pgsrt gc of keys file shrunk size from %zu to %zu",
		 pgsrt->extent, extent);

	/* Reset the shared extent pointer */
	pgsrt->extent = extent;

	/*
	 * Also update the mean keys length, to be sure that need_gc_ktexts()
	 * won't still think we have a problem.
	 */
	if (nentries > 0)
		pgsrt->mean_keys_len = extent / nentries;
	else
		pgsrt->mean_keys_len = ASSUMED_LENGTH_INIT;

	free(kbuffer);

	/*
	 * OK, count a garbage collection cycle.  (Note: even though we have
	 * exclusive lock on pgsrt->lock, we must take pgsrt->mutex for this, since
	 * other processes may examine gc_count while holding only the mutex.
	 * Also, we have to advance the count *after* we've rewritten the file,
	 * else other processes might not realize they read a stale file.)
	 */
	record_gc_ktexts();

	return;

gc_fail:
	/* clean up resources */
	if (kfile)
		FreeFile(kfile);
	if (kbuffer)
		free(kbuffer);

	/*
	 * Since the contents of the external file are now uncertain, mark all
	 * hashtable entries as having invalid texts.
	 */
	hash_seq_init(&hash_seq, pgsrt_hash);
	while ((entry = hash_seq_search(&hash_seq)) != NULL)
	{
		entry->keys_offset = 0;
		entry->keys_len = -1;
	}

	/*
	 * Destroy the keys text file and create a new, empty one
	 */
	(void) unlink(PGSRT_TEXT_FILE);
	kfile = AllocateFile(PGSRT_TEXT_FILE, PG_BINARY_W);
	if (kfile == NULL)
		ereport(LOG,
				(errcode_for_file_access(),
				 errmsg("could not recreate file \"%s\": %m",
						PGSRT_TEXT_FILE)));
	else
		FreeFile(kfile);

	/* Reset the shared extent pointer */
	pgsrt->extent = 0;

	/* Reset mean_keys_len to match the new state */
	pgsrt->mean_keys_len = ASSUMED_LENGTH_INIT;

	/*
	 * Bump the GC count even though we failed.
	 *
	 * This is needed to make concurrent readers of file without any lock on
	 * pgsrt->lock notice existence of new version of file.  Once readers
	 * subsequently observe a change in GC count with pgsrt->lock held, that
	 * forces a safe reopen of file.  Writers also require that we bump here,
	 * of course.  (As required by locking protocol, readers and writers don't
	 * trust earlier file contents until gc_count is found unchanged after
	 * pgsrt->lock acquired in shared or exclusive mode respectively.)
	 */
	record_gc_ktexts();
}

static void
pgsrt_process_sortstate(SortState *srtstate, pgsrtWalkerContext *context)
{
	Plan *plan = srtstate->ss.ps.plan;
	Tuplesortstate *state = (Tuplesortstate *) srtstate->tuplesortstate;
#if PG_VERSION_NUM >= 110000
	TuplesortInstrumentation stats;
#endif
	Sort *sort = (Sort *) plan;
	pgsrt_queryid queryId;
	pgsrtCounters counters;
	char *deparsed;
	int nbtapes = 0;
#if PG_VERSION_NUM < 110000
	const char *sortMethod;
	const char *spaceType;
#endif
	long		spaceUsed;
	bool found;
	int memtupsize_palloc;		/* tuplesort's main storage total size,
								   including palloc overhead */
	int tuple_palloc;			/* average tuple size, including palloc overhead */
	int64 lines,				/* number of lines underlying node returned */
		  lines_to_sort,		/* number of lines the sort will actually
								   process (may differ when bounded) */
		  memtupsize_length,	/* tuplesort's main storage array size */
		  w_m;					/* estimated work_mem */

	Assert(state);

	/* First estimate the size of the main array that stores the lines */
	lines = 0;
	/* get effective number of lines fed to the sort if available */
	if (srtstate->ss.ps.instrument)
		lines = srtstate->ss.ps.instrument->ntuples;

	/* fallback to estimated # of lines if no value */
	if (lines == 0)
		lines = sort->plan.plan_rows;

	/*
	 * If the sort is bounded, set the number of lines to sort
	 * accordingly, otherwise use the Sort input lines count.
	 */
	if (srtstate->bounded)
		lines_to_sort = srtstate->bound;
	else
		lines_to_sort = lines;

	/* The minimal memtupsize is 1024 */
	memtupsize_length = Max(1024, lines_to_sort);
	/*
	 * growth is done by doubling the size each time with a minimum of 1024
	 * entries, so we'll have a power of 2.  No need to deal with the the last
	 * growth special rule, there's no way we can exhaust the work_mem for the
	 * main array and still put all the rows to sort in memory
	 */
	memtupsize_length = round_up_pow2(memtupsize_length);

	/* compute the memtupsize palloc size */
	memtupsize_palloc = sizeof(SortTuple) * memtupsize_length;
	memtupsize_palloc += PGSRT_ALLOC_CHUNKHDRSZ;

	/*
	 * Then estimate the per-line space used.  We use the average row width,
	 * and add the fixed MnimalTuple header overhead
	 * FIXME: take into account NULLs, OIDs and alignment lost bytes
	 */
	tuple_palloc = sort->plan.plan_width + MAXALIGN(SizeofMinimalTupleHeader);

	/*
	 * Each tuple is palloced, and a palloced chunk uses a 2^N size unless size
	 * is more then PGSRT_ALLOC_CHUNK_LIMIT
	 */
	if (tuple_palloc < PGSRT_ALLOC_CHUNK_LIMIT)
		tuple_palloc = round_up_pow2(tuple_palloc);

	/* Add the palloc overhead */
	tuple_palloc += PGSRT_ALLOC_CHUNKHDRSZ;

	/*
	 * compute the estimated total work_mem that's needed to perform the
	 * sort in memory.  First add the space needed for the lines
	 */
	w_m = lines_to_sort * tuple_palloc;

	/*
	 * If a bounded sort was asked, we'll try to sort only the bound limit
	 * number of line, but a Top-N heapsort may need to be able to store twice
	 * the amount of rows, so use twice the memory, assuming that the worst
	 * case always happens.
	 */
	if (srtstate->bounded)
		w_m *= 2;

	/* add the tuplesort's main array storage and we're done */
	w_m += memtupsize_palloc;

	/* convert in kB, and add 1 kB as a quick round up */
	w_m /= 1024;
	w_m += 1;

	/* deparse the sort keys */
	deparsed = pgsrt_get_sort_group_keys(srtstate, sort->numCols,
			sort->sortColIdx, sort->sortOperators, sort->collations,
			sort->nullsFirst, context);

#if PG_VERSION_NUM >= 110000
	tuplesort_get_stats(state, &stats);
	//sortMethod = tuplesort_method_name(stats.sortMethod);
	//spaceType = tuplesort_space_type_name(stats.spaceType);
	spaceUsed = stats.spaceUsed;
#else
	tuplesort_get_stats(state, &sortMethod, &spaceType, &spaceUsed);
#endif

	counters.lines = lines;
	counters.lines_to_sort = lines_to_sort;
	counters.work_mems = w_m;
	found = false;
#if PG_VERSION_NUM >= 110000
	if (stats.sortMethod == SORT_TYPE_TOP_N_HEAPSORT)
#else
	if (strcmp(sortMethod, "top-N heapsort") == 0)
#endif
	{
		counters.topn_sorts = 1;
		found = true;
	}
	else
		counters.topn_sorts = 0;

#if PG_VERSION_NUM >= 110000
	if (stats.sortMethod == SORT_TYPE_QUICKSORT)
#else
	if (!found && strcmp(sortMethod, "quicksort") == 0)
#endif
	{
		counters.quicksorts = 1;
		found = true;
	}
	else
		counters.quicksorts = 0;

#if PG_VERSION_NUM >= 110000
	if (stats.sortMethod == SORT_TYPE_EXTERNAL_SORT)
#else
	if (!found && strcmp(sortMethod, "external sort") == 0)
#endif
	{
		counters.external_sorts = 1;
		found = true;
	}
	else
		counters.external_sorts = 0;

#if PG_VERSION_NUM >= 110000
	if (stats.sortMethod == SORT_TYPE_EXTERNAL_MERGE)
#else
	if (!found && strcmp(sortMethod, "external merge") == 0)
#endif
	{
		counters.external_merges = 1;
		nbtapes = ((struct pgsrt_Tuplesortstate *) state)->currentRun + 1;
		found = true;
	}
	else
		counters.external_merges = 0;

	Assert(found);

	counters.nbtapes = nbtapes;

#if PG_VERSION_NUM >= 110000
	if (stats.spaceType == SORT_SPACE_TYPE_DISK)
#else
	if (strcmp(spaceType, "Disk") == 0)
#endif
	{
		counters.space_disk = spaceUsed;
		counters.space_memory = 0;
	}
	else
	{
		counters.space_disk = 0;
		counters.space_memory = spaceUsed;
	}

#if PG_VERSION_NUM >= 110000
	if (srtstate->shared_info){
		counters.non_parallels = 0;
		/*
		 * we compute the total number of processes participating to the sort,
		 * so we have to increment the number of workers to take the gather
		 * node into account
		 */
		counters.nb_workers = srtstate->shared_info->num_workers + 1;
	}
	else
	{
		counters.non_parallels = 1;
		counters.nb_workers = 0;
	}
#else
	counters.non_parallels = 1;
	counters.nb_workers = 0;
#endif

#if PG_VERSION_NUM >= 90600
	if (IsParallelWorker())
		queryId = pgsrt_get_queryid();
	else
		queryId = context->queryDesc->plannedstmt->queryId;
#else
	queryId = context->queryDesc->plannedstmt->queryId;
#endif

	pgsrt_store(queryId, sort->numCols, deparsed, &counters);

	//elog(WARNING, "sort info:\n"
	//		"keys: %s\n"
	//		"type: %s\n"
	//		"space type: %s\n"
	//		"space: %ld kB\n"
	//		"lines to sort: %ld\n"
	//		"w_m estimated: %ld kB\n"
	//		"nbTapes: %d\n"
#if PG_VERSION_NUM >= 110000
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
#if PG_VERSION_NUM >= 110000
	//		(srtstate->shared_info ? "yes" : "no"),(srtstate->shared_info ? srtstate->shared_info->num_workers : -1),
#endif
	//		(srtstate->bounded ? "yes":"no"),(srtstate->bounded_Done ? "yes":"no"), srtstate->bound, srtstate->bound_Done);
}

/*
 * walker functions that recurse the planstate tree looking for sort nodes.
 */
static bool pgsrt_planstate_walker(PlanState *ps, pgsrtWalkerContext *context)
{
	if (IsA(ps, SortState))
	{
		SortState *srtstate = (SortState *) ps;

		if (srtstate->tuplesortstate)
			pgsrt_process_sortstate(srtstate, context);
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
pgsrt_get_sort_group_keys(SortState *srtstate,
					 int nkeys, AttrNumber *keycols,
					 Oid *sortOperators, Oid *collations, bool *nullsFirst,
					 pgsrtWalkerContext *context)
{
	Plan	   *plan = srtstate->ss.ps.plan;
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
											(Node *) srtstate,
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
	bool			showtext = PG_GETARG_BOOL(0);
	ReturnSetInfo  *rsinfo = (ReturnSetInfo *) fcinfo->resultinfo;
	MemoryContext	per_query_ctx;
	MemoryContext	oldcontext;
	TupleDesc		tupdesc;
	Tuplestorestate	*tupstore;
	char		   *kbuffer = NULL;
	Size			kbuffer_size = 0;
	Size			extent = 0;
	int				gc_count = 0;
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

	/*
	 * We'd like to load the keys text file (if needed) while not holding any
	 * lock on pgsrt->lock.  In the worst case we'll have to do this again
	 * after we have the lock, but it's unlikely enough to make this a win
	 * despite occasional duplicated work.  We need to reload if anybody
	 * writes to the file (either a retail ktext_store(), or a garbage
	 * collection) between this point and where we've gotten shared lock.  If
	 * a ktext_store is actually in progress when we look, we might as well
	 * skip the speculative load entirely.
	 */
	if (showtext)
	{
		int			n_writers;

		/* Take the mutex so we can examine variables */
		{
			volatile pgsrtSharedState *s = (volatile pgsrtSharedState *) pgsrt;

			SpinLockAcquire(&s->mutex);
			extent = s->extent;
			n_writers = s->n_writers;
			gc_count = s->gc_count;
			SpinLockRelease(&s->mutex);
		}

		/* No point in loading file now if there are active writers */
		if (n_writers == 0)
			kbuffer = ktext_load_file(&kbuffer_size);
	}

	/*
	 * Get shared lock, load or reload the keys text file if we must, and
	 * iterate over the hashtable entries.
	 *
	 * With a large hash table, we might be holding the lock rather longer
	 * than one could wish.  However, this only blocks creation of new hash
	 * table entries, and the larger the hash table the less likely that is to
	 * be needed.  So we can hope this is okay.  Perhaps someday we'll decide
	 * we need to partition the hash table to limit the time spent holding any
	 * one lock.
	 */
	LWLockAcquire(pgsrt->lock, LW_SHARED);

	if (showtext)
	{
		/*
		 * Here it is safe to examine extent and gc_count without taking the
		 * mutex.  Note that although other processes might change
		 * pgsrt->extent just after we look at it, the strings they then write
		 * into the file cannot yet be referenced in the hashtable, so we
		 * don't care whether we see them or not.
		 *
		 * If ktext_load_file fails, we just press on; we'll return NULL for
		 * every keys text.
		 */
		if (kbuffer == NULL ||
			pgsrt->extent != extent ||
			pgsrt->gc_count != gc_count)
		{
			if (kbuffer)
				free(kbuffer);
			kbuffer = ktext_load_file(&kbuffer_size);
		}
	}

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

		values[i++] = Int64GetDatumFast(entry->key.queryid);
		values[i++] = ObjectIdGetDatum(entry->key.userid);
		values[i++] = ObjectIdGetDatum(entry->key.dbid);
		values[i++] = Int32GetDatum(entry->nbkeys);
		if (showtext)
		{
			char	   *kstr = ktext_fetch(entry->keys_offset,
					entry->keys_len,
					kbuffer,
					kbuffer_size);

			if (kstr)
			{
				char	   *enc;

				enc = pg_any_to_server(kstr,
						entry->keys_len,
						entry->encoding);

				values[i++] = CStringGetTextDatum(enc);

				if (enc != kstr)
					pfree(enc);
				}
				else
				{
					/* Just return a null if we fail to find the text */
					nulls[i++] = true;
				}
		}
		else
		{
			/* keys text not requested */
			nulls[i++] = true;
		}
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
		values[i++] = Int64GetDatumFast(tmp.non_parallels);
#if PG_VERSION_NUM >= 110000
		values[i++] = Int64GetDatumFast(tmp.nb_workers);
#else
		nulls[i++] = true;
#endif

		Assert(i == PGSRT_COLUMNS);

		tuplestore_putvalues(tupstore, tupdesc, values, nulls);
	}

	/* clean up and return the tuplestore */
	LWLockRelease(pgsrt->lock);

	if (kbuffer)
		free(kbuffer);

	tuplestore_donestoring(tupstore);
	return (Datum) 0;
}

static unsigned long
round_up_pow2(int64 val)
{
	val--;
	val |= val >> 1;
	val |= val >> 2;
	val |= val >> 4;
	val |= val >> 8;
	val |= val >> 16;
	val++;
	return val;
}
