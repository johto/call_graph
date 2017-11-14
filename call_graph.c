/* -------------------------------------------------------------------------
 *
 * call_graph.c
 *
 * -------------------------------------------------------------------------
 */
#include "postgres.h"

#include "port.h"
#include "fmgr.h"
#include "access/xact.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/tqual.h"
#include "utils/memutils.h"
#include "utils/regproc.h"
#include "storage/shmem.h"
#include "catalog/pg_type.h"
#include "catalog/namespace.h"
#include "commands/sequence.h"
#include "executor/spi.h"
#include "portability/instr_time.h"

/*
 * When enabled, this module keeps track of all the edges it has seen in a
 * single call graph.  The edges are stored in a local hash table, which also
 * stores how many times an edge has been called in this particular call graph,
 * and how much time has been spent in a single edge.  Before exiting the top
 * level function of the call graph, we store all our data into a "buffer"
 * table inside the database.
 *
 * Because of the fact that we might get enabled in the middle of a call graph,
 * we can't simply stop tracking when the module is disabled.  However, there's
 * no need to keep track of the full call stack; just track how many times we've
 * recursed into the top level function.
 */

PG_MODULE_MAGIC;

void		_PG_init(void);

typedef struct {
	Oid caller;
	Oid callee;
} EdgeHashKey;

typedef struct {
	EdgeHashKey key;
	int num_calls;
	instr_time self_time;
	instr_time total_time;

	/* temporary variable to keep track of the total time */
	instr_time total_time_start;
} EdgeHashElem;


typedef struct {
	Oid relid;
} TableStatHashKey;

typedef struct {
	TableStatHashKey key;

	int64 seq_scan;
	int64 seq_tup_read;

	int64 idx_scan;
	int64 idx_tup_fetch;

	int64 n_tup_ins;
	int64 n_tup_upd;
	int64 n_tup_del;
} TableStatHashElem;

/* hash_table should be NULL if num_tables == 0 */
typedef struct {
	HTAB *hash_table;
	int num_tables;
} TableStatSnapshot;


static bool enable_call_graph = false;
static bool track_table_usage = false;

static needs_fmgr_hook_type next_needs_fmgr_hook = NULL;
static fmgr_hook_type next_fmgr_hook = NULL;

static HTAB *edge_hash_table = NULL;

static List *call_stack = NULL;
static Oid top_level_function_oid = InvalidOid;

static bool tracking_current_graph = false;
static int recursion_depth = 0;

/*
 * Table stat snapshot taken at top level function entry and freed at function
 * exit.  Allows us to track table usage.
 */
static TableStatSnapshot *table_stat_snapshot;

static instr_time current_self_time_start; /* we only need one variable to keep track of all self_times */

static
TableStatSnapshot *get_table_stat_snapshot()
{
	int ret;
	SPIPlanPtr planptr;
	HASHCTL ctl;
	TableStatSnapshot *snapshot;

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "could not connect to the SPI: %d", ret);

	planptr = SPI_prepare("SELECT																				"
						  "   relid, seq_scan, seq_tup_read,													"
						  /* idx_* columns might be NULL if there are no indexes on the table */
						  "	  COALESCE(idx_scan, 0), COALESCE(idx_tup_fetch, 0),								"
						  "   n_tup_ins, n_tup_upd, n_tup_del													"
						  "FROM																					"
						  "   pg_stat_xact_user_tables															"
						  "WHERE																				"
						  "   relid <> 'call_graph.TableAccessBuffer'::regclass AND								"
						  "   relid <> 'call_graph.CallGraphBuffer'::regclass AND								"
						  "   GREATEST(seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del) > 0					",
						  0, NULL);

	if (!planptr)
		elog(ERROR, "could not prepare an SPI plan");

	ret = SPI_execp(planptr, NULL, NULL, 0);
	if (ret < 0)
		elog(ERROR, "SPI_execp() failed: %d", ret);

	/*
	 * We need to use TopTransactionContext explicitly for any allocations or else
	 * our memory will disappear after we call SPI_finish().
	 */
	snapshot = MemoryContextAlloc(TopTransactionContext, sizeof(TableStatSnapshot));

	/* create the hash table */
	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(TableStatHashKey);
	ctl.entrysize = sizeof(TableStatHashElem);
	ctl.hash = tag_hash;
	/* use TopTransactionContext for the hash table */
	ctl.hcxt = TopTransactionContext;
	snapshot->hash_table = hash_create("snapshot_hash_table", 32, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
	if (ret > 0)
	{
		SPITupleTable *tuptable;
		TupleDesc tupdesc;
		int i;
		int proc;

		tuptable = SPI_tuptable;
		if (!tuptable)
			elog(ERROR, "SPI_tuptable == NULL");
		tupdesc = tuptable->tupdesc;

		proc = SPI_processed;
		for (i = 0; i < proc; ++i)
		{
			HeapTuple tuple = tuptable->vals[i];
			bool isnull;
			bool found;
			TableStatHashKey key;
			TableStatHashElem* elem;

			key.relid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			Assert(!isnull);

			elem = hash_search(snapshot->hash_table, (void *) &key, HASH_ENTER, &found);
			if (found)
				elog(ERROR, "oops");
			else
			{
				elem->key = key;

				elem->seq_scan = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 2, &found));
				elem->seq_tup_read = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 3, &found));
				elem->idx_scan = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 4, &found));
				elem->idx_tup_fetch = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 5, &found));
				elem->n_tup_ins = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 6, &found));
				elem->n_tup_upd = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 7, &found));
				elem->n_tup_del = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 8, &found));
			}
		}

		snapshot->num_tables = proc;
	}

	SPI_finish();

	/* freeze the hash table; nobody's going to modify it anymore */
	hash_freeze(snapshot->hash_table);

	return snapshot;
}

static
void insert_snapshot_delta(Datum callgraph_buffer_id, TableStatSnapshot *snapshot)
{
	int ret;
	SPIPlanPtr planptr;

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "could not connect to the SPI: %d", ret);

	planptr = SPI_prepare("SELECT																				"
						  "   relid, seq_scan, seq_tup_read,													"
						  /* idx_* columns might be NULL if there are no indexes on the table */
						  "	  COALESCE(idx_scan, 0), COALESCE(idx_tup_fetch, 0),								"
						  "   n_tup_ins, n_tup_upd, n_tup_del													"
						  "FROM																					"
						  "   pg_stat_xact_user_tables															"
						  "WHERE																				"
						  "   relid <> 'call_graph.TableAccessBuffer'::regclass AND								"
						  "   relid <> 'call_graph.CallGraphBuffer'::regclass AND								"
						  "   GREATEST(seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del) > 0					",
						  0, NULL);

	if (!planptr)
		elog(ERROR, "could not prepare an SPI plan");

	ret = SPI_execp(planptr, NULL, NULL, 0);
	if (ret != SPI_OK_SELECT)
		elog(ERROR, "SPI_execp() failed: %d", ret);

	if (SPI_processed > 0)
	{
		SPITupleTable *tuptable;
		SPIPlanPtr insertplanptr;
		TupleDesc tupdesc;
		int i;
		int proc;

		Oid argtypes[] = { INT8OID, OIDOID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, INT8OID, InvalidOid };
		Datum args[9];

		args[0] = callgraph_buffer_id;

		tuptable = SPI_tuptable;
		if (!tuptable)
			elog(ERROR, "SPI_tuptable == NULL");
		tupdesc = tuptable->tupdesc;

		insertplanptr = SPI_prepare("INSERT INTO																			"
									"   call_graph.TableAccessBuffer (CallGraphBufferID, relid, seq_scan, seq_tup_read,		"
									"								  idx_scan, idx_tup_read,								"
									"								  n_tup_ins, n_tup_upd, n_tup_del)						"
									"		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)										",
									9, argtypes);

		if (!insertplanptr)
			elog(ERROR, "could not prepare an SPI plan");

		proc = SPI_processed;
		for (i = 0; i < proc; ++i)
		{
			HeapTuple tuple = tuptable->vals[i];
			bool isnull;
			bool found;
			Oid relid;
			int64 seq_scan, seq_tup_read,
				  idx_scan, idx_tup_fetch,
				  n_tup_ins, n_tup_upd, n_tup_del;

			relid = DatumGetObjectId(SPI_getbinval(tuple, tupdesc, 1, &isnull));
			Assert(!isnull);

			seq_scan = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 2, &isnull));
			seq_tup_read = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 3, &isnull));
			idx_scan = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 4, &isnull));
			idx_tup_fetch = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 5, &isnull));
			n_tup_ins = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 6, &isnull));
			n_tup_upd = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 7, &isnull));
			n_tup_del = DatumGetInt64(SPI_getbinval(tuple, tupdesc, 8, &isnull));			

			/* If the snapshot isn't empty, calculate deltas */
			if (snapshot->num_tables > 0)
			{
				TableStatHashKey key;
				TableStatHashElem *elem;

				key.relid = relid;

				elem = hash_search(snapshot->hash_table, (void *) &key, HASH_FIND, &found);
				if (found)
				{
					seq_scan -= elem->seq_scan;
					seq_tup_read -= elem->seq_tup_read;
					idx_scan -= elem->idx_scan;
					idx_tup_fetch -= elem->idx_tup_fetch;
					n_tup_ins -= elem->n_tup_ins;
					n_tup_upd -= elem->n_tup_upd;
					n_tup_del -= elem->n_tup_del;

					/* If there was no change to the previous snapshot, skip this table */
					if (seq_scan == 0 && idx_scan == 0 &&
						n_tup_ins == 0 && n_tup_upd == 0 && n_tup_del == 0)
						continue;
				}
			}

			args[1] = ObjectIdGetDatum(relid);
			args[2] = Int64GetDatum(seq_scan);
			args[3] = Int64GetDatum(seq_tup_read);
			args[4] = Int64GetDatum(idx_scan);
			args[5] = Int64GetDatum(idx_tup_fetch);
			args[6] = Int64GetDatum(n_tup_ins);
			args[7] = Int64GetDatum(n_tup_upd);
			args[8] = Int64GetDatum(n_tup_del);

			if ((ret = SPI_execp(insertplanptr, args, NULL, 0)) != SPI_OK_INSERT)
				elog(ERROR, "SPI_execp() failed: %d", ret);
		}
	}

	SPI_finish();
}

static
void release_table_stat_snapshot(TableStatSnapshot *snapshot)
{
	if (snapshot->hash_table)
		hash_destroy(snapshot->hash_table);
	else
		Assert(snapshot->num_tables == 0);

	pfree(snapshot);
}

static bool
call_graph_needs_fmgr_hook(Oid functionId)
{
	/* our hook needs to always be called to keep track of the call stack */
	return true;
}

static void create_edge_hash_table()
{
	HASHCTL ctl;

	Assert(edge_hash_table == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(EdgeHashKey);
	ctl.entrysize = sizeof(EdgeHashElem);
	ctl.hash = tag_hash;
	/* use TopTransactionContext for the hash table */
	ctl.hcxt = TopTransactionContext;

	edge_hash_table = hash_create("call_graph_edge_hash_table", 128, &ctl, HASH_ELEM | HASH_FUNCTION | HASH_CONTEXT);
}

static void destroy_edge_hash_table()
{
	Assert(edge_hash_table != NULL);

	hash_destroy(edge_hash_table);
	edge_hash_table = NULL;
}

static Datum assign_callgraph_buffer_id()
{
	List *names;
	Oid seqoid;

	names = stringToQualifiedNameList("call_graph.seqCallGraphBuffer");

#if PG_VERSION_NUM >= 90200
	seqoid = RangeVarGetRelid(makeRangeVarFromNameList(names), NoLock, false);
#else
	seqoid = RangeVarGetRelid(makeRangeVarFromNameList(names), false);
#endif

	return DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(seqoid));
}

static void process_edge_data(Datum callgraph_buffer_id)
{
	int ret;
	HASH_SEQ_STATUS hst;
	EdgeHashElem *elem;
	SPIPlanPtr planptr;
	Datum args[7];
	Oid argtypes[] = { INT8OID, OIDOID, OIDOID, OIDOID, INT8OID, FLOAT8OID, FLOAT8OID, InvalidOid };

	/* Start by freezing the hash table.  This saves us some trouble. */
	hash_freeze(edge_hash_table);

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "could not connect to the SPI: %d", ret);

	planptr = SPI_prepare("INSERT INTO 																				"
						  "   call_graph.CallGraphBuffer (CallGraphBufferID, TopLevelFunction, Caller, Callee,		"
						  "								  Calls, TotalTime, SelfTime)								"
						  "   VALUES ($1, $2, $3, $4, $5, $6, $7)													",
						  7, argtypes);
	if (!planptr)
		elog(ERROR, "could not prepare an SPI plan for the INSERT into CallGraphBuffer");

	args[0] = callgraph_buffer_id;
	args[1] = ObjectIdGetDatum(top_level_function_oid);

	hash_seq_init(&hst, edge_hash_table);
	while ((elem = hash_seq_search(&hst)) != NULL)
	{
		args[2] = ObjectIdGetDatum(elem->key.caller);
		args[3] = ObjectIdGetDatum(elem->key.callee);
		args[4] = Int8GetDatum(elem->num_calls);
		args[5] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(elem->total_time));
		args[6] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(elem->self_time));

		if ((ret = SPI_execp(planptr, args, NULL, 0)) < 0)
			elog(ERROR, "SPI_execp() failed: %d", ret);
	}

	SPI_finish();
}

static void
call_graph_fmgr_hook(FmgrHookEventType event,
			  FmgrInfo *flinfo, Datum *args)
{
	bool aborted = false;
	EdgeHashKey key;
	EdgeHashElem *elem;
	instr_time current_time;

	if (next_fmgr_hook)
		(*next_fmgr_hook) (event, flinfo, args);

	INSTR_TIME_SET_CURRENT(current_time);

	switch (event)
	{
		case FHET_START:
		{
			bool found;

			if (call_stack == NIL)
			{
				top_level_function_oid = flinfo->fn_oid;

				/* We're about to enter the top level function; check whether we've been disabled */
				if (!enable_call_graph)
				{
					tracking_current_graph = false;
					recursion_depth = 1;
					return;
				}

				/* Start tracking the call graph; we need to create the hash table */
				create_edge_hash_table();
				tracking_current_graph = true;

				/* If we're tracking table usage, take a stat snapshot now */
				if (track_table_usage)
					table_stat_snapshot = get_table_stat_snapshot();

				/* Use InvalidOid for the imaginary edge into the top level function */
				key.caller = InvalidOid;
			}
			else
			{
				if (!tracking_current_graph)
				{
					/*
					 * Not tracking this graph, just see whether we've recursed into the top level function
					 * (see the comments near the beginning of the file)
					 */
					if (flinfo->fn_oid == top_level_function_oid)
						recursion_depth++;

					return;
				}

				elem = linitial(call_stack);

				/* Calculate the self time we spent in the previous function (elem->key.callee in this case). */
				INSTR_TIME_ACCUM_DIFF(elem->self_time, current_time, current_self_time_start);

				key.caller = elem->key.callee;
			}

			key.callee = flinfo->fn_oid;

			elem = hash_search(edge_hash_table, (void *) &key, HASH_ENTER, &found);
			if (found)
				elem->num_calls++;
			else
			{
				elem->key = key;
				elem->num_calls = 1;
				INSTR_TIME_SET_ZERO(elem->total_time);
				INSTR_TIME_SET_ZERO(elem->self_time);
			}

			call_stack = lcons(elem, call_stack);

			INSTR_TIME_SET_CURRENT(elem->total_time_start);
			memcpy(&current_self_time_start, &elem->total_time_start, sizeof(instr_time));
		}
			break;

		/*
		 * In both ABORT and END cases we pop off the last element from the call stack, and if the stack
		 * is empty, we process the data we gathered.
		 *
		 * XXX for some reason if the top level function aborted SPI won't work correctly.
		 */
		case FHET_ABORT:
			aborted = true;

		case FHET_END:
			/*
			 * If we're not tracking this particular graph, we only need to see whether we're done
			 * with the graph or not.
			 */
			if (!tracking_current_graph)
			{
				if (top_level_function_oid == flinfo->fn_oid)
				{
					recursion_depth--;
					if (recursion_depth == 0)
						top_level_function_oid = InvalidOid;
				}

				Assert(table_stat_snapshot == NULL);
				return;
			}

			Assert(((EdgeHashElem *) linitial(call_stack))->key.callee == flinfo->fn_oid);

			elem = linitial(call_stack);
			INSTR_TIME_ACCUM_DIFF(elem->self_time, current_time, current_self_time_start);
			INSTR_TIME_ACCUM_DIFF(elem->total_time, current_time, elem->total_time_start);

			call_stack = list_delete_first(call_stack);

			if (call_stack != NIL)
			{
				/* we're going back to the previous node, start recording its self_time */
				INSTR_TIME_SET_CURRENT(current_self_time_start);
				break;
			}

			/*
			 * At this point we're done with the graph.  If the top level function exited cleanly, we can
			 * process the data we've gathered in the hash table and add that data into the buffer table.
			 */
			if (!aborted)
			{
				/* temporarily disable call graph to allow triggers on the target tables */
				bool save_enable_call_graph = enable_call_graph;
				enable_call_graph = false;

				/*
				 * It is in some cases possible that process_edge_data() throws an exception.  We really need to
				 * clean up our state in case that happens.
				 */
				PG_TRY();
				{
					Datum buffer_id = assign_callgraph_buffer_id();

					/* Better check both conditions here */
					if (table_stat_snapshot && track_table_usage)
						insert_snapshot_delta(buffer_id, table_stat_snapshot);

					process_edge_data(buffer_id);
					enable_call_graph = save_enable_call_graph;
				}
				PG_CATCH();
				{
					if (table_stat_snapshot)
					{
						release_table_stat_snapshot(table_stat_snapshot);
						table_stat_snapshot = NULL;
					}

					enable_call_graph = save_enable_call_graph;
					destroy_edge_hash_table();
					top_level_function_oid = InvalidOid;
					PG_RE_THROW();
				}
				PG_END_TRY();
			}

			if (table_stat_snapshot)
			{
				release_table_stat_snapshot(table_stat_snapshot);
				table_stat_snapshot = NULL;
			}

			destroy_edge_hash_table();
			top_level_function_oid = InvalidOid;

			break;
		default:
			elog(ERROR, "Unknown FmgrHookEventType %d", event);
			return;
	}
}

/*
 * Module Load Callback
 */
void
_PG_init(void)
{
	DefineCustomBoolVariable("call_graph.enable", "Enables real-time tracking of function calls.", "", &enable_call_graph, false, PGC_USERSET,
							 0, NULL, NULL, NULL);
	DefineCustomBoolVariable("call_graph.track_table_usage", "Enables tracking of per-callgraph table usage.", "Has no effect if call_graph.enable is not set.", &track_table_usage, false, PGC_USERSET,
							 0, NULL, NULL, NULL);

	/* Install our hooks */
	next_needs_fmgr_hook = needs_fmgr_hook;
	needs_fmgr_hook = call_graph_needs_fmgr_hook;

	next_fmgr_hook = fmgr_hook;
	fmgr_hook = call_graph_fmgr_hook;
}
