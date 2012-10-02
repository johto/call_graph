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

typedef struct hashkey {
	Oid caller;
	Oid callee;
} HashKey;

typedef struct hashelem {
	HashKey key;
	int num_calls;
	instr_time self_time;
	instr_time total_time;

	/* temporary variable to keep track of the total time */
	instr_time total_time_start;
} HashElem;


static bool enable_call_graph = false;
static bool track_table_usage = false;

static needs_fmgr_hook_type next_needs_fmgr_hook = NULL;
static fmgr_hook_type next_fmgr_hook = NULL;

static HTAB *edge_hash_table = NULL;

static List *call_stack = NULL;
static Oid top_level_function_oid = InvalidOid;

static bool tracking_current_graph = false;
static int recursion_depth = 0;

static instr_time current_self_time_start; /* we only need one variable to keep track of all self_times */


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
	ctl.keysize = sizeof(HashKey);
	ctl.entrysize = sizeof(HashElem);
	ctl.hash = tag_hash;

	edge_hash_table = hash_create("call_graph_edge_hash_table", 128, &ctl, HASH_ELEM | HASH_FUNCTION);
}

static void destroy_edge_hash_table()
{
	Assert(edge_hash_table != NULL);

	hash_destroy(edge_hash_table);
	edge_hash_table = NULL;
}

static Datum get_session_identifier()
{
	List *names;
	Oid seqoid;

	names = stringToQualifiedNameList("call_graph.seqCallGraphBuffer");
	seqoid = RangeVarGetRelid(makeRangeVarFromNameList(names), false);

	return DirectFunctionCall1(nextval_oid, ObjectIdGetDatum(seqoid));
}

static void process_edge_data()
{
	int ret;
	HASH_SEQ_STATUS hst;
	HashElem *elem;
	SPIPlanPtr planptr;
	Datum args[7];
	Oid argtypes[] = { INT8OID, OIDOID, OIDOID, OIDOID, INT8OID, FLOAT8OID, FLOAT8OID, InvalidOid };

	/* Start by freezing the hash table.  This saves us some trouble. */
	hash_freeze(edge_hash_table);

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "could not connect to the SPI: %d", ret);


	args[0] = get_session_identifier();

	/* Track table usage before adding data to CallGraphBuffer to avoid it from appearing
	 * in TableAccessBuffer. */
	if (track_table_usage)
	{
		TimestampTz xact_start, stmt_start;

		xact_start = GetCurrentTransactionStartTimestamp();
		stmt_start = GetCurrentStatementStartTimestamp();

		/* Only track usage if this was the first query in the transaction */
		if (timestamp_cmp_internal(xact_start, stmt_start) == 0)
		{
			planptr = SPI_prepare("INSERT INTO																			"
								  "   call_graph.TableAccessBuffer (CallGraphBufferID, relid, seq_scan, seq_tup_read,	"
								  "									idx_scan, idx_tup_read,								"
								  "									n_tup_ins, n_tup_upd, n_tup_del)					"
								  "SELECT																				"
								  "   $1, relid, seq_scan, seq_tup_read,												"
								  /* idx_* columns might be NULL if there are no indexes on the table */
								  "	  COALESCE(idx_scan, 0), COALESCE(idx_tup_fetch, 0),								"
								  "   n_tup_ins, n_tup_upd, n_tup_del													"
								  "FROM																					"
								  "   pg_stat_xact_user_tables															"
								  "WHERE																				"
								  /* Exclude data for TableAccessBuffer; if we insert any rows before we get to the
								   * TableAccessBuffer row, it will include the rows we inserted.  We do not want that. */
								  "   relid <> 'call_graph.TableAccessBuffer'::regclass::oid AND						"
								  "   GREATEST(seq_scan, idx_scan, n_tup_ins, n_tup_upd, n_tup_del) > 0					",
								  1, argtypes);

			if (!planptr)
				elog(ERROR, "could not prepare an SPI plan for the INSERT into TableAccessBuffer");

			/* args[0] was set above */

			if ((ret = SPI_execp(planptr, args, NULL, 0)) < 0)
				elog(ERROR, "SPI_execp() failed: %d", ret);
		}
	}

	planptr = SPI_prepare("INSERT INTO 																				"
						  "   call_graph.CallGraphBuffer (CallGraphBufferID, TopLevelFunction, Caller, Callee,		"
						  "								  Calls, TotalTime, SelfTime)								"
						  "   VALUES ($1, $2, $3, $4, $5, $6, $7)													",
						  7, argtypes);
	if (!planptr)
		elog(ERROR, "could not prepare an SPI plan for the INSERT into CallGraphBuffer");

	/* args[0] was set above */
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
	HashKey key;
	HashElem *elem;
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

				return;
			}

			Assert(((HashElem *) linitial(call_stack))->key.callee == flinfo->fn_oid);

			elem = linitial(call_stack);
			INSTR_TIME_ACCUM_DIFF(elem->self_time, current_time, current_self_time_start);
			INSTR_TIME_ACCUM_DIFF(elem->total_time, current_time, elem->total_time_start);

			call_stack = list_delete_first(call_stack);

			if (call_stack != NIL)
			{
				/* we're back to the previous node, start recording its self_time */
				INSTR_TIME_SET_CURRENT(current_self_time_start);
				break;
			}

			/*
			 * At this point we're done with the graph.  If the top level function exited cleanly, we can
			 * process the data we've gathered in the hash table and add that data into the buffer table.
			 */
			if (!aborted)
			{
				/*
				 * It is in some cases possible that process_edge_data() throws an exception.  We really need to
				 * clean up our state in case that happens.
				 */
				PG_TRY();
				{
					process_edge_data();
				}
				PG_CATCH();
				{
					destroy_edge_hash_table();
					top_level_function_oid = InvalidOid;
					PG_RE_THROW();
				}
				PG_END_TRY();
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
