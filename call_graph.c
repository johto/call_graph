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

static needs_fmgr_hook_type next_needs_fmgr_hook = NULL;
static fmgr_hook_type next_fmgr_hook = NULL;

static HTAB *edge_hash_table = NULL;

static List *call_stack = NULL;
static Oid top_level_function_oid = InvalidOid;
instr_time current_self_time_start; /* we only need one variable to keep track of all self_times */


static bool
call_graph_needs_fmgr_hook(Oid functionId)
{
	/*
	 * It is possible that we get disabled in the middle of a call graph.  In that case, finish the call graph
	 * at hand, but don't start tracking new ones.
	 */
	return enable_call_graph ||
		   top_level_function_oid != InvalidOid;
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

	names = stringToQualifiedNameList("seqCallGraphBuffer");
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

	planptr = SPI_prepare("INSERT INTO CallGraphBuffer(CallGraphBufferID, TopLevelFunction, Caller, Callee, Calls, TotalTime, SelfTime) VALUES ($1, $2, $3, $4, $5, $6, $7)", 7, argtypes);
	if (!planptr)
		elog(ERROR, "could not prepare an SPI plan for call graph buffer");

	args[0] = get_session_identifier();
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
			  FmgrInfo *flinfo, Datum *private)
{
	bool aborted = false;
	HashKey key;
	HashElem *elem;
	instr_time current_time;

	if (next_fmgr_hook)
		(*next_fmgr_hook) (event, flinfo, private);

	INSTR_TIME_SET_CURRENT(current_time);

	switch (event)
	{
		case FHET_START:
		{
			bool found;

			if (list_head(call_stack) != NULL)
			{
				elem = linitial(call_stack);

				/* Calculate the self time we spent in the previous function (elem->key.callee in this case). */
				INSTR_TIME_ACCUM_DIFF(elem->self_time, current_time, current_self_time_start);

				key.caller = elem->key.callee;
			}
			else
			{
				create_edge_hash_table();
				key.caller = InvalidOid;
				top_level_function_oid = flinfo->fn_oid;
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

			INSTR_TIME_SET_CURRENT(current_self_time_start);
			INSTR_TIME_SET_CURRENT(elem->total_time_start);
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
			Assert(list_length(call_stack) > 0);
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

			/* if the top level function finished cleanly, we can process the data */
			if (!aborted)
			{
				/*
				 * It is in some cases possible that process_edge_data() throws an exception.  We really need to
				 * clean up our state in case that happens, or the backend needs to be restarted (see the checks
				 * in call_graph_needs_fmgr_hook() ).
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
	DefineCustomBoolVariable("call_graph.enable", "enable call_graph", "", &enable_call_graph, false, PGC_USERSET,
							 0, NULL, NULL, NULL);

	/* Install our hooks */
	next_needs_fmgr_hook = needs_fmgr_hook;
	needs_fmgr_hook = call_graph_needs_fmgr_hook;

	next_fmgr_hook = fmgr_hook;
	fmgr_hook = call_graph_fmgr_hook;
}
