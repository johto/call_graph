/*
 * call_graph.c
 *   Implementation of the hooks necessary to make call_graph work.
 */
#include "postgres.h"

#include "fmgr.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "port.h"
#include "access/hash.h"
#include "access/xact.h"
#include "utils/guc.h"
#include "utils/builtins.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/tqual.h"
#include "storage/shmem.h"
#include "catalog/pg_type.h"
#include "catalog/pg_proc.h"
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
 * no need to keep track of the full call stack; just track how many times
 * we've recursed into the top level function.
 */

PG_MODULE_MAGIC;

void _PG_init(void);

typedef struct {
	char *caller_nspname;
	char *caller_signature;

	char *callee_nspname;
	char *callee_signature;
} EdgeHashKey;

typedef struct {
	EdgeHashKey key;
	int num_calls;
	instr_time self_time;
	instr_time total_time;

	/* temporary variable to keep track of the total time */
	instr_time total_time_start;
} EdgeHashElem;


static bool enable_call_graph = false;

static needs_fmgr_hook_type next_needs_fmgr_hook = NULL;
static fmgr_hook_type next_fmgr_hook = NULL;

/*
 * Our own memory context.  All per-graph memory we allocate should be
 * allocated in this context.  The context will be reset any time the call
 * stack is completely unwound (see cg_release_graph_state) to make sure we
 * never leak memory.
 */
static MemoryContext cg_memory_ctx = NULL;

static HTAB *edge_hash_table = NULL;

static List *call_stack = NULL;
static Oid top_level_function_oid = -1;

static bool tracking_current_graph = false;
static int recursion_depth = 0;

static instr_time current_self_time_start; /* we only need one variable to keep track of all self_times */


static void cg_lookup_function(Oid fnoid, char **nspname, char **signature);
static void cg_enter_function(Oid fnoid, instr_time current_time);
static bool cg_needs_fmgr_hook(Oid functionId);
static void cg_fmgr_hook(FmgrHookEventType event, FmgrInfo *flinfo, Datum *args);
static void cg_create_edge_hash_table();
static void cg_destroy_edge_hash_table();
static void cg_release_graph_state();
static Datum cg_assign_callgraph_buffer_id();
static void cg_process_edge_data(Datum callgraph_buffer_id);



/* hash funcs */
static uint32 cg_hash_fn(const void *key, Size keysize);
static int cg_cmp(const char *v1, const char *v2);
static int cg_hash_match_fn(const void *key1, const void *key2, Size keysize);



static bool
cg_needs_fmgr_hook(Oid functionId)
{
	/* our hook needs to always be called to keep track of the call stack */
	return true;
}

/*
 * Release any per-graph state we have: release memory allocated in our memory
 * context, destroy the edge hash table and reset some globals.
 */
static void
cg_release_graph_state()
{
	Assert(call_stack == NIL);

	cg_destroy_edge_hash_table();
	top_level_function_oid = InvalidOid;
	tracking_current_graph = false;

	MemoryContextReset(cg_memory_ctx);
}

static uint32
cg_hash_fn(const void *key, Size keysize)
{
	EdgeHashKey *edge;
	uint32 h;

	Assert(keysize == sizeof(EdgeHashKey));

	edge = (EdgeHashKey *) key;
	h = 0;
	if (edge->caller_nspname != NULL)
	{
		Assert(edge->caller_signature != NULL);
		h ^= DatumGetUInt32(hash_any((const unsigned char *) edge->caller_nspname,
									 strlen(edge->caller_nspname)));
		h ^= DatumGetUInt32(hash_any((const unsigned char *) edge->caller_signature,
									 strlen(edge->caller_signature)));
	}
	Assert(edge->callee_nspname != NULL);
	Assert(edge->callee_signature != NULL);
	h ^= DatumGetUInt32(hash_any((const unsigned char *) edge->callee_nspname,
								 strlen(edge->callee_nspname)));
	h ^= DatumGetUInt32(hash_any((const unsigned char *) edge->callee_signature,
								 strlen(edge->callee_signature)));

	//return h;
	return h ^ h;
}

/*
 * Compare two strings for equality.  NULL pointer is considered smaller than
 * any non-NULL string, and equal to another NULL pointer.
 */
static int
cg_cmp(const char *v1, const char *v2)
{
	if (v1 == NULL && v2 != NULL)
		return -1;
	else if (v1 != NULL && v2 == NULL)
		return 1;
	else if (v1 == NULL && v2 == NULL)
		return 0;
	else
		return strcmp(v1, v2);
}

static int
cg_hash_match_fn(const void *key1, const void *key2, Size keysize)
{
	EdgeHashKey *edge1;
	EdgeHashKey *edge2;
	int cmp;

	Assert(keysize == sizeof(EdgeHashKey));

	edge1 = (EdgeHashKey *) key1;
	edge2 = (EdgeHashKey *) key2;

	elog(INFO, "%s %s %s %s", edge1->caller_nspname, edge1->caller_signature, edge2->caller_nspname, edge2->caller_signature);

	cmp = cg_cmp(edge1->caller_nspname, edge2->caller_nspname);
	if (cmp != 0)
		return cmp;
	cmp = cg_cmp(edge1->caller_signature, edge2->caller_signature);
	if (cmp != 0)
		return cmp;

	cmp = cg_cmp(edge1->callee_nspname, edge2->callee_nspname);
	if (cmp != 0)
		return cmp;
	return cg_cmp(edge1->callee_signature, edge2->callee_signature);
}

static void
cg_create_edge_hash_table()
{
	HASHCTL ctl;

	Assert(edge_hash_table == NULL);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(EdgeHashKey);
	ctl.entrysize = sizeof(EdgeHashElem);
	ctl.hash = cg_hash_fn;
	ctl.match = cg_hash_match_fn;
	/* use our memory context for the hash table */
	ctl.hcxt = cg_memory_ctx;

	edge_hash_table = hash_create("call_graph_edge_hash_table", 128, &ctl,
								  HASH_ELEM | HASH_FUNCTION | HASH_COMPARE | HASH_CONTEXT);
}

static void
cg_destroy_edge_hash_table()
{
	Assert(edge_hash_table != NULL);

	hash_destroy(edge_hash_table);
	edge_hash_table = NULL;
}

static Datum
cg_assign_callgraph_buffer_id()
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

static void
cg_process_edge_data(Datum callgraph_buffer_id)
{
	int ret;
	HASH_SEQ_STATUS hst;
	EdgeHashElem *elem;
	SPIPlanPtr planptr;
	Datum args[8];
	Oid argtypes[] = { INT8OID, TEXTOID, TEXTOID, TEXTOID, TEXTOID,
					   INT8OID, FLOAT8OID, FLOAT8OID, InvalidOid };
	char nulls[8] = "        ";


	/* Start by freezing the hash table.  This saves us some trouble. */
	hash_freeze(edge_hash_table);

	if ((ret = SPI_connect()) < 0)
		elog(ERROR, "could not connect to the SPI: %d", ret);

	planptr = SPI_prepare("INSERT INTO                                 "
						  "   call_graph.CallGraphBuffer               "
 						  "     (CallGraphBufferID,                    "
 						  "      CallerNspName, CallerSignature,       "
 						  "      CalleeNspName, CalleeSignature,       "
 						  "      Calls, TotalTime, SelfTime)           "
 						  "   VALUES ($1, $2, $3, $4, $5, $6, $7, $8)  ",
						  8, argtypes);
	if (!planptr)
		elog(ERROR, "could not prepare an SPI plan for the INSERT into CallGraphBuffer");

	args[0] = callgraph_buffer_id;
	//args[1] = ObjectIdGetDatum(top_level_function_oid);

	hash_seq_init(&hst, edge_hash_table);
	while ((elem = hash_seq_search(&hst)) != NULL)
	{
		if (elem->key.caller_nspname != NULL)
		{
			args[1] = CStringGetTextDatum(elem->key.caller_nspname);
			args[2] = CStringGetTextDatum(elem->key.caller_signature);
			nulls[1] = nulls[2] = ' ';
		}
		else
			nulls[1] = nulls[2] = 'n';

		args[3] = CStringGetTextDatum(elem->key.callee_nspname);
		args[4] = CStringGetTextDatum(elem->key.callee_signature);
		args[5] = Int8GetDatum(elem->num_calls);
		args[6] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(elem->total_time));
		args[7] = Float8GetDatum(INSTR_TIME_GET_MILLISEC(elem->self_time));

		if ((ret = SPI_execp(planptr, args, nulls, 0)) < 0)
			elog(ERROR, "SPI_execp() failed: %d", ret);
	}

	SPI_finish();
}

static char *
cg_get_function_signature(HeapTuple htup, Form_pg_proc procform)
{
	StringInfoData str;
	int nargs;
	int i;
	int input_argno;
	Oid *argtypes;
	char **argnames;
	char *argmodes;

	initStringInfo(&str);
	appendStringInfo(&str, "%s(", NameStr(procform->proname));
	nargs = get_func_arg_info(htup, &argtypes, &argnames, &argmodes);
	input_argno = 0;
	for (i = 0; i < nargs; i++)
	{
		Oid argtype = argtypes[i];

		if (argmodes &&
			argmodes[i] != PROARGMODE_IN &&
			argmodes[i] != PROARGMODE_INOUT)
			continue;

		if (input_argno++ > 0)
			appendStringInfoString(&str, ", ");

		appendStringInfoString(&str, format_type_be(argtype));
	}
	appendStringInfoChar(&str, ')');

	return str.data;
}

static void
cg_lookup_function(Oid fnoid, char **nspname, char **signature)
{
	HeapTuple htup;
	Form_pg_proc procform;
	Oid nspoid;

	htup = SearchSysCache1(PROCOID, ObjectIdGetDatum(fnoid));
	if (!HeapTupleIsValid(htup))
	{
		ReleaseSysCache(htup);
		return;
	}
	procform = (Form_pg_proc) GETSTRUCT(htup);

	*signature = cg_get_function_signature(htup, procform);
	nspoid = procform->pronamespace;
	ReleaseSysCache(htup);

	*nspname = get_namespace_name(nspoid);
	if (!*nspname)
	{
		pfree(*signature);
		*signature = NULL;
		return;
	}
}

static void
cg_enter_function(Oid fnoid, instr_time current_time)
{
	EdgeHashKey key;
	EdgeHashElem *elem;
	bool found;
	MemoryContext oldctx;

	Assert(cg_memory_ctx != NULL);

	if (call_stack == NIL)
	{
		top_level_function_oid = fnoid;

		/*
		 * We're about to enter the top level function; check whether we've
		 * been disabled.
		 */
		if (!enable_call_graph)
		{
			tracking_current_graph = false;
			recursion_depth = 1;
			return;
		}

		/*
		 * OK, the user wants us to start tracking this call graph; we need to
		 * create the hash table.
		*/
		cg_create_edge_hash_table();
		tracking_current_graph = true;

		key.caller_nspname = NULL;
		key.caller_signature = NULL;
	}
	else
	{
		if (!tracking_current_graph)
		{
			/*
			 * Not tracking this graph -- just see whether we've recursed into
			 * the top level function (see the comments near the beginning of
			 * this file)
			 */
			if (fnoid == top_level_function_oid)
				recursion_depth++;

			return;
		}

		elem = linitial(call_stack);

		/*
		 * Calculate the self time we spent in the previous function
		 * (elem->key.callee in this case).
		 */
		INSTR_TIME_ACCUM_DIFF(elem->self_time, current_time, current_self_time_start);

		key.caller_nspname = elem->key.callee_nspname;
		key.caller_signature = elem->key.callee_signature;
	}

	oldctx = MemoryContextSwitchTo(cg_memory_ctx);
	cg_lookup_function(fnoid, &key.callee_nspname, &key.callee_signature);
	MemoryContextSwitchTo(oldctx);

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

static void
cg_fmgr_hook(FmgrHookEventType event, FmgrInfo *flinfo, Datum *args)
{
	bool aborted = false;
	EdgeHashElem *elem;
	instr_time current_time;

	if (next_fmgr_hook)
		(*next_fmgr_hook) (event, flinfo, args);

	INSTR_TIME_SET_CURRENT(current_time);

	switch (event)
	{
		case FHET_START:
			cg_enter_function(flinfo->fn_oid, current_time);
			break;

		/*
		 * In both ABORT and END cases we pop off the last element from the
		 * call stack.  If we didn't abort and the stack is empty, we process
		 * the data we gathered.  Unfortunately we can't process any data if an
		 * exception is raised as we're in a failed transaction.
		 */
		case FHET_ABORT:
			aborted = true;

		case FHET_END:
			/*
			 * If we're not tracking this particular graph, we only need to see
			 * whether we're done with the graph or not.
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

			elem = linitial(call_stack);
			INSTR_TIME_ACCUM_DIFF(elem->self_time, current_time, current_self_time_start);
			INSTR_TIME_ACCUM_DIFF(elem->total_time, current_time, elem->total_time_start);

			call_stack = list_delete_first(call_stack);

			if (call_stack != NIL)
			{
				/*
				 * We're going back to the previous node, start recording its
				 * self_time.
				 */
				INSTR_TIME_SET_CURRENT(current_self_time_start);
				break;
			}

			/*
			 * At this point we're done with the graph.  If the top level
			 * function exited cleanly, we can process the data we've gathered
			 * in the hash table and add that data into the buffer table.
			 */
			if (!aborted)
			{
				/*
				 * Temporarily disable call graph to allow triggers on the
				 * target tables.
				 */
				bool save_enable_call_graph = enable_call_graph;
				enable_call_graph = false;

				/*
				 * It is in some cases possible that process_edge_data() throws
				 * an exception.  We really need to clean up our state in case
				 * that happens.
				 */
				PG_TRY();
				{
					Datum buffer_id = cg_assign_callgraph_buffer_id();

					cg_process_edge_data(buffer_id);
					enable_call_graph = save_enable_call_graph;
				}
				PG_CATCH();
				{
					enable_call_graph = save_enable_call_graph;
					cg_release_graph_state();
					PG_RE_THROW();
				}
				PG_END_TRY();
			}

			cg_release_graph_state();

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
	if (!process_shared_preload_libraries_in_progress)
		elog(ERROR, "please no");

	DefineCustomBoolVariable("call_graph.enable",
							 "Enables real-time tracking of function calls.",
							 "",
							 &enable_call_graph,
							 false,
							 PGC_USERSET,
							 0, NULL, NULL, NULL);

	/* Install our hooks */
	next_needs_fmgr_hook = needs_fmgr_hook;
	needs_fmgr_hook = cg_needs_fmgr_hook;

	next_fmgr_hook = fmgr_hook;
	fmgr_hook = cg_fmgr_hook;

	cg_memory_ctx = AllocSetContextCreate(TopMemoryContext,
										  "call_graph memory context",
										  ALLOCSET_SMALL_MINSIZE,
										  ALLOCSET_SMALL_INITSIZE,
										  ALLOCSET_SMALL_MAXSIZE);
}
