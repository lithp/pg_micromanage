
#include "postgres.h"

#include "access/htup_details.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/portal.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

static PlannedStmt* planQuery(char *queryString);

typedef struct
{
	Portal portal;
	Tuplestorestate *tstore;
} run_select_context;

/* makes sure everything is building correctly */
PG_FUNCTION_INFO_V1(return_one);
Datum return_one(PG_FUNCTION_ARGS)
{
	PG_RETURN_INT16(1);
}

/* accepts a query, plans it, then dumps the result */
PG_FUNCTION_INFO_V1(dump_query);
Datum dump_query(PG_FUNCTION_ARGS)
{
	text *t = PG_GETARG_TEXT_P(0);

	char *queryString = text_to_cstring(t);
	List *parseTreeList = pg_parse_query(queryString);
	uint32 treeCount = list_length(parseTreeList);

	Node *parsedTree;
	List *queryTreeList;
	Query *selectQuery;
	PlannedStmt *plan;

	char *serialized, *prettyPrinted;

	text *result;

	if (treeCount != 1)
	{
		ereport(ERROR, (errmsg("argument must include a single command")));
	}

	parsedTree = (Node *) linitial(parseTreeList);

	if (!IsA(parsedTree, SelectStmt))
	{
		ereport(ERROR, (errmsg("only SELECT statements are supported")));
	}

	queryTreeList = pg_analyze_and_rewrite(parsedTree, queryString, NULL, 0);
	selectQuery = (Query *) linitial(queryTreeList);

	plan = standard_planner(selectQuery, 0, NULL);

	serialized = nodeToString(plan);
	prettyPrinted = pretty_format_node_dump(serialized);

	result = cstring_to_text(prettyPrinted);
	PG_RETURN_TEXT_P(result);
}


PG_FUNCTION_INFO_V1(return_variadic);
Datum return_variadic(PG_FUNCTION_ARGS)
{
	int arg = PG_GETARG_INT32(0);

	HeapTuple resultTuple;
	Datum resultDatum;

	Datum *values;
	bool *isnull;

	/* figure out what we're supposed to return */
	TupleDesc tupleDesc;
	TypeFuncClass typeClass = get_call_result_type(fcinfo, NULL, &tupleDesc);

	if (typeClass == TYPEFUNC_SCALAR)
	{
		/* we've only been asked to return one value */
		PG_RETURN_INT32(arg);
	}

	if (typeClass != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR, (errmsg("called in context that cannot accept type record")));
	}

	tupleDesc = BlessTupleDesc(tupleDesc);

	/* return the requested number of t */
	values = palloc(sizeof(Datum) * tupleDesc->natts);
	isnull = palloc(sizeof(bool) * tupleDesc->natts);
	
	// todo: also check that it expects ints...

	for(int i = 0; i < tupleDesc->natts; i++)
	{
		values[i] = Int32GetDatum(arg);
		isnull[i] = false;
	}

	resultTuple = heap_form_tuple(tupleDesc, values, isnull);
	resultDatum = HeapTupleGetDatum(resultTuple);

	PG_RETURN_DATUM(resultDatum);
}

PG_FUNCTION_INFO_V1(return_set);
Datum return_set(PG_FUNCTION_ARGS)
{
	int arg = PG_GETARG_INT32(0);

	FuncCallContext *funcctx;

	if (SRF_IS_FIRSTCALL())
	{
		MemoryContext oldcontext;

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);

		MemoryContextSwitchTo(oldcontext);
	}

	funcctx = SRF_PERCALL_SETUP();

	if (funcctx->call_cntr < arg)
	{
		SRF_RETURN_NEXT(funcctx, Int32GetDatum(arg));
	}
	else
	{
		SRF_RETURN_DONE(funcctx);
	}
}

static PlannedStmt *
planQuery(char *queryString)
{
	List *parseTreeList = pg_parse_query(queryString);
	uint32 treeCount = list_length(parseTreeList);

	Node *parsedTree;
	List *queryTreeList;
	Query *selectQuery;
	PlannedStmt *plan;

	if (treeCount != 1)
	{
		ereport(ERROR, (errmsg("argument must include a single command")));
	}

	parsedTree = (Node *) linitial(parseTreeList);

	if (!IsA(parsedTree, SelectStmt))
	{
		ereport(ERROR, (errmsg("only SELECT statements are supported")));
	}

	queryTreeList = pg_analyze_and_rewrite(parsedTree, queryString, NULL, 0);
	selectQuery = (Query *) linitial(queryTreeList);

	plan = standard_planner(selectQuery, 0, NULL);
	return plan;
}


PG_FUNCTION_INFO_V1(run_select);
Datum run_select(PG_FUNCTION_ARGS)
{
	/* pg_cursor does something crazy with tuplestores, maybe duplicate it? */
	FuncCallContext *funcctx;
	run_select_context *ctx;
	Portal portal;

	text *t = PG_GETARG_TEXT_P(0);
	char *queryString = text_to_cstring(t);

	Tuplestorestate *tstore;
	DestReceiver *receiver;
	TupleTableSlot *slot;
	Datum result;
	uint64 count;

	TupleDesc tupleDesc;
	TypeFuncClass typeClass = get_call_result_type(fcinfo, NULL, &tupleDesc);

	if (typeClass != TYPEFUNC_COMPOSITE)
	{
		ereport(ERROR, (errmsg("called in context that cannot accept type record")));
	}

	tupleDesc = BlessTupleDesc(tupleDesc);

	if (SRF_IS_FIRSTCALL())
	{
		/* TODO: Check that the dest TupleDesc matches this query */
		MemoryContext oldcontext;
		PlannedStmt *plan = planQuery(queryString);

		funcctx = SRF_FIRSTCALL_INIT();
		oldcontext = MemoryContextSwitchTo(funcctx->multi_call_memory_ctx);
		{
			ctx = (run_select_context *) palloc(sizeof(run_select_context));

			portal = CreateNewPortal();
			portal->visible = false; /* hide the portal from pg_cursors */

			ctx->portal = portal;
			funcctx->user_fctx = ctx;
		}
		MemoryContextSwitchTo(oldcontext);

		queryString = MemoryContextStrdup(PortalGetHeapMemory(portal), queryString);

		PortalDefineQuery(portal, NULL, queryString, "SELECT", list_make1(plan), NULL);

		PortalStart(portal, NULL, 0, InvalidSnapshot);
	}

	funcctx = SRF_PERCALL_SETUP();
	ctx = funcctx->user_fctx;
	portal = ctx->portal;

	tstore = tuplestore_begin_heap(false, false, work_mem);
	receiver = CreateDestReceiver(DestTuplestore);
	SetTuplestoreDestReceiverParams(receiver, tstore, CurrentMemoryContext, true);

	count = PortalRunFetch(portal, FETCH_FORWARD, 1, receiver);
	if (count == 0)
	{
		PortalDrop(portal, false);
		SRF_RETURN_DONE(funcctx);
	}

	slot = MakeSingleTupleTableSlot(tupleDesc); /* TODO: check that desc matches */
	tuplestore_rescan(tstore); /* put the read pointer at the beginning */
	tuplestore_gettupleslot(tstore, true, true, slot); /* makes a copy for us */
	result = ExecFetchSlotTupleDatum(slot); /* makes another copy for us */

	(*receiver->rDestroy) (receiver);

	SRF_RETURN_NEXT(funcctx, result);
}
