#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/portal.h"

#include "queries.pb-c.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

void _PG_init(void);
PlannedStmt * skip_planner(Query *, int, ParamListInfo);

static PlannedStmt * planQuery(char *queryString);
static PlannedStmt * planForFunc(FuncExpr *funcexpr);
static PlannedStmt * scanTable(char *tableName);
static SeqScan * createSeqScan(void);
static RangeTblEntry * createRangeTable(char *tableName);

static SelectQuery * decodeQuery(char *string);

typedef struct
{
	Portal portal;
	Tuplestorestate *tstore;
} run_select_context;

void
_PG_init(void)
{
	if (planner_hook != NULL)
	{
		ereport(ERROR, (errmsg("a planner hook already exists.")));
	}

	planner_hook = skip_planner;
}

PlannedStmt *
skip_planner(Query *parse, int cursorOptions, ParamListInfo boundParams)
{
	CmdType commandType = parse->commandType;
	RangeTblEntry *rangeTable;
	RangeTblFunction *rangeTableFunction;
	Node *funcexpr;
	Oid funcid;
	Oid runSelectFuncId;
	Oid textOid = TEXTOID;

	/* Only SELECT * FROM run_select('some query'); is supported */

	if (commandType != CMD_SELECT)
		goto use_standard_planner;

	if (list_length(parse->rtable) != 1)
		goto use_standard_planner;

	rangeTable = linitial(parse->rtable);

	if (rangeTable->rtekind != RTE_FUNCTION
		|| (rangeTable->functions == NULL)
		|| list_length(rangeTable->functions) != 1)
		goto use_standard_planner;

	rangeTableFunction = linitial(rangeTable->functions);
	funcexpr = rangeTableFunction->funcexpr;

	if (!IsA(funcexpr, FuncExpr))
		goto use_standard_planner;

	{
		Value *string = makeString(pstrdup("run_select"));
		runSelectFuncId = LookupFuncName(list_make1(string), 1, &textOid, false);
	}

	funcid = ((FuncExpr *)funcexpr)->funcid;
	if (funcid != runSelectFuncId)
		goto use_standard_planner;

	/* TODO: This doesn't happen the first time.. but the second time it does? */
	/* Maybe we need to be added to shared_preload_libraries? */
	ereport(WARNING, (errmsg("replacing plan")));

	//return emptyPlan();
	return planForFunc((FuncExpr *)funcexpr);

use_standard_planner:
	return standard_planner(parse, cursorOptions, boundParams);
}

PlannedStmt *
planForFunc(FuncExpr *funcexpr)
{
	Const *arg;
	char *protobufString;

	SelectQuery *protobuf;

	Assert(IsA(funcexpr, FuncExpr));
	Assert(list_length(funcexpr->args) == 1);

	arg = linitial(funcexpr->args);

	Assert(IsA(arg, Const));
	Assert(arg->consttype == TEXTOID);

	protobufString = TextDatumGetCString(arg->constvalue);
	protobuf = decodeQuery(protobufString);

	return scanTable(protobuf->table);

	//return planQuery(protobuf);
}

static
SelectQuery *
decodeQuery(char *string)
{
	SelectQuery *protobuf;

	/* first, base64 decode the input string */
	Oid decode_oid = fmgr_internal_function("binary_decode");

	text *string_as_text = cstring_to_text(string);
	Datum protobuf_datum = PointerGetDatum(string_as_text);

	text *base64_text = cstring_to_text(pstrdup("base64"));
	Datum base64_datum = PointerGetDatum(base64_text);

	Datum buffer = OidFunctionCall2Coll(decode_oid,
										InvalidOid,
										protobuf_datum,
										base64_datum);

	/* TODO: Use the PG allocator */
	protobuf = select_query__unpack(NULL, VARSIZE(buffer) - VARHDRSZ,
									(uint8_t*) VARDATA(buffer));
	if (protobuf == NULL)
	{
		ereport(ERROR, (errmsg("protobuf could not decode")));
	}

	return protobuf;
}

/* Attempt to make a plan, completely from scratch, and see if it crashes */
static
PlannedStmt *
scanTable(char *tableName)
{
	PlannedStmt *result = makeNode(PlannedStmt);

	result->commandType = CMD_SELECT; /* only support SELECT for now */
	result->queryId = 0; /* TODO: does this need a real value? */
	result->hasReturning = false;
	result->hasModifyingCTE = false;
	result->canSetTag = true;
	result->transientPlan = false;
	result->dependsOnRole = false;
	result->parallelModeNeeded = false;

	result->planTree = (Plan *) createSeqScan();
	result->rtable = list_make1(createRangeTable(tableName));

	result->resultRelations = NULL;
	result->utilityStmt = NULL;
	result->subplans = NULL;
	result->rewindPlanIDs = NULL;
	result->rowMarks = NULL;

	/* relationOids is used for plan caching, not by the executor. */
	result->relationOids = NULL;

	/* also used for caching, this plan will never be cached */
	result->invalItems = NULL;

	/* This is described well in primnodes.h:200 */
	/* For the forseeable future we will have none of these */
	result->nParamExec = 0;

	return result;
}

static
SeqScan *
createSeqScan(void)
{
	SeqScan *node = makeNode(SeqScan);
	Plan *plan = &node->plan;

	Var *targetExpr = makeVar(1, 1, 23, -1, 0, 0);
	TargetEntry *entry = makeTargetEntry((Expr *)targetExpr, 1, "a", false);
	List *tlist = list_make1(entry);

	plan->targetlist = tlist;
	plan->qual = NULL; // Nothing for now, this is where WHERE clauses will go
	plan->lefttree = NULL;
	plan->righttree = NULL;
	node->scanrelid = 1; /* An index into the range table, not an oid */

	// TODO: Do we need to populate any of the cost info? Does anything but
	//       EXPLAIN use it?

	return node;
}

static
RangeTblEntry *
createRangeTable(char *tableName)
{
	RangeTblEntry *result = makeNode(RangeTblEntry);
	Oid relationId = get_relname_relid(tableName, PG_PUBLIC_NAMESPACE);

	result->rtekind = RTE_RELATION;
	result->relid = relationId;
	result->relkind = 'r';

	return result;
}

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
	pprint(plan);
	return plan;
}


/*
 * Improvements:
 * - This does not check that the TupleDesc of its return type matches that of the query
 * - Using a tuplestore to store a single tuple feels silly, make your own receiver?
 *
 * Tests:
 * - Does this work inside a transaction?
 * - Does this work inside a larger query?
 *
 * Resources:
 * - pg_cursor does something crazy with tuplestore, maybe duplicate that?
 */
PG_FUNCTION_INFO_V1(run_select_old);
Datum run_select_old(PG_FUNCTION_ARGS)
{
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

	slot = MakeSingleTupleTableSlot(tupleDesc);
	tuplestore_gettupleslot(tstore, true, true, slot); /* makes a copy for us */
	result = ExecFetchSlotTupleDatum(slot); /* makes another copy for us */

	(*receiver->rDestroy) (receiver);

	SRF_RETURN_NEXT(funcctx, result);
}


PG_FUNCTION_INFO_V1(run_select);
Datum run_select(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("must be run like: SELECT * FROM run_select() in a top level")));
}
