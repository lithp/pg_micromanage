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

static PlannedStmt * planForFunc(FuncExpr *funcexpr);
static SelectQuery * decodeQuery(char *string);

static PlannedStmt * scanTable(SelectQuery *query);
static SeqScan * createSeqScan(void);
static RangeTblEntry * createRangeTable(char *tableName);

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

	return scanTable(protobuf);
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


static
PlannedStmt *
scanTable(SelectQuery *query)
{
	Plan *scan = (Plan *) createSeqScan();
	List *rtables = NULL;

	if (query->n_rtable == 0)
	{
		ereport(ERROR, (errmsg("must include at least one range table")));
	}

	for (int i = 0; i < query->n_rtable; i++)
	{
		RangeTable *rtable = query->rtable[i];
		RangeTblEntry *entry = createRangeTable(rtable->name);
		rtables = lappend(rtables, entry);
	}

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

		result->planTree = scan;
		result->rtable = rtables;

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

	if (relationId == InvalidOid)
	{
		ereport(ERROR, (errmsg("table %s does not exist", tableName)));
	}

	result->rtekind = RTE_RELATION;
	result->relid = relationId;
	result->relkind = 'r';

	return result;
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

	pprint(plan);

	result = cstring_to_text(pstrdup("check the log"));
	PG_RETURN_TEXT_P(result);
}


PG_FUNCTION_INFO_V1(run_select);
Datum run_select(PG_FUNCTION_ARGS)
{
	ereport(ERROR, (errmsg("must be run like: SELECT * FROM run_select() in a top level")));
}
