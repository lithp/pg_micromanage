/*
 * Known limitations:
 * - does not populate any cost info
 * - likely does not respect any security settings
 * - who knows whether it will work inside a transaction
 * - leaks memory, maybe don't run too many of these in a single session
 */
#include "postgres.h"

#include "access/htup_details.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "executor/tstoreReceiver.h"
#include "funcapi.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/print.h"
#include "optimizer/planner.h"
#include "parser/parse_func.h"
#include "tcop/dest.h"
#include "tcop/tcopprot.h"
#include "tcop/pquery.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/syscache.h"
#include "utils/portal.h"

#include "queries.pb-c.h"


#ifdef PG_MODULE_MAGIC
PG_MODULE_MAGIC;
#endif

/* TODO: visibleTable should be a list (or possibly a bitmap) */
typedef struct
{
	List *rtables;
	Index visibleTable;  // Which rtables are currently visible to vars
	List *leftTargets;   // Inside a join, so LeftRef's know what they're referencing
	List *rightTargets;  // Inside a join, so RightRef's know what they're referencing
} Context;

void _PG_init(void);
PlannedStmt * skip_planner(Query *, int, ParamListInfo);

static PlannedStmt * planForFunc(FuncExpr *funcexpr);
static SelectQuery * decodeQuery(char *string);

static PlannedStmt * scanTable(SelectQuery *query);

static SeqScan * createSeqScan(Context *context, SequenceScan *scan, Index *scanTable);
static RangeTblEntry * createRangeTable(char *tableName);

static Expr * createExpression(Context *context, Expression *expression);
static Expr * createQual(Context *context, Expression *expression);
static OpExpr * createOpExpr(Context *context, Expression__Operation *op);
static Var * createVar(Context *context, Expression__ColumnRef *ref);
static Const * createConst(Expression__Constant *constant);

static Plan * createPlan(PlanNode *plan, List *rtables);
static Join * createJoin(JoinNode *join, List *rtables);
static NestLoop * createNestedLoop(JoinNode *join, List *rtables);

static Oid rangeTableId(List *rtables, Index index);
static JoinType joinTypeMapping(JoinNode__Type protobufType);
static char * get_typname(Oid typid);
static Context * makeContext(void);

void
_PG_init(void)
{
	if (planner_hook != NULL)
	{
		ereport(ERROR, (errmsg("a planner hook already exists.")));
	}

	planner_hook = skip_planner;
}

/* TODO: This doesn't happen the first time.. but the second time it does? */
/* Maybe we need to be added to shared_preload_libraries? */
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
	Plan *plan;
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

	plan = createPlan(query->plan, rtables);

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

		result->planTree = plan;
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

		pprint(result);
		return result;
	}
}

/* we need the list of rtables in order to resolve column references */
static
SeqScan *
createSeqScan(Context *context, SequenceScan *scan, Index *scanTable)
{
	SeqScan *node = makeNode(SeqScan);

	if (scan->table == 0)
	{
		ereport(ERROR, (errmsg("range tables are 1-indexed, sequence scan cannot use "
							   "table \"0\"")));
	}

	if (scan->table > list_length(context->rtables))
	{
		/* TODO: In a large plan this will be confusing, somehow name `scan` */
		ereport(ERROR, (errmsg("range table %d does not exist", scan->table)));
	}

	*scanTable = node->scanrelid = scan->table;

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

static
Expr *
createExpression(Context *context, Expression *expression)
{
	switch (expression->expr_case)
	{
		case EXPRESSION__EXPR_VAR:
			return (Expr *) createVar(context, expression->var);
		case EXPRESSION__EXPR_CONST:
			return (Expr *) createConst(expression->const_);
		case EXPRESSION__EXPR_OP:
			return (Expr *) createOpExpr(context, expression->op);
		default:
			ereport(ERROR, (errmsg("unrecognized expression type")));
	}
}

static Expr * createQual(Context *context, Expression *expression)
{
	Expr *result = createExpression(context, expression);

	Oid type = exprType((Node *) result);
	if (type != BOOLOID)
	{
		/* TODO: The parser calls coerce_to_boolean here, we can too? */
		ereport(ERROR, (errmsg("quals must return a boolean")));
	}

	return result;
}

static
OpExpr *
createOpExpr(Context *context, Expression__Operation *op)
{
	OpExpr *expr = makeNode(OpExpr);
	Expression *left = op->arg[0];
	Expression *right = op->arg[1];

	Expr *leftExpr, *rightExpr;
	Oid leftType, rightType;

	Value *nameAsString = makeString(pstrdup(op->name));

	if (op->n_arg != 2)
	{
		ereport(ERROR, (errmsg("only binary operators are supported")));
	}

	/* TOOD: check stack depth? */
	leftExpr = createExpression(context, left);
	rightExpr = createExpression(context, right);

	leftType = exprType((Node *) leftExpr);
	rightType = exprType((Node *) rightExpr);

	expr->opno = OpernameGetOprid(list_make1(nameAsString), leftType, rightType);
	if (expr->opno == InvalidOid)
	{
		char *leftName = get_typname(leftType);
		char *rightName = get_typname(rightType);

		char *leftDesc = (leftName == NULL) ? psprintf("%d", leftType) : leftName;
		char *rightDesc = (rightName == NULL) ? psprintf("%d", rightType) : rightName;

		ereport(ERROR, (errmsg("could not find operator named \"%s\"", op->name),
					    errdetail("arg types: \"%s\" and \"%s\"", leftDesc, rightDesc)));
	}

	expr->opfuncid = get_opcode(expr->opno);
	expr->opresulttype = get_op_rettype(expr->opno);

	expr->opretset = false; // set-returning operators are not supported

	/* collations are not supported */
	expr->opcollid = InvalidOid;
	expr->inputcollid = InvalidOid;

	expr->args = list_make2(leftExpr, rightExpr);
	
	expr->location = -1;
	return expr;
}

static
Var *
createVar(Context *context, Expression__ColumnRef *ref)
{
	AttrNumber attnum;
	Oid atttype;
	Var *result;
	Oid relid;

	Assert(ref != NULL);

	if (context->visibleTable == 0)
	{
		ereport(ERROR, (errmsg("cannot create Var, no tables are visible")));
	}

	if (ref->table != context->visibleTable)
	{
		ereport(WARNING, (errmsg("cant select from table %d, using table %d instead",
								 ref->table, context->visibleTable)));
		ref->table = context->visibleTable;
	}

	relid = rangeTableId(context->rtables, ref->table);
	attnum = get_attnum(relid, ref->column);

	if (attnum == InvalidAttrNumber)
	{
		ereport(ERROR, (errmsg("could not find column %s of table %d",
							   ref->column, ref->table)));
	}

	/* TODO: Add support for typemod and colid */
	atttype = get_atttype(relid, attnum);
	result = makeVar(ref->table, attnum, atttype, -1, 0, 0);
	return result;
}

/* nb: the tables are 1-indexed */
static
Oid
rangeTableId(List *rtables, Index index)
{
	RangeTblEntry *rangeTable = (RangeTblEntry *) list_nth(rtables, index - 1);
	Assert(IsA(rangeTable, RangeTblEntry));
	return rangeTable->relid;
}

static
Const *
createConst(Expression__Constant *constant)
{
	Const *result = makeNode(Const);

	switch (constant->type_case){
		case EXPRESSION__CONSTANT__TYPE_BOOL:
			return (Const *) makeBoolConst(constant->bool_, false);
		case EXPRESSION__CONSTANT__TYPE_UINT:
			result->consttype = INT4OID;
			result->consttypmod = -1;
			result->constcollid = InvalidOid;
			result->constlen = sizeof(uint32_t);
			result->constvalue = UInt32GetDatum(constant->uint);
			result->constisnull = false;
			result->constbyval = true;
			break;
		default:
			ereport(ERROR, (errmsg("constants other than uint and bool aren't supported")));
	}

	result->location = -1; /* "unknown", where in the query string it was found */

	return result;
}

static Plan * createPlan(PlanNode *plan, List *rtables)
{
	Plan *result;
	List *targetList = NULL;
	Expr *qualExpr;
	Context *context = makeContext();

	context->rtables = rtables;

	if (plan->n_target == 0)
	{
		ereport(ERROR, (errmsg("plans must project at least one column")));
	}

	switch (plan->kind_case)
	{
		case PLAN_NODE__KIND_SSCAN:
			result = (Plan *) createSeqScan(context, plan->sscan, &context->visibleTable);
			break;
		case PLAN_NODE__KIND_JOIN:
			/* TODO: this should be given visibility information */
			result = (Plan *) createJoin(plan->join, rtables);
			break;
		default:
			ereport(ERROR, (errmsg("this plan kind is not supported yet")));
	}

	for (int attrno = 0; attrno < plan->n_target; attrno++)
	{
		Expression *expression = plan->target[attrno];
		Expr *expr = createExpression(context, expression);
		TargetEntry *entry = makeTargetEntry(expr, attrno + 1, "a", false);
		targetList = lappend(targetList, entry);
	}
	result->targetlist = targetList;

	if (plan->qual != NULL)
	{
		qualExpr = createQual(context, plan->qual);
		result->qual = list_make1(qualExpr);
	}

	return result;
}

static Join * createJoin(JoinNode *join, List *rtables)
{
	Join *result;
	Expr *joinqual;
	Context *context = makeContext();

	switch (join->kind)
	{
		case JOIN_NODE__KIND__NESTED:
			result = (Join *) createNestedLoop(join, rtables);
			break;
		default:
			ereport(ERROR, (errmsg("only nestedloop joins are supported")));
	}

	context->visibleTable = 0;
	context->rtables = rtables;

	/* what do we pass createExpression? which tables are visible? */
	/* there's a difference between Join->joinqual and Join->plan->qual */
	/* joinqual is a predicate saying whether two rows match */
	/* qual is just selection again, on the finished row */
	joinqual = createExpression(context, join->joinqual);
	result->joinqual = list_make1(joinqual);

	return result;
}

static NestLoop * createNestedLoop(JoinNode *join, List *rtables)
{
	NestLoop *result = makeNode(NestLoop);
	Join *resultJoin = &result->join;
	Plan *resultPlan = &resultJoin->plan;

	if (join->type == JOIN_NODE__TYPE__RIGHT)
	{
		ereport(ERROR, (errmsg("nestedloops do not support RIGHT OUTER joins"),
						errhint("flip the branches then use a LEFT OUTER join")));
	}

	if (join->type == JOIN_NODE__TYPE__FULL)
	{
		ereport(ERROR, (errmsg("nestedloops do not support FULL OUTER joins"),
						errdetail("how would a nestedloop even do that?"),
						errhint("use a different join type")));
	}

	resultJoin->jointype = joinTypeMapping(join->type);

	resultPlan->lefttree = createPlan(join->left, rtables);
	resultPlan->righttree = createPlan(join->right, rtables);

	ereport(ERROR, (errmsg("nestedloops do not yet work")));
	return result;
}

static JoinType joinTypeMapping(JoinNode__Type protobufType)
{
	switch (protobufType)
	{
		case JOIN_NODE__TYPE__INNER:
			return JOIN_INNER;
		case JOIN_NODE__TYPE__LEFT:
			return JOIN_LEFT;
		case JOIN_NODE__TYPE__FULL:
			return JOIN_FULL;
		case JOIN_NODE__TYPE__RIGHT:
			return JOIN_RIGHT;
		case JOIN_NODE__TYPE__SEMI:
			return JOIN_SEMI;
		case JOIN_NODE__TYPE__ANTI:
			return JOIN_ANTI;
		default:
			ereport(ERROR, (errmsg("unrecognized join type: %d", protobufType)));
	}
}

static Context * makeContext(void)
{
	Context *context = palloc0fast(sizeof(Context));

	context->rtables = NULL;
	context->visibleTable = 0;
	context->leftTargets = NULL;
	context->rightTargets = NULL;

	return context;
}

static char * get_typname(Oid typid)
{
	HeapTuple tp;

	tp = SearchSysCache1(TYPEOID, ObjectIdGetDatum(typid));
	if (HeapTupleIsValid(tp))
	{
		Form_pg_type typtup = (Form_pg_type) GETSTRUCT(tp);
		char *result = pstrdup(NameStr(typtup->typname));
		ReleaseSysCache(tp);
		return result;
	}
	else
		return NULL;
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
