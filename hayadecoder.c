#include "postgres.h"

#include "catalog/pg_class.h"

#include "replication/logical.h"
#include "replication/origin.h"
#include "utils/builtins.h"
#include "utils/relcache.h"

extern void _PG_init(void);
extern void _PG_output_plugin_init(OutputPluginCallbacks *cb);

PG_MODULE_MAGIC;

typedef struct hayadecoder_data
{
    bool write_txd;
} hayadecodeData;

/* support routines */
static void output_insert(StringInfo out,
                          Relation relation,
			  ReorderBufferChange *change);
static void output_update(StringInfo out,
                          Relation relation,
			  ReorderBufferChange *change);
static void output_delete(StringInfo out,
                          Relation relation,
			  ReorderBufferChange *change);

/* callback routines */
static void hayade_startup(LogicalDecodingContext *ctx,
                           OutputPluginOptions *options,
                           bool is_init);
static void hayade_shutdown(LogicalDecodingContext *ctx);
static void hayade_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn);
static void hayade_change(LogicalDecodingContext *ctx,
                      ReorderBufferTXN *txn,
                      Relation relation,
                      ReorderBufferChange *change);
static void hayade_commit(LogicalDecodingContext *ctx,
                      ReorderBufferTXN *txn,
                      XLogRecPtr commit_lsn);

static void
hayade_startup(LogicalDecodingContext *ctx,
                           OutputPluginOptions *options,
                           bool is_init)
{
    hayadecodeData *data;
    ListCell *option;

    data = palloc(sizeof(hayadecodeData));
    data->write_txd = true;

    options->output_type = OUTPUT_PLUGIN_TEXTUAL_OUTPUT;
    foreach(option, ctx->output_plugin_options)
    {
        DefElem *elem = lfirst(option);

        if (strcmp(elem->defname, "write_txd") == 0)
        {
            if (elem->arg == NULL)
                data->write_txd = true;
            else if (!parse_bool(strVal(elem->arg), &data->write_txd))
                elog(ERROR, "write_txd: wrong style");
        }
        else
            elog(ERROR, "unknown option");
    }

    ctx->output_plugin_private = data;
}

static void
hayade_shutdown(LogicalDecodingContext *ctx)
{
    hayadecodeData *data = ctx->output_plugin_private;

    pfree(data);
}

static void
hayade_begin(LogicalDecodingContext *ctx, ReorderBufferTXN *txn)
{
    hayadecodeData *data = ctx->output_plugin_private;

    OutputPluginPrepareWrite(ctx, true);

    if (data->write_txd)
        appendStringInfo(ctx->out, "START TRANSACTION %u!", txn->xid);
	else
        appendStringInfo(ctx->out, "START TRANSACTION!");

    OutputPluginWrite(ctx, true);
}

static void
hayade_change(LogicalDecodingContext *ctx,
                      ReorderBufferTXN *txn,
                      Relation relation,
                      ReorderBufferChange *change)
{
    switch (change->action)
    {
        case REORDER_BUFFER_CHANGE_INSERT:
            OutputPluginPrepareWrite(ctx, true);
            output_insert(ctx->out, relation, change);
            OutputPluginWrite(ctx, true);
            break;
        case REORDER_BUFFER_CHANGE_UPDATE:
            OutputPluginPrepareWrite(ctx, true);
            output_update(ctx->out, relation, change);
            OutputPluginWrite(ctx, true);
            break;
        case REORDER_BUFFER_CHANGE_DELETE:
            OutputPluginPrepareWrite(ctx, true);
            output_delete(ctx->out, relation, change);
            OutputPluginWrite(ctx, true);
            break;
        default:
            elog(ERROR, "unknown change");
            break;
    }
}

static void
output_insert(StringInfo out,
	      Relation relation,
	      ReorderBufferChange *change)
{
    Form_pg_class rel;
    char *relname;

    Assert(change->action == REORDER_BUFFER_CHANGE_INSERT);

    rel = relation->rd_rel;
    relname = NameStr(rel->relname);

    appendStringInfo(out, "INSERTED ");
    appendStringInfo(out, "TABLE: %s ", relname);
}

static void
output_update(StringInfo out,
	      Relation relation,
	      ReorderBufferChange *change)
{
    Form_pg_class rel;
    char *relname;

    Assert(change->action == REORDER_BUFFER_CHANGE_UPDATE);

    rel = relation->rd_rel;
    relname = NameStr(rel->relname);

    appendStringInfo(out, "UPDATED");
    appendStringInfo(out, "TABLE: %s ", relname);
}

static void
output_delete(StringInfo out,
	      Relation relation,
	      ReorderBufferChange *change)
{
    Form_pg_class rel;
    char *relname;

    Assert(change->action == REORDER_BUFFER_CHANGE_DELETE);

    rel = relation->rd_rel;
    relname = NameStr(rel->relname);

    appendStringInfo(out, "DELETED");
    appendStringInfo(out, "TABLE: %s ", relname);
}

static void
hayade_commit(LogicalDecodingContext *ctx,
                      ReorderBufferTXN *txn,
                      XLogRecPtr commit_lsn)
{
    hayadecodeData *data = ctx->output_plugin_private;

    OutputPluginPrepareWrite(ctx, true);

    if (data->write_txd)
        appendStringInfo(ctx->out, "END OF TRANSACTION %u!", txn->xid);
	else
        appendStringInfo(ctx->out, "END OF TRANSACTION!");

    OutputPluginWrite(ctx, true);
}

/* Public Routines */

void
_PG_init(void)
{
    /* NOP */
}

void
_PG_output_plugin_init(OutputPluginCallbacks *cb)
{
	AssertVariableIsOfType(&_PG_output_plugin_init, LogicalOutputPluginInit);

    cb->startup_cb = hayade_startup;
    cb->shutdown_cb = hayade_shutdown;
    cb->begin_cb = hayade_begin;
    cb->change_cb = hayade_change;
    cb->commit_cb = hayade_commit;
}
