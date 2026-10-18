/* Passive relation-OID catalog logging. Never resolves authority or changes tuples. */
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_attribute.h"
#include "cluster/cluster_catalog_trace.h"
static uint64 trace_sequence;
static uint64 trace_call;
static bool
selected(HeapTuple tuple)
{
	if (cluster_catalog_trace_relation == 0 || tuple == NULL || tuple->t_data == NULL)
		return false;
	if (tuple->t_len < SizeofHeapTupleHeader || tuple->t_data->t_hoff < SizeofHeapTupleHeader
		|| tuple->t_data->t_hoff > tuple->t_len)
		return false;
	if (tuple->t_tableOid == (Oid)cluster_catalog_trace_relation)
		return true;
	if (tuple->t_tableOid == RelationRelationId
		&& tuple->t_len - tuple->t_data->t_hoff >= CLASS_TUPLE_SIZE)
		return ((Form_pg_class)GETSTRUCT(tuple))->oid == (Oid)cluster_catalog_trace_relation;
	if (tuple->t_tableOid == AttributeRelationId
		&& tuple->t_len - tuple->t_data->t_hoff >= ATTRIBUTE_FIXED_PART_SIZE)
		return ((Form_pg_attribute)GETSTRUCT(tuple))->attrelid
			   == (Oid)cluster_catalog_trace_relation;
	return false;
}
uint64
cluster_catalog_trace_enter(HeapTuple tuple)
{
	uint64 previous = trace_call;
	trace_call = selected(tuple) ? ++trace_sequence : 0;
	return previous;
}

void
cluster_catalog_trace_begin(HeapTuple tuple, Snapshot snapshot)
{
	if (trace_call) {
		elog(LOG,
			 "PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
			 " target=%d stage=mvcc.enter table=%u block=%u off=%u source=%u "
			 "read_scn=" UINT64_FORMAT,
			 trace_call, cluster_catalog_trace_relation, tuple->t_tableOid,
			 ItemPointerGetBlockNumber(&tuple->t_self), ItemPointerGetOffsetNumber(&tuple->t_self),
			 snapshot ? snapshot->cluster_source : 0, snapshot ? snapshot->read_scn : 0);
		cluster_catalog_trace_header("mvcc.live", tuple->t_data, snapshot);
	}
}

void
cluster_catalog_trace_leave(uint64 previous)
{
	trace_call = previous;
}
void
cluster_catalog_trace_header(const char *stage, HeapTupleHeader tuple, Snapshot snapshot)
{
	if (!trace_call || !tuple)
		return;
	elog(LOG,
		 "PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
		 " target=%d stage=%s xmin=%u xmax=%u infomask=%u infomask2=%u raw_cid=%u combo=%d "
		 "curcid=%u itl=%u",
		 trace_call, cluster_catalog_trace_relation, stage, HeapTupleHeaderGetRawXmin(tuple),
		 HeapTupleHeaderGetRawXmax(tuple), tuple->t_infomask, tuple->t_infomask2,
		 HeapTupleHeaderGetRawCommandId(tuple), (tuple->t_infomask & HEAP_COMBOCID) != 0,
		 snapshot ? snapshot->curcid : InvalidCommandId, tuple->t_itl_slot_idx);
}
void
cluster_catalog_trace_slot(const char *stage, const ClusterItlSlotData *slot)
{
	if (!trace_call || !slot)
		return;
	elog(LOG,
		 "PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
		 " target=%d stage=%s slot_xid=%u flags=%u wrap=%u locks=%u uba0=" UINT64_FORMAT
		 " uba1=" UINT64_FORMAT " write_scn=" UINT64_FORMAT " commit_scn=" UINT64_FORMAT
		 " first_lsn=" UINT64_FORMAT,
		 trace_call, cluster_catalog_trace_relation, stage, slot->xid, slot->flags, slot->wrap,
		 slot->lock_count, slot->undo_segment_head.raw[0], slot->undo_segment_head.raw[1],
		 slot->write_scn, slot->commit_scn, slot->first_change_lsn);
}
void
cluster_catalog_trace_event(const char *stage, const char *branch, int64 value)
{
	if (!trace_call)
		return;
	elog(LOG,
		 "PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
		 " target=%d stage=%s branch=%s value=" INT64_FORMAT,
		 trace_call, cluster_catalog_trace_relation, stage, branch, value);
}
int
cluster_catalog_trace_result(const char *function, int line, int value)
{
	if (trace_call)
		elog(LOG,
			 "PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
			 " target=%d stage=return function=%s line=%d value=%d",
			 trace_call, cluster_catalog_trace_relation, function, line, value);
	return value;
}
CommandId
cluster_catalog_trace_cid(const char *kind, CommandId value)
{
	cluster_catalog_trace_event("native.cid", kind, value);
	return value;
}
void
cluster_catalog_trace_catalog(const char *stage, Oid relation, HeapTuple tuple, CommandId curcid)
{
	if (cluster_catalog_trace_relation == 0 || relation != (Oid)cluster_catalog_trace_relation)
		return;
	if (!HeapTupleIsValid(tuple)) {
		elog(LOG, "PGRAC_CATALOG_TRACE target=%u stage=%s curcid=%u found=0", relation, stage,
			 curcid);
		return;
	}
	elog(LOG,
		 "PGRAC_CATALOG_TRACE target=%u stage=%s curcid=%u found=1 table=%u block=%u off=%u "
		 "xmin=%u xmax=%u raw_cid=%u combo=%d infomask=%u infomask2=%u",
		 relation, stage, curcid, tuple->t_tableOid, ItemPointerGetBlockNumber(&tuple->t_self),
		 ItemPointerGetOffsetNumber(&tuple->t_self), HeapTupleHeaderGetRawXmin(tuple->t_data),
		 HeapTupleHeaderGetRawXmax(tuple->t_data), HeapTupleHeaderGetRawCommandId(tuple->t_data),
		 (tuple->t_data->t_infomask & HEAP_COMBOCID) != 0, tuple->t_data->t_infomask,
		 tuple->t_data->t_infomask2);
	if (tuple->t_tableOid == RelationRelationId) {
		Form_pg_class row = (Form_pg_class)GETSTRUCT(tuple);
		elog(LOG, "PGRAC_CATALOG_TRACE target=%u stage=%s relnatts=%d relfilenode=%u", relation,
			 stage, row->relnatts, row->relfilenode);
	} else if (tuple->t_tableOid == AttributeRelationId) {
		Form_pg_attribute row = (Form_pg_attribute)GETSTRUCT(tuple);
		elog(LOG, "PGRAC_CATALOG_TRACE target=%u stage=%s attnum=%d atttypid=%u atttypmod=%d",
			 relation, stage, row->attnum, row->atttypid, row->atttypmod);
	}
}
void
cluster_catalog_trace_descriptor(const char *stage, Relation relation, int attnum, CommandId curcid)
{
	bool valid;
	if (cluster_catalog_trace_relation == 0
		|| RelationGetRelid(relation) != (Oid)cluster_catalog_trace_relation)
		return;
	valid = attnum > 0 && attnum <= relation->rd_att->natts;
	elog(LOG,
		 "PGRAC_CATALOG_TRACE target=%u stage=%s curcid=%u relnatts=%d natts=%d attnum=%d "
		 "in_bounds=%d atttypid=%u atttypmod=%d rd_isvalid=%d",
		 RelationGetRelid(relation), stage, curcid, relation->rd_rel->relnatts,
		 relation->rd_att->natts, attnum, valid,
		 valid ? TupleDescAttr(relation->rd_att, attnum - 1)->atttypid : InvalidOid,
		 valid ? TupleDescAttr(relation->rd_att, attnum - 1)->atttypmod : -1, relation->rd_isvalid);
}
void
cluster_catalog_trace_type(Oid relation, Oid target, int32 typmod, Oid source, CommandId curcid)
{
	if (cluster_catalog_trace_relation == 0 || relation != (Oid)cluster_catalog_trace_relation)
		return;
	elog(LOG,
		 "PGRAC_CATALOG_TRACE target=%u stage=cookDefault.coerce curcid=%u source_type=%u "
		 "target_type=%u typmod=%d",
		 relation, curcid, source, target, typmod);
}
