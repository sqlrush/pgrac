/* Passive relation-OID catalog logging. Never resolves authority or changes tuples. */
#include "postgres.h"
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_attribute.h"
#include "cluster/cluster_catalog_trace.h"
#include "cluster/cluster_uba.h"
static uint64 trace_sequence;
static ClusterCatalogTraceScope active;
static Oid
selected(HeapTuple tuple)
{
	Oid owner = InvalidOid;
	if (!cluster_trace_visibility_enabled || tuple == NULL || tuple->t_data == NULL)
		return InvalidOid;
	if (tuple->t_len < SizeofHeapTupleHeader || tuple->t_data->t_hoff < SizeofHeapTupleHeader
		|| tuple->t_data->t_hoff > tuple->t_len)
		return InvalidOid;
	if (tuple->t_tableOid == RelationRelationId
		&& tuple->t_len - tuple->t_data->t_hoff >= CLASS_TUPLE_SIZE)
		owner = ((Form_pg_class)GETSTRUCT(tuple))->oid;
	if (tuple->t_tableOid == AttributeRelationId
		&& tuple->t_len - tuple->t_data->t_hoff >= ATTRIBUTE_FIXED_PART_SIZE)
		owner = ((Form_pg_attribute)GETSTRUCT(tuple))->attrelid;
	if (OidIsValid(owner)
		&& cluster_visibility_trace_contains(&cluster_trace_visibility_filter, owner))
		return owner;
	return cluster_visibility_trace_contains(&cluster_trace_visibility_filter, tuple->t_tableOid)
			   ? tuple->t_tableOid
			   : InvalidOid;
}
ClusterCatalogTraceScope
cluster_catalog_trace_enter(HeapTuple tuple)
{
	ClusterCatalogTraceScope previous = active;
	Oid relation = selected(tuple);
	active.call = OidIsValid(relation) ? ++trace_sequence : 0;
	active.relation = relation;
	active.attnum = 0;
	return previous;
}
ClusterCatalogTraceScope
cluster_catalog_trace_default_enter(Oid relation, int attnum)
{
	ClusterCatalogTraceScope previous = active;
	active.call
		= cluster_trace_visibility_enabled
				  && cluster_visibility_trace_contains(&cluster_trace_visibility_filter, relation)
			  ? ++trace_sequence
			  : 0;
	active.relation = relation;
	active.attnum = attnum;
	return previous;
}

void
cluster_catalog_trace_begin(HeapTuple tuple, Snapshot snapshot, const char *input_kind)
{
	if (active.call) {
		ereport(LOG,
				(errmsg_internal("PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
								 " target=%u stage=mvcc.enter table=%u block=%u off=%u source=%u "
								 "read_scn=" UINT64_FORMAT " input_kind=%s",
								 active.call, active.relation, tuple->t_tableOid,
								 ItemPointerGetBlockNumber(&tuple->t_self),
								 ItemPointerGetOffsetNumber(&tuple->t_self),
								 snapshot ? snapshot->cluster_source : 0,
								 snapshot ? snapshot->read_scn : 0, input_kind)));
		cluster_catalog_trace_header("mvcc.input", tuple->t_data, snapshot);
	}
}

void
cluster_catalog_trace_leave(ClusterCatalogTraceScope previous)
{
	active = previous;
}
void
cluster_catalog_trace_header(const char *stage, HeapTupleHeader tuple, Snapshot snapshot)
{
	TransactionId effective_xmax;
	bool known;
	if (!active.call || !tuple)
		return;
	known = (tuple->t_infomask & HEAP_XMAX_IS_MULTI) == 0
			|| (tuple->t_infomask & HEAP_XMAX_INVALID) != 0
			|| HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask);
	effective_xmax = known && (tuple->t_infomask & HEAP_XMAX_INVALID) == 0
							 && !HEAP_XMAX_IS_LOCKED_ONLY(tuple->t_infomask)
						 ? HeapTupleHeaderGetRawXmax(tuple)
						 : InvalidTransactionId;
	ereport(LOG,
			(errmsg_internal(
				"PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
				" target=%u stage=%s xmin=%u xmax=%u infomask=%u infomask2=%u raw_cid=%u combo=%d "
				"curcid=%u itl=%u effective_xmax=%u effective_xmax_known=%d",
				active.call, active.relation, stage, HeapTupleHeaderGetRawXmin(tuple),
				HeapTupleHeaderGetRawXmax(tuple), tuple->t_infomask, tuple->t_infomask2,
				HeapTupleHeaderGetRawCommandId(tuple), (tuple->t_infomask & HEAP_COMBOCID) != 0,
				snapshot ? snapshot->curcid : InvalidCommandId, tuple->t_itl_slot_idx,
				effective_xmax, known)));
}
void
cluster_catalog_trace_slot(const char *stage, const ClusterItlSlotData *slot)
{
	if (!active.call || !slot)
		return;
	ereport(LOG, (errmsg_internal("PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
								  " target=%u stage=%s slot_xid=%u flags=%u wrap=%u locks=%u "
								  "origin=%u generation=%u uba0=" UINT64_FORMAT
								  " uba1=" UINT64_FORMAT " write_scn=" UINT64_FORMAT
								  " commit_scn=" UINT64_FORMAT " first_lsn=" UINT64_FORMAT,
								  active.call, active.relation, stage, slot->xid, slot->flags,
								  slot->wrap, slot->lock_count,
								  (unsigned)uba_origin_node_id(slot->undo_segment_head), slot->wrap,
								  slot->undo_segment_head.raw[0], slot->undo_segment_head.raw[1],
								  slot->write_scn, slot->commit_scn, slot->first_change_lsn)));
}
void
cluster_catalog_trace_event(const char *stage, const char *branch, int64 value)
{
	if (!active.call)
		return;
	ereport(LOG, (errmsg_internal("PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
								  " target=%u stage=%s branch=%s value=" INT64_FORMAT,
								  active.call, active.relation, stage, branch, value)));
}
int
cluster_catalog_trace_result(const char *function, int line, int value)
{
	if (active.call)
		ereport(LOG, (errmsg_internal("PGRAC_CATALOG_TRACE call=" UINT64_FORMAT
									  " target=%u stage=return function=%s line=%d value=%d",
									  active.call, active.relation, function, line, value)));
	return value;
}
CommandId
cluster_catalog_trace_cid(const char *kind, CommandId value)
{
	cluster_catalog_trace_event("NATIVE_SELF", kind, value);
	return value;
}
void
cluster_catalog_trace_catalog(const char *stage, Oid relation, HeapTuple tuple, CommandId curcid)
{
	if (!cluster_trace_visibility_enabled
		|| !cluster_visibility_trace_contains(&cluster_trace_visibility_filter, relation))
		return;
	if (!HeapTupleIsValid(tuple)) {
		ereport(LOG, (errmsg_internal("PGRAC_CATALOG_TRACE target=%u stage=%s curcid=%u found=0",
									  relation, stage, curcid)));
		return;
	}
	ereport(
		LOG,
		(errmsg_internal(
			"PGRAC_CATALOG_TRACE target=%u stage=%s curcid=%u found=1 table=%u block=%u off=%u "
			"xmin=%u xmax=%u raw_cid=%u combo=%d infomask=%u infomask2=%u",
			relation, stage, curcid, tuple->t_tableOid, ItemPointerGetBlockNumber(&tuple->t_self),
			ItemPointerGetOffsetNumber(&tuple->t_self), HeapTupleHeaderGetRawXmin(tuple->t_data),
			HeapTupleHeaderGetRawXmax(tuple->t_data), HeapTupleHeaderGetRawCommandId(tuple->t_data),
			(tuple->t_data->t_infomask & HEAP_COMBOCID) != 0, tuple->t_data->t_infomask,
			tuple->t_data->t_infomask2)));
	if (tuple->t_tableOid == RelationRelationId) {
		Form_pg_class row = (Form_pg_class)GETSTRUCT(tuple);
		ereport(LOG, (errmsg_internal(
						 "PGRAC_CATALOG_TRACE target=%u stage=%s relnatts=%d relfilenode=%u",
						 relation, stage, row->relnatts, row->relfilenode)));
	} else if (tuple->t_tableOid == AttributeRelationId) {
		Form_pg_attribute row = (Form_pg_attribute)GETSTRUCT(tuple);
		ereport(LOG,
				(errmsg_internal(
					"PGRAC_CATALOG_TRACE target=%u stage=%s attnum=%d atttypid=%u atttypmod=%d",
					relation, stage, row->attnum, row->atttypid, row->atttypmod)));
	}
}
void
cluster_catalog_trace_descriptor(const char *stage, Relation relation, int attnum, CommandId curcid)
{
	bool valid;
	if (!cluster_trace_visibility_enabled
		|| !cluster_visibility_trace_contains(&cluster_trace_visibility_filter,
											  RelationGetRelid(relation)))
		return;
	valid = attnum > 0 && attnum <= relation->rd_att->natts;
	ereport(LOG,
			(errmsg_internal(
				"PGRAC_CATALOG_TRACE target=%u stage=%s curcid=%u relnatts=%d natts=%d attnum=%d "
				"in_bounds=%d atttypid=%u atttypmod=%d rd_isvalid=%d",
				RelationGetRelid(relation), stage, curcid, relation->rd_rel->relnatts,
				relation->rd_att->natts, attnum, valid,
				valid ? TupleDescAttr(relation->rd_att, attnum - 1)->atttypid : InvalidOid,
				valid ? TupleDescAttr(relation->rd_att, attnum - 1)->atttypmod : -1,
				relation->rd_isvalid)));
}
void
cluster_catalog_trace_type(Oid relation, Oid target, int32 typmod, Oid source, CommandId curcid)
{
	if (!cluster_trace_visibility_enabled
		|| !cluster_visibility_trace_contains(&cluster_trace_visibility_filter, relation))
		return;
	ereport(LOG,
			(errmsg_internal(
				"PGRAC_CATALOG_TRACE target=%u stage=cookDefault.coerce curcid=%u source_type=%u "
				"target_type=%u typmod=%d attnum=%d attnum_known=%d",
				relation, curcid, source, target, typmod,
				active.relation == relation ? active.attnum : 0,
				active.relation == relation && active.attnum > 0)));
}
