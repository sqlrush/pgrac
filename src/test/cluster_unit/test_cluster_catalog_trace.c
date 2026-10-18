/* Passive catalog trace: real logger, captured server log boundary. */
#include "postgres.h"
#include <stdarg.h>
#include "access/htup_details.h"
#include "catalog/pg_class.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_type.h"
#include "cluster/cluster_uba.h"
#include "cluster/cluster_catalog_trace.h"
#include "cluster/cluster_guc.h"
#undef printf
#undef fprintf
#undef snprintf
#undef vsnprintf
#include "unit_test.h"
UT_DEFINE_GLOBALS();
bool cluster_trace_visibility_enabled = false;
ClusterVisibilityTraceFilter cluster_trace_visibility_filter = { 0 };
static void
set_filter(const char *value)
{
	if (!cluster_visibility_trace_parse(value, &cluster_trace_visibility_filter))
		abort();
	cluster_trace_visibility_enabled = cluster_trace_visibility_filter.count != 0;
}
void
ExceptionalCondition(const char *condition, const char *file, int line)
{
	(void)condition;
	(void)file;
	(void)line;
	abort();
}
static int messages;
static char captured[16384];
static char last_message[4096];
bool
errstart(int level, const char *domain)
{
	(void)domain;
	if (level != LOG)
		abort();
	messages++;
	return true;
}
bool
errstart_cold(int level, const char *domain)
{
	return errstart(level, domain);
}
int
errmsg_internal(const char *fmt, ...)
{
	va_list ap;
	va_start(ap, fmt);
	vsnprintf(last_message, sizeof(last_message), fmt, ap);
	va_end(ap);
	snprintf(captured + strlen(captured), sizeof(captured) - strlen(captured), "%s\n",
			 last_message);
	return 0;
}
void
errfinish(const char *file, int line, const char *func)
{
	(void)file;
	(void)line;
	(void)func;
}
static union {
	long align;
	char data[1024];
} storage;
static HeapTupleData tuple;
static SnapshotData snap;
static void
fixture(Oid table, Oid object)
{
	memset(&storage, 0, sizeof(storage));
	memset(&tuple, 0, sizeof(tuple));
	memset(&snap, 0, sizeof(snap));
	tuple.t_data = (HeapTupleHeader)storage.data;
	tuple.t_data->t_hoff = MAXALIGN(SizeofHeapTupleHeader);
	tuple.t_len = sizeof(storage);
	tuple.t_tableOid = table;
	ItemPointerSet(&tuple.t_self, 4, 3);
	if (table == RelationRelationId)
		((Form_pg_class)GETSTRUCT(&tuple))->oid = object;
	if (table == AttributeRelationId)
		((Form_pg_attribute)GETSTRUCT(&tuple))->attrelid = object;
	HeapTupleHeaderSetXmin(tuple.t_data, 100);
	HeapTupleHeaderSetXmax(tuple.t_data, 200);
	HeapTupleHeaderSetCmin(tuple.t_data, 2);
	snap.curcid = 3;
	messages = 0;
	captured[0] = 0;
}
UT_TEST(default_off_does_not_log_or_inspect_tuple)
{
	set_filter("");
	messages = 0;
	captured[0] = 0;
	UT_ASSERT_EQ(cluster_catalog_trace_enter(NULL).call, 0);
	UT_ASSERT_EQ(cluster_catalog_trace_result("gate", 7, 2), 2);
	UT_ASSERT_EQ(messages, 0);
}
UT_TEST(exact_catalog_oid_and_nested_scope_are_preserved)
{
	ClusterCatalogTraceScope outer, inner;
	set_filter("16385");
	fixture(RelationRelationId, 16385);
	outer = cluster_catalog_trace_enter(&tuple);
	cluster_catalog_trace_begin(&tuple, &snap, "live");
	UT_ASSERT(messages > 0);
	UT_ASSERT(strstr(last_message, "xmin=100 xmax=200") != NULL);
	UT_ASSERT(strstr(last_message, "raw_cid=2") != NULL);
	UT_ASSERT(strstr(last_message, "curcid=3") != NULL);
	fixture(RelationRelationId, 16388);
	inner = cluster_catalog_trace_enter(&tuple);
	cluster_catalog_trace_begin(&tuple, &snap, "live");
	cluster_catalog_trace_result("unmatched", 1, 1);
	UT_ASSERT_EQ(messages, 0);
	cluster_catalog_trace_leave(inner);
	cluster_catalog_trace_result("restored", 8, 1);
	UT_ASSERT_EQ(messages, 1);
	UT_ASSERT(strstr(last_message, "restored") != NULL);
	cluster_catalog_trace_leave(outer);
}
UT_TEST(attribute_owner_is_selected_and_values_do_not_change)
{
	ClusterCatalogTraceScope prior;
	char before[1024];
	fixture(AttributeRelationId, 16385);
	memcpy(before, storage.data, sizeof(before));
	prior = cluster_catalog_trace_enter(&tuple);
	cluster_catalog_trace_begin(&tuple, &snap, "live");
	UT_ASSERT_EQ(cluster_catalog_trace_result("gate", 4068, 1), 1);
	UT_ASSERT_EQ(cluster_catalog_trace_result("gate", 4068, 0), 0);
	UT_ASSERT_EQ(memcmp(before, storage.data, sizeof(before)), 0);
	cluster_catalog_trace_leave(prior);
}
UT_TEST(short_catalog_tuple_is_not_deformed)
{
	set_filter("16385");
	fixture(RelationRelationId, 16385);
	tuple.t_len = tuple.t_data->t_hoff;
	cluster_catalog_trace_enter(&tuple);
	UT_ASSERT_EQ(messages, 0);
}
UT_TEST(slot_and_existing_cid_results_are_only_observed)
{
	ClusterItlSlotData slot;
	ClusterCatalogTraceScope prior;
	fixture(RelationRelationId, 16385);
	prior = cluster_catalog_trace_enter(&tuple);
	cluster_catalog_trace_begin(&tuple, &snap, "live");
	memset(&slot, 0, sizeof(slot));
	slot.xid = 100;
	slot.wrap = 7;
	slot.flags = ITL_FLAG_ACTIVE;
	slot.undo_segment_head.raw[0] = 123;
	cluster_catalog_trace_slot("selected", &slot);
	UT_ASSERT(strstr(last_message, "slot_xid=100 flags=1 wrap=7") != NULL);
	UT_ASSERT(strstr(last_message, "uba0=123") != NULL);
	UT_ASSERT_EQ(cluster_catalog_trace_cid("Cmax", 2), 2);
	UT_ASSERT(strstr(last_message, "branch=Cmax value=2") != NULL);
	cluster_catalog_trace_leave(prior);
}
UT_TEST(descriptor_outside_natts_is_logged_without_attribute_access)
{
	RelationData relation;
	FormData_pg_class form;
	union {
		long align;
		char data[1024];
	} desc;
	memset(&relation, 0, sizeof(relation));
	memset(&form, 0, sizeof(form));
	memset(&desc, 0, sizeof(desc));
	relation.rd_id = 16385;
	relation.rd_rel = &form;
	relation.rd_att = (TupleDesc)desc.data;
	relation.rd_att->natts = 1;
	form.relnatts = 1;
	cluster_catalog_trace_descriptor("before", &relation, 2, 3);
	UT_ASSERT(strstr(last_message, "natts=1 attnum=2 in_bounds=0 atttypid=0") != NULL);
	UT_ASSERT_EQ(relation.rd_att->natts, 1);
}
UT_TEST(enter_saves_context_without_logging_before_guard)
{
	ClusterCatalogTraceScope previous;
	set_filter("16385");
	fixture(RelationRelationId, 16385);
	previous = cluster_catalog_trace_enter(&tuple);
	UT_ASSERT_EQ(messages, 0);
	cluster_catalog_trace_leave(previous);
}

UT_TEST(oid_list_validates_bounds_duplicates_and_failed_parse_is_atomic)
{
	ClusterVisibilityTraceFilter filter = { 0 };
	ClusterVisibilityTraceFilter before;
	char values[1024] = "";
	int i;
	UT_ASSERT(cluster_visibility_trace_parse("1259, 1249,1247,16385,4294967295,1259", &filter));
	UT_ASSERT_EQ(filter.count, 5);
	UT_ASSERT(cluster_visibility_trace_contains(&filter, (Oid)4294967295U));
	before = filter;
	UT_ASSERT(!cluster_visibility_trace_parse("1259,4294967296", &filter));
	UT_ASSERT_EQ(memcmp(&before, &filter, sizeof(filter)), 0);
	UT_ASSERT(!cluster_visibility_trace_parse("0", &filter));
	UT_ASSERT(!cluster_visibility_trace_parse("1259,", &filter));
	UT_ASSERT(!cluster_visibility_trace_parse("-1", &filter));
	UT_ASSERT(!cluster_visibility_trace_parse("1259 1249", &filter));
	for (i = 1; i <= 65; i++)
		snprintf(values + strlen(values), sizeof(values) - strlen(values), "%s%d",
				 i == 1 ? "" : ",", i);
	UT_ASSERT(!cluster_visibility_trace_parse(values, &filter));
	UT_ASSERT(cluster_visibility_trace_parse(" \t", &filter));
	UT_ASSERT_EQ(filter.count, 0);
}
UT_TEST(enabled_trace_has_required_fields_and_distinguishes_full_input)
{
	ClusterCatalogTraceScope previous, defaults;
	ClusterItlSlotData slot = { 0 };
	const char *fields[] = { "input_kind=full_scratch",
							 "xmin=100",
							 "xmax=200",
							 "infomask=",
							 "raw_cid=2",
							 "combo=0",
							 "curcid=3",
							 "effective_xmax=200",
							 "slot_xid=100",
							 "origin=0",
							 "generation=7",
							 "attnum=2",
							 "target_type=23" };
	size_t i;
	set_filter("1259,1249,1247,16385");
	fixture(RelationRelationId, 16385);
	previous = cluster_catalog_trace_enter(&tuple);
	cluster_catalog_trace_begin(&tuple, &snap, "full_scratch");
	slot.xid = 100;
	slot.flags = ITL_FLAG_ACTIVE;
	slot.wrap = 7;
	slot.undo_segment_head.raw[0] = 1;
	cluster_catalog_trace_slot("selected", &slot);
	defaults = cluster_catalog_trace_default_enter(16385, 2);
	cluster_catalog_trace_type(16385, 23, -1, 23, 3);
	cluster_catalog_trace_leave(defaults);
	for (i = 0; i < lengthof(fields); i++)
		UT_ASSERT(strstr(captured, fields[i]) != NULL);
	cluster_catalog_trace_leave(previous);
	fixture(TypeRelationId, 0);
	previous = cluster_catalog_trace_enter(&tuple);
	cluster_catalog_trace_begin(&tuple, &snap, "live");
	UT_ASSERT(messages > 0);
	cluster_catalog_trace_leave(previous);
}
int
main(void)
{
	UT_PLAN(9);
	UT_RUN(oid_list_validates_bounds_duplicates_and_failed_parse_is_atomic);
	UT_RUN(enabled_trace_has_required_fields_and_distinguishes_full_input);
	UT_RUN(enter_saves_context_without_logging_before_guard);
	UT_RUN(default_off_does_not_log_or_inspect_tuple);
	UT_RUN(exact_catalog_oid_and_nested_scope_are_preserved);
	UT_RUN(attribute_owner_is_selected_and_values_do_not_change);
	UT_RUN(short_catalog_tuple_is_not_deformed);
	UT_RUN(slot_and_existing_cid_results_are_only_observed);
	UT_RUN(descriptor_outside_natts_is_logged_without_attribute_access);
	UT_DONE();
	return ut_failed_count ? 1 : 0;
}
