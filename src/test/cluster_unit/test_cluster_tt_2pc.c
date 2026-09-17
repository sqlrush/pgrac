/*-------------------------------------------------------------------------
 *
 * test_cluster_tt_2pc.c
 *	  cluster_unit tests for the spec-3.15 2PC record serialize/parse
 *	  layer (pure functions; no backend linked beyond the record TU).
 *
 *	      S1  round-trip: single binding
 *	      S2  round-trip: multi binding + sub-links (field-exact)
 *	      S3  empty payload round-trip (header-only record)
 *	      S4  cap rejection: bindings > MAX -> serialize returns 0
 *	      S5  cap rejection: sublinks > MAX -> serialize returns 0
 *	      S6  dstcap too small -> serialize returns 0
 *	      S7  CRC corruption (flip one payload byte) -> parse false
 *	      S8  version mismatch -> parse false
 *	      S9  magic mismatch -> parse false
 *	      S10 length mismatch (truncated / padded) -> parse false
 *	      S11 count fields tampered (crc re-stamped) -> length check trips
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_tt_2pc.c
 *
 * NOTES
 *	  This is a pgrac-original file.
 *	  Spec: spec-3.15-2pc-prepared-visibility.md (FROZEN v0.2) §2.1/§4.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_tt_2pc.h"

#ifndef TT_2PC_SOURCE_PATH
#error "TT_2PC_SOURCE_PATH must identify cluster_tt_2pc.c"
#endif
#ifndef TWOPHASE_SOURCE_PATH
#error "TWOPHASE_SOURCE_PATH must identify twophase.c"
#endif

/* Drop PG's port.h printf -> pg_printf override; unit_test.h uses
 * stdlib printf (libpgport is linked for CRC32C only). */
#undef printf

#include "unit_test.h"

UT_DEFINE_GLOBALS();


static char *
read_source(const char *path)
{
	FILE *fp;
	char *source;
	long length;
	size_t nread;

	fp = fopen(path, "rb");
	UT_ASSERT_NOT_NULL(fp);
	if (fp == NULL)
		return NULL;
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_END), 0);
	length = ftell(fp);
	UT_ASSERT(length > 0);
	UT_ASSERT_EQ(fseek(fp, 0, SEEK_SET), 0);
	if (length <= 0) {
		fclose(fp);
		return NULL;
	}
	source = malloc((size_t)length + 1);
	UT_ASSERT_NOT_NULL(source);
	if (source == NULL) {
		fclose(fp);
		return NULL;
	}
	nread = fread(source, 1, (size_t)length, fp);
	fclose(fp);
	UT_ASSERT_EQ(nread, (size_t)length);
	if (nread != (size_t)length) {
		free(source);
		return NULL;
	}
	source[length] = '\0';
	return source;
}

static char *
read_tt_2pc_source(void)
{
	return read_source(TT_2PC_SOURCE_PATH);
}


static ClusterTT2PCBinding
mk_binding(uint32 seg, uint16 off, uint16 wrap, uint32 epoch, TransactionId xid)
{
	ClusterTT2PCBinding b;

	memset(&b, 0, sizeof(b));
	b.undo_segment_id = seg;
	b.slot_offset = off;
	b.wrap = wrap;
	b.cluster_epoch = epoch;
	b.xid = xid;
	return b;
}

static ClusterTT2PCSubLink
mk_link(uint32 cxid, uint32 pxid)
{
	ClusterTT2PCSubLink l;

	memset(&l, 0, sizeof(l));
	l.child_key.origin_node_id = 1;
	l.child_key.undo_segment_id = 7;
	l.child_key.tt_slot_id = 3;
	l.child_key.cluster_epoch = 42;
	l.child_key.local_xid = cxid;
	l.parent_key = l.child_key;
	l.parent_key.local_xid = pxid;
	return l;
}


UT_TEST(test_s1_roundtrip_single_binding)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len == cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION, 1, 0)), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.nbindings, 1);
	UT_ASSERT_EQ((int)p.nsublinks, 0);
	UT_ASSERT_EQ((int)p.bindings[0].undo_segment_id, 11);
	UT_ASSERT_EQ((int)p.bindings[0].slot_offset, 5);
	UT_ASSERT_EQ((int)p.bindings[0].wrap, 2);
	UT_ASSERT_EQ((int)p.bindings[0].cluster_epoch, 9);
	UT_ASSERT_EQ((int)p.bindings[0].xid, 1001);
}

UT_TEST(test_s2_roundtrip_multi_with_sublinks)
{
	ClusterTT2PCBinding bs[3];
	ClusterTT2PCSubLink ls[2];
	char buf[1024];
	uint32 len;
	ClusterTT2PCParsed p;
	int i;

	for (i = 0; i < 3; i++)
		bs[i] = mk_binding(100 + i, (uint16)i, (uint16)(i * 7), 5, 2000 + i);
	ls[0] = mk_link(3001, 2000);
	ls[1] = mk_link(3002, 2000);

	len = cluster_tt_2pc_serialize(bs, NULL, 3, ls, 2, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len > 0), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.nbindings, 3);
	UT_ASSERT_EQ((int)p.nsublinks, 2);
	for (i = 0; i < 3; i++) {
		UT_ASSERT_EQ((int)p.bindings[i].undo_segment_id, 100 + i);
		UT_ASSERT_EQ((int)p.bindings[i].xid, 2000 + i);
	}
	UT_ASSERT_EQ((int)p.sublinks[0].child_key.local_xid, 3001);
	UT_ASSERT_EQ((int)p.sublinks[1].child_key.local_xid, 3002);
	UT_ASSERT_EQ((int)p.sublinks[1].parent_key.local_xid, 2000);
}

UT_TEST(test_s3_roundtrip_empty)
{
	char buf[64];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(NULL, NULL, 0, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len == sizeof(ClusterTT2PCRecord)), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.nbindings, 0);
	UT_ASSERT_EQ((int)p.nsublinks, 0);
	UT_ASSERT_EQ((int)(p.bindings == NULL), 1);
	UT_ASSERT_EQ((int)(p.sublinks == NULL), 1);
}

UT_TEST(test_s4_cap_bindings_reject)
{
	ClusterTT2PCBinding bs[CLUSTER_TT_2PC_MAX_BINDINGS + 1];
	char buf[4096];
	int i;

	for (i = 0; i <= CLUSTER_TT_2PC_MAX_BINDINGS; i++)
		bs[i] = mk_binding(1, 0, 0, 1, 100 + i);
	UT_ASSERT_EQ((int)cluster_tt_2pc_serialize(bs, NULL, CLUSTER_TT_2PC_MAX_BINDINGS + 1, NULL, 0,
											   buf, sizeof(buf)),
				 0);
}

UT_TEST(test_s5_cap_sublinks_reject)
{
	ClusterTT2PCSubLink ls[CLUSTER_TT_2PC_MAX_SUBLINKS + 1];
	char buf[8192];
	int i;

	for (i = 0; i <= CLUSTER_TT_2PC_MAX_SUBLINKS; i++)
		ls[i] = mk_link(100 + i, 50);
	UT_ASSERT_EQ((int)cluster_tt_2pc_serialize(NULL, NULL, 0, ls, CLUSTER_TT_2PC_MAX_SUBLINKS + 1,
											   buf, sizeof(buf)),
				 0);
}

UT_TEST(test_s6_dstcap_too_small_reject)
{
	ClusterTT2PCBinding b = mk_binding(1, 0, 0, 1, 100);
	char buf[8];

	UT_ASSERT_EQ((int)cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf)), 0);
}

UT_TEST(test_s7_crc_corruption_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	buf[sizeof(ClusterTT2PCRecord) + 3] ^= 0x40; /* flip a payload bit */
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

UT_TEST(test_s8_version_mismatch_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;
	ClusterTT2PCRecord *hdr = (ClusterTT2PCRecord *)buf;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	hdr->version = CLUSTER_TT_2PC_VERSION + 1;
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

UT_TEST(test_s9_magic_mismatch_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;
	ClusterTT2PCRecord *hdr = (ClusterTT2PCRecord *)buf;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	hdr->magic = 0xDEADBEEF;
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

UT_TEST(test_s10_length_mismatch_reject)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len - 1, &p), 0); /* truncated */
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len + 1, &p), 0); /* padded */
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, sizeof(ClusterTT2PCRecord) - 1, &p), 0);
}

UT_TEST(test_s11_count_tamper_trips_length_check)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;
	ClusterTT2PCRecord *hdr = (ClusterTT2PCRecord *)buf;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	/* Claim 2 bindings: even with a freshly-stamped CRC the length
	 * arithmetic must trip (defence-in-depth ordering). */
	hdr->nbindings = 2;
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 0);
}

/* spec-4.8 D7-A: v2 heads[] section round-trips parallel to bindings[]. */
UT_TEST(test_s12_v2_heads_roundtrip)
{
	ClusterTT2PCBinding bs[2];
	UBA heads[2];
	char buf[512];
	uint32 len;
	ClusterTT2PCParsed p;

	bs[0] = mk_binding(11, 5, 2, 9, 1001);
	bs[1] = mk_binding(12, 6, 3, 9, 1002);
	memset(heads, 0, sizeof(heads));
	heads[0].raw[0] = 0xABCD1234;
	heads[0].raw[1] = 0x5678;
	heads[1].raw[0] = 0x0; /* binding 1: no head (InvalidUba) */
	heads[1].raw[1] = 0x0;

	len = cluster_tt_2pc_serialize(bs, heads, 2, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)(len == cluster_tt_2pc_record_size(CLUSTER_TT_2PC_VERSION, 2, 0)), 1);
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.version, CLUSTER_TT_2PC_VERSION);
	UT_ASSERT_EQ((int)(p.heads != NULL), 1);
	UT_ASSERT_EQ((int)(p.heads[0].raw[0] == 0xABCD1234), 1);
	UT_ASSERT_EQ((int)(p.heads[0].raw[1] == 0x5678), 1);
	UT_ASSERT_EQ((int)UBA_is_invalid(p.heads[1]), 1); /* binding 1: InvalidUba */
}

/* spec-4.8 D7-A: NULL heads serializes an all-InvalidUba heads[] section; the
 * record is still v2 (heads != NULL on parse) -> D7 no-op per binding. */
UT_TEST(test_s13_v2_null_heads_all_invalid)
{
	ClusterTT2PCBinding b = mk_binding(11, 5, 2, 9, 1001);
	char buf[256];
	uint32 len;
	ClusterTT2PCParsed p;

	len = cluster_tt_2pc_serialize(&b, NULL, 1, NULL, 0, buf, sizeof(buf));
	UT_ASSERT_EQ((int)cluster_tt_2pc_parse_record(buf, len, &p), 1);
	UT_ASSERT_EQ((int)p.version, CLUSTER_TT_2PC_VERSION);
	UT_ASSERT_EQ((int)(p.heads != NULL), 1);
	UT_ASSERT_EQ((int)UBA_is_invalid(p.heads[0]), 1);
}

/*
 * Spec 8.4A I18/I19: COMMIT/ROLLBACK PREPARED is an ordinary live modifier.
 * It must own the common modifier debt before parsing or touching durable TT,
 * recheck that admission at each binding, and release through one ERROR-safe
 * funnel.  This source-edge test complements the pure record fixture without
 * linking the full backend-only prefinish call graph.
 */
UT_TEST(test_s14_prefinish_is_modifier_gated_and_error_safe)
{
	char *source = read_tt_2pc_source();
	const char *start;
	const char *end;
	const char *enter;
	const char *try_block;
	const char *parse;
	const char *loop;
	const char *commit_recheck;
	const char *durable_commit;
	const char *abort_recheck;
	const char *durable_abort;
	const char *head_recheck;
	const char *durable_set_head;
	const char *finally_block;
	const char *leave;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_tt_twophase_prefinish(");
	end = start == NULL ? NULL : strstr(start, "\n}\n\n#endif /* USE_PGRAC_CLUSTER */");
	enter = start == NULL ? NULL : strstr(start, "cluster_semantic_activation_modifier_enter(");
	try_block = start == NULL ? NULL : strstr(start, "PG_TRY();");
	parse = start == NULL ? NULL : strstr(start, "parse_or_corrupt(");
	loop = start == NULL ? NULL : strstr(start, "for (i = 0; i < p.nbindings; i++)");
	commit_recheck
		= loop == NULL ? NULL : strstr(loop, "cluster_tt_twophase_modifier_recheck_or_error(");
	durable_commit = start == NULL ? NULL : strstr(start, "cluster_tt_slot_durable_commit(");
	abort_recheck = durable_commit == NULL
						? NULL
						: strstr(durable_commit, "cluster_tt_twophase_modifier_recheck_or_error(");
	durable_abort = start == NULL ? NULL : strstr(start, "cluster_tt_slot_durable_abort(");
	head_recheck = durable_abort == NULL
					   ? NULL
					   : strstr(durable_abort, "cluster_tt_twophase_modifier_recheck_or_error(");
	durable_set_head = start == NULL ? NULL : strstr(start, "cluster_tt_slot_durable_set_head(");
	finally_block = start == NULL ? NULL : strstr(start, "PG_FINALLY();");
	leave = finally_block == NULL ? NULL
								  : strstr(finally_block, "cluster_semantic_activation_leave(");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(enter);
	UT_ASSERT_NOT_NULL(try_block);
	UT_ASSERT_NOT_NULL(parse);
	UT_ASSERT_NOT_NULL(loop);
	UT_ASSERT_NOT_NULL(commit_recheck);
	UT_ASSERT_NOT_NULL(durable_commit);
	UT_ASSERT_NOT_NULL(abort_recheck);
	UT_ASSERT_NOT_NULL(durable_abort);
	UT_ASSERT_NOT_NULL(head_recheck);
	UT_ASSERT_NOT_NULL(durable_set_head);
	UT_ASSERT_NOT_NULL(finally_block);
	UT_ASSERT_NOT_NULL(leave);
	if (start != NULL && end != NULL && enter != NULL && try_block != NULL && parse != NULL
		&& loop != NULL && commit_recheck != NULL && durable_commit != NULL && abort_recheck != NULL
		&& durable_abort != NULL && head_recheck != NULL && durable_set_head != NULL
		&& finally_block != NULL && leave != NULL)
		UT_ASSERT(start < enter && enter < try_block && try_block < parse && parse < loop
				  && loop < commit_recheck && commit_recheck < durable_commit
				  && durable_commit < abort_recheck && abort_recheck < durable_abort
				  && durable_abort < head_recheck && head_recheck < durable_set_head
				  && durable_set_head < finally_block && finally_block < leave && leave < end);
	free(source);
}

/* RF-SIDE: a survivor resolving a failed-origin prepared transaction must
 * preserve the binding's striped segment owner in the overlay key. */
UT_TEST(test_s15_prefinish_preserves_binding_origin)
{
	char *source = read_tt_2pc_source();
	const char *start;
	const char *end;
	const char *derive;
	const char *assign;
	const char *wrong;

	if (source == NULL)
		return;
	start = strstr(source, "\ncluster_tt_twophase_prefinish(");
	end = start == NULL ? NULL : strstr(start, "\n}\n\n#endif /* USE_PGRAC_CLUSTER */");
	derive = start == NULL
				 ? NULL
				 : strstr(start, "cluster_tt_2pc_binding_origin_node(b, &origin_node_id)");
	assign = start == NULL ? NULL : strstr(start, "key.origin_node_id = origin_node_id;");
	wrong = start == NULL ? NULL : strstr(start, "key.origin_node_id = (uint16)cluster_node_id;");

	UT_ASSERT_NOT_NULL(start);
	UT_ASSERT_NOT_NULL(end);
	UT_ASSERT_NOT_NULL(derive);
	UT_ASSERT_NOT_NULL(assign);
	UT_ASSERT(wrong == NULL || (end != NULL && wrong > end));
	if (start != NULL && end != NULL && derive != NULL && assign != NULL) {
		UT_ASSERT(derive < assign);
		UT_ASSERT(assign < end);
	}
	free(source);
}

/* RF-SIDE RFSIDE-V2-A: the durable v2 row is only a projection.  Recovery
 * install may report success only after the native prepared owner is fully
 * activated; an interrupted activation stays unfinishable and unpublished. */
UT_TEST(test_s16_recovery_pending_activates_native_owner_before_success)
{
	char *source = read_source(TWOPHASE_SOURCE_PATH);
	const char *install;
	const char *install_end;
	const char *mark_guts;
	const char *mark_pending;
	const char *load_subxids;
	const char *mark_prepared;
	const char *recover_records;
	const char *catch_block;
	const char *panic_transition;
	const char *clear_pending;
	const char *post_prepare;
	const char *postread;
	const char *lock_gxact;
	const char *lock_end;
	const char *view;
	const char *view_end;
	const char *prepared_predicate;
	const char *prepared_predicate_end;

	if (source == NULL)
		return;
	install = strstr(source, "\nTwoPhaseRecoveryPendingInstall(");
	install_end
		= install == NULL ? NULL : strstr(install, "\n}\n\n/* Caller holds TwoPhaseStateLock");
	mark_guts = install == NULL ? NULL : strstr(install, "MarkAsPreparingGuts(");
	mark_pending
		= install == NULL ? NULL : strstr(install, "gxact->recovery_activation_pending = true;");
	load_subxids = install == NULL ? NULL : strstr(install, "GXactLoadSubxactData(");
	mark_prepared = install == NULL ? NULL : strstr(install, "MarkAsPrepared(gxact, true);");
	recover_records
		= install == NULL
			  ? NULL
			  : strstr(install, "ProcessRecords(bufptr, xid, twophase_recover_callbacks);");
	catch_block = recover_records == NULL ? NULL : strstr(recover_records, "PG_CATCH();");
	panic_transition = catch_block == NULL ? NULL : strstr(catch_block, "ereport(PANIC");
	clear_pending
		= install == NULL ? NULL : strstr(install, "gxact->recovery_activation_pending = false;");
	post_prepare = install == NULL ? NULL : strstr(install, "PostPrepare_Twophase();");
	postread = post_prepare == NULL
				   ? NULL
				   : strstr(post_prepare, "postread = ReadTwoPhaseFile(xid, false);");
	lock_gxact = strstr(source, "\nLockGXact(");
	lock_end = lock_gxact == NULL ? NULL : strstr(lock_gxact, "\n}\n\n/*\n * RemoveGXact");
	view = strstr(source, "\npg_prepared_xact(PG_FUNCTION_ARGS)");
	view_end = view == NULL ? NULL : strstr(view, "\n}\n\n/*\n * TwoPhaseGetGXact");
	prepared_predicate = strstr(source, "\nTwoPhaseTransactionIdIsPrepared(");
	prepared_predicate_end = prepared_predicate == NULL
								 ? NULL
								 : strstr(prepared_predicate, "\n}\n\n\n/* Working status");

	UT_ASSERT_NOT_NULL(install);
	UT_ASSERT_NOT_NULL(install_end);
	UT_ASSERT_NOT_NULL(mark_guts);
	UT_ASSERT_NOT_NULL(mark_pending);
	UT_ASSERT_NOT_NULL(load_subxids);
	UT_ASSERT_NOT_NULL(mark_prepared);
	UT_ASSERT_NOT_NULL(recover_records);
	UT_ASSERT_NOT_NULL(catch_block);
	UT_ASSERT_NOT_NULL(panic_transition);
	UT_ASSERT_NOT_NULL(clear_pending);
	UT_ASSERT_NOT_NULL(post_prepare);
	UT_ASSERT_NOT_NULL(postread);
	UT_ASSERT_NOT_NULL(lock_gxact);
	UT_ASSERT_NOT_NULL(lock_end);
	UT_ASSERT_NOT_NULL(view);
	UT_ASSERT_NOT_NULL(view_end);
	UT_ASSERT_NOT_NULL(prepared_predicate);
	UT_ASSERT_NOT_NULL(prepared_predicate_end);
	if (install != NULL && install_end != NULL && mark_guts != NULL && mark_pending != NULL
		&& load_subxids != NULL && mark_prepared != NULL && recover_records != NULL
		&& catch_block != NULL && panic_transition != NULL && clear_pending != NULL
		&& post_prepare != NULL && postread != NULL)
		UT_ASSERT(install < mark_guts && mark_guts < mark_pending && mark_pending < load_subxids
				  && load_subxids < mark_prepared && mark_prepared < recover_records
				  && recover_records < catch_block && catch_block < panic_transition
				  && panic_transition < clear_pending && clear_pending < post_prepare
				  && post_prepare < postread && postread < install_end);
	if (lock_gxact != NULL && lock_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(lock_gxact, "gxact->recovery_activation_pending"));
	if (view != NULL && view_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(view, "gxact->recovery_activation_pending"));
	if (prepared_predicate != NULL && prepared_predicate_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(prepared_predicate, "!gxact->recovery_activation_pending"));
	free(source);
}

/* RF-SIDE D-SIDE-03: a prepared transaction reconstructed by failed-origin
 * recovery is owned by the recovery path until matching terminal redo and
 * canonical TT/undo truth resolve it.  Ordinary COMMIT/ROLLBACK PREPARED must
 * not be able to steal that responsibility after native activation finishes. */
UT_TEST(test_s17_recovery_pending_remains_reco_owned_after_activation)
{
	char *source = read_source(TWOPHASE_SOURCE_PATH);
	const char *definition;
	const char *definition_end;
	const char *install;
	const char *install_end;
	const char *mark_owned;
	const char *clear_activation;
	const char *remark_owned;
	const char *lock_gxact;
	const char *lock_end;

	if (source == NULL)
		return;
	definition = strstr(source, "typedef struct GlobalTransactionData {");
	definition_end = definition == NULL ? NULL : strstr(definition, "} GlobalTransactionData;");
	install = strstr(source, "\nTwoPhaseRecoveryPendingInstall(");
	install_end
		= install == NULL ? NULL : strstr(install, "\n}\n\n/* Caller holds TwoPhaseStateLock");
	mark_owned = install == NULL ? NULL : strstr(install, "gxact->recovery_managed = true;");
	clear_activation
		= install == NULL ? NULL : strstr(install, "gxact->recovery_activation_pending = false;");
	remark_owned = clear_activation == NULL
					   ? NULL
					   : strstr(clear_activation, "gxact->recovery_managed = true;");
	lock_gxact = strstr(source, "\nLockGXact(");
	lock_end = lock_gxact == NULL ? NULL : strstr(lock_gxact, "\n}\n\n/*\n * RemoveGXact");

	UT_ASSERT_NOT_NULL(definition);
	UT_ASSERT_NOT_NULL(definition_end);
	UT_ASSERT_NOT_NULL(install);
	UT_ASSERT_NOT_NULL(install_end);
	UT_ASSERT_NOT_NULL(mark_owned);
	UT_ASSERT_NOT_NULL(clear_activation);
	UT_ASSERT_NOT_NULL(remark_owned);
	UT_ASSERT_NOT_NULL(lock_gxact);
	UT_ASSERT_NOT_NULL(lock_end);
	if (definition != NULL && definition_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(definition, "bool recovery_managed;"));
	if (install != NULL && install_end != NULL && mark_owned != NULL && clear_activation != NULL
		&& remark_owned != NULL)
		UT_ASSERT(install < mark_owned && mark_owned < clear_activation
				  && clear_activation < remark_owned && remark_owned < install_end);
	if (lock_gxact != NULL && lock_end != NULL)
		UT_ASSERT_NOT_NULL(strstr(lock_gxact, "gxact->recovery_managed"));
	free(source);
}

/* Only a fully released RF-SIDE terminal receipt may leave the unresolved
 * prepared count.  Native finish's transient valid=false window still counts
 * until RemoveGXact, so undo retention cannot drain ahead of callbacks. */
UT_TEST(test_s20_prepared_count_excludes_only_terminal_cleanup_receipts)
{
	char *source = read_source(TWOPHASE_SOURCE_PATH);
	const char *counter;
	const char *counter_end;

	if (source == NULL)
		return;
	counter = strstr(source, "\nGetNumberOfPreparedTransactions(void)");
	counter_end = counter == NULL
					  ? NULL
					  : strstr(counter, "\n}\n#endif\t\t\t\t\t\t\t/* USE_PGRAC_CLUSTER */");
	UT_ASSERT_NOT_NULL(counter);
	UT_ASSERT_NOT_NULL(counter_end);
	if (counter != NULL && counter_end != NULL) {
		UT_ASSERT_NOT_NULL(
			strstr(counter, "!TwoPhaseState->prepXacts[i]->recovery_terminal_cleanup_pending"));
		UT_ASSERT(strstr(counter, "prepXacts[i]->valid") == NULL
				  || strstr(counter, "prepXacts[i]->valid") > counter_end);
	}
	free(source);
}

/* Restart/new-actor closure uses only existing authorities: the current
 * FAIL_STOP dead set covers the crash-before-projection window, while an exact
 * v2 PREPARED row is a conservative deny after publication.  Neither arm can
 * grant PREPARED or terminal truth, and no new persistent marker is allowed. */
UT_TEST(test_s18_restart_finish_fence_reuses_root_and_projection)
{
	char *source = read_source(TWOPHASE_SOURCE_PATH);
	const char *helper;
	const char *helper_end;
	const char *event_read;
	const char *dead_test;
	const char *digest;
	const char *projection;
	const char *lock_gxact;
	const char *lock_end;
	const char *claim_owner;
	const char *release_owner_lock;
	const char *finish_fence;

	if (source == NULL)
		return;
	helper = strstr(source, "\nTwoPhaseRecoveryFinishIsFenced(");
	helper_end = helper == NULL ? NULL : strstr(helper, "\n#endif\n\n/*\n * LockGXact");
	event_read = helper == NULL ? NULL : strstr(helper, "cluster_reconfig_get_last_event(");
	dead_test = helper == NULL ? NULL : strstr(helper, "RECONFIG_KIND_FAIL_STOP");
	digest = helper == NULL ? NULL : strstr(helper, "cluster_remote_xact_prepare_digest_v2(");
	projection = helper == NULL ? NULL : strstr(helper, "cluster_remote_xact_pending_matches_v2(");
	lock_gxact = strstr(source, "\nLockGXact(");
	lock_end = lock_gxact == NULL ? NULL : strstr(lock_gxact, "\n}\n\n/*\n * RemoveGXact");
	claim_owner
		= lock_gxact == NULL ? NULL : strstr(lock_gxact, "gxact->locking_backend = MyBackendId;");
	release_owner_lock
		= claim_owner == NULL ? NULL : strstr(claim_owner, "LWLockRelease(TwoPhaseStateLock);");
	finish_fence
		= lock_gxact == NULL ? NULL : strstr(lock_gxact, "TwoPhaseRecoveryFinishIsFenced(");

	UT_ASSERT_NOT_NULL(helper);
	UT_ASSERT_NOT_NULL(helper_end);
	UT_ASSERT_NOT_NULL(event_read);
	UT_ASSERT_NOT_NULL(dead_test);
	UT_ASSERT_NOT_NULL(digest);
	UT_ASSERT_NOT_NULL(projection);
	UT_ASSERT_NOT_NULL(lock_gxact);
	UT_ASSERT_NOT_NULL(lock_end);
	UT_ASSERT_NOT_NULL(claim_owner);
	UT_ASSERT_NOT_NULL(release_owner_lock);
	UT_ASSERT_NOT_NULL(finish_fence);
	if (helper != NULL && helper_end != NULL && event_read != NULL && dead_test != NULL
		&& digest != NULL && projection != NULL)
		UT_ASSERT(helper < event_read && event_read < dead_test && dead_test < digest
				  && digest < projection && projection < helper_end);
	if (lock_gxact != NULL && lock_end != NULL && claim_owner != NULL && release_owner_lock != NULL
		&& finish_fence != NULL)
		UT_ASSERT(lock_gxact < claim_owner && claim_owner < release_owner_lock
				  && release_owner_lock < finish_fence && finish_fence < lock_end);
	UT_ASSERT(strstr(source, ".rfside") == NULL);
	free(source);
}

/* Matching terminal redo must resolve the already-durable native owner without
 * emitting a second COMMIT/ABORT PREPARED record.  The recovery-only primitive
 * rechecks exact identity under the native owner lock, removes ProcArray before
 * callbacks, and panics on a partially-run callback chain.  It retains the
 * exact native file as a restart receipt until a separate post-OPEN cleanup. */
UT_TEST(test_s19_recovery_terminal_resolves_native_owner_without_new_wal)
{
	char *source = read_source(TWOPHASE_SOURCE_PATH);
	const char *read_exact;
	const char *read_end;
	const char *managed_read;
	const char *read_file;
	const char *resolve;
	const char *resolve_end;
	const char *hold;
	const char *proc_remove;
	const char *callbacks;
	const char *catch_block;
	const char *panic_transition;
	const char *predicate_finish;
	const char *cleanup_mark;
	const char *cleanup;
	const char *cleanup_end;
	const char *remove_gxact;
	const char *remove_file;

	if (source == NULL)
		return;
	read_exact = strstr(source, "\nTwoPhaseRecoveryPendingReadExact(");
	read_end
		= read_exact == NULL
			  ? NULL
			  : strstr(
					read_exact,
					"\n}\n\nTwoPhaseRecoveryPendingResult\nTwoPhaseRecoveryPendingResolveExact(");
	managed_read = read_exact == NULL ? NULL : strstr(read_exact, "gxact->recovery_managed");
	read_file = read_exact == NULL ? NULL : strstr(read_exact, "ReadTwoPhaseFile(");
	resolve = strstr(source, "\nTwoPhaseRecoveryPendingResolveExact(");
	resolve_end
		= resolve == NULL
			  ? NULL
			  : strstr(
					resolve,
					"\n}\n\nTwoPhaseRecoveryPendingResult\nTwoPhaseRecoveryPendingCleanupExact(");
	hold = resolve == NULL ? NULL : strstr(resolve, "HOLD_INTERRUPTS();");
	proc_remove = resolve == NULL ? NULL : strstr(resolve, "ProcArrayRemove(");
	callbacks = resolve == NULL ? NULL : strstr(resolve, "ProcessRecords(");
	catch_block = callbacks == NULL ? NULL : strstr(callbacks, "PG_CATCH();");
	panic_transition = catch_block == NULL ? NULL : strstr(catch_block, "ereport(PANIC");
	predicate_finish = resolve == NULL ? NULL : strstr(resolve, "PredicateLockTwoPhaseFinish(");
	cleanup_mark
		= resolve == NULL ? NULL : strstr(resolve, "recovery_terminal_cleanup_pending = true;");
	cleanup = strstr(source, "\nTwoPhaseRecoveryPendingCleanupExact(");
	cleanup_end = cleanup == NULL
					  ? NULL
					  : strstr(cleanup, "\n}\n#endif\t\t\t\t\t\t\t/* USE_PGRAC_CLUSTER */");
	remove_gxact = cleanup == NULL ? NULL : strstr(cleanup, "RemoveGXact(");
	remove_file = cleanup == NULL ? NULL : strstr(cleanup, "durable_unlink(");

	UT_ASSERT_NOT_NULL(read_exact);
	UT_ASSERT_NOT_NULL(read_end);
	UT_ASSERT_NOT_NULL(managed_read);
	UT_ASSERT_NOT_NULL(read_file);
	UT_ASSERT_NOT_NULL(resolve);
	UT_ASSERT_NOT_NULL(resolve_end);
	UT_ASSERT_NOT_NULL(hold);
	UT_ASSERT_NOT_NULL(proc_remove);
	UT_ASSERT_NOT_NULL(callbacks);
	UT_ASSERT_NOT_NULL(catch_block);
	UT_ASSERT_NOT_NULL(panic_transition);
	UT_ASSERT_NOT_NULL(predicate_finish);
	UT_ASSERT_NOT_NULL(cleanup_mark);
	UT_ASSERT_NOT_NULL(cleanup);
	UT_ASSERT_NOT_NULL(cleanup_end);
	UT_ASSERT_NOT_NULL(remove_gxact);
	UT_ASSERT_NOT_NULL(remove_file);
	if (read_exact != NULL && read_end != NULL && managed_read != NULL && read_file != NULL)
		UT_ASSERT(read_exact < managed_read && managed_read < read_file && read_file < read_end);
	if (resolve != NULL && resolve_end != NULL && hold != NULL && proc_remove != NULL
		&& callbacks != NULL && catch_block != NULL && panic_transition != NULL
		&& predicate_finish != NULL && cleanup_mark != NULL)
		UT_ASSERT(resolve < hold && hold < proc_remove && proc_remove < callbacks
				  && callbacks < catch_block && catch_block < panic_transition
				  && panic_transition < predicate_finish && predicate_finish < cleanup_mark
				  && cleanup_mark < resolve_end);
	if (cleanup != NULL && cleanup_end != NULL && remove_gxact != NULL && remove_file != NULL)
		UT_ASSERT(cleanup < remove_file && remove_file < remove_gxact
				  && remove_gxact < cleanup_end);
	if (resolve != NULL && resolve_end != NULL) {
		const char *duplicate_commit = strstr(resolve, "RecordTransactionCommitPrepared(");
		const char *duplicate_abort = strstr(resolve, "RecordTransactionAbortPrepared(");

		UT_ASSERT(duplicate_commit == NULL || duplicate_commit > resolve_end);
		UT_ASSERT(duplicate_abort == NULL || duplicate_abort > resolve_end);
	}
	free(source);
}

int
main(void)
{
	UT_RUN(test_s1_roundtrip_single_binding);
	UT_RUN(test_s2_roundtrip_multi_with_sublinks);
	UT_RUN(test_s3_roundtrip_empty);
	UT_RUN(test_s4_cap_bindings_reject);
	UT_RUN(test_s5_cap_sublinks_reject);
	UT_RUN(test_s6_dstcap_too_small_reject);
	UT_RUN(test_s7_crc_corruption_reject);
	UT_RUN(test_s8_version_mismatch_reject);
	UT_RUN(test_s9_magic_mismatch_reject);
	UT_RUN(test_s10_length_mismatch_reject);
	UT_RUN(test_s11_count_tamper_trips_length_check);
	UT_RUN(test_s12_v2_heads_roundtrip);
	UT_RUN(test_s13_v2_null_heads_all_invalid);
	UT_RUN(test_s14_prefinish_is_modifier_gated_and_error_safe);
	UT_RUN(test_s15_prefinish_preserves_binding_origin);
	UT_RUN(test_s16_recovery_pending_activates_native_owner_before_success);
	UT_RUN(test_s17_recovery_pending_remains_reco_owned_after_activation);
	UT_RUN(test_s18_restart_finish_fence_reuses_root_and_projection);
	UT_RUN(test_s19_recovery_terminal_resolves_native_owner_without_new_wal);
	UT_RUN(test_s20_prepared_count_excludes_only_terminal_cleanup_receipts);

	UT_DONE();
	return ut_failed_count != 0 ? 1 : 0;
}
