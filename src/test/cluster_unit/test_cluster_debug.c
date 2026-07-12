/*-------------------------------------------------------------------------
 *
 * test_cluster_debug.c
 *	  Compile-time / link-level invariants for the cluster_debug
 *	  diagnostic snapshot framework shipped at stage 0.29.
 *
 *	  Locks:
 *	    - The cluster_dump_state SRF entry-point address resolves at
 *	      link time (validates the always-linked / pg_proc.dat
 *	      contract).
 *	    - cluster_debug.c links cleanly when paired with stubs for
 *	      every cross-module dependency it pulls in (cluster_shmem /
 *	      cluster_guc / cluster_ic / cluster_inject / cluster_pgstat /
 *	      cluster_conf / cluster_elog public API).
 *	    - cluster_inject_get_count + _get_state_at iterators added at
 *	      stage 0.29 work without crashing on out-of-range indices.
 *	    - cluster_pgstat_get_count + _get_at iterators added at stage
 *	      0.29 work without crashing on out-of-range indices.
 *
 *	  End-to-end SRF behaviour (column types, row counts, value
 *	  formatting) is verified on a real PG instance by cluster_tap
 *	  t/017_debug.pl.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/cluster_unit/test_cluster_debug.c
 *
 * NOTES
 *	  This is a pgrac-original file.  cluster_debug.c is a cross-module
 *	  aggregator -- linking it standalone requires stubs for the public
 *	  symbols from seven other cluster_*.o files.  The stubs below are
 *	  the minimum set; the SRF body itself is never invoked from the
 *	  unit test (we only take its address), so stub return values are
 *	  inert.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "cluster/cluster_catalog_stats.h" /* spec-6.14 D10b catalog counter stubs */
#include "cluster/cluster_debug.h"
#include "cluster/cluster_grd.h"		  /* ClusterGrdRecoveryCounters */
#include "cluster/cluster_hang.h"		  /* spec-5.11: ClusterHangDumpData for dump_hang stubs */
#include "cluster/cluster_hang_resolve.h" /* spec-5.12: ClusterHangResolveCounters for dump stubs */
#include "cluster/cluster_reconfig.h"	  /* spec-5.14 D6 touched getter stubs */
#include "cluster/cluster_touched_peers.h"	  /* spec-5.14 D6 self_hex stub */
#include "cluster/cluster_xnode_profile.h"	  /* spec-5.59 D1 profiling gate stubs */
#include "cluster/cluster_xnode_lever.h"	  /* spec-6.12 lever counter stub */
#include "cluster/cluster_hw_lease.h"		  /* spec-6.12d lease counter stub */
#include "cluster/cluster_relmap_authority.h" /* spec-6.14 D5 header-read stub */
#include "cluster/cluster_xid_authority.h"	  /* spec-6.15b XID authority dump stubs */

#undef printf
#undef fprintf
#undef snprintf
#undef sprintf
#undef vsnprintf
#undef vfprintf
#undef vprintf
#undef vsprintf
#undef strerror
#undef strerror_r

#include "unit_test.h"


/* ----------
 * Stubs needed to link cluster_debug.o standalone.  cluster_debug.c
 * depends on (read-only): cluster_shmem (ClusterShmem global pointer),
 * cluster_guc (4 GUC vars), cluster_ic (ClusterICOps_Active +
 * ClusterICTier enum), cluster_inject (armed_count + iterator),
 * cluster_pgstat (iterator), cluster_conf (lookup + node_count),
 * cluster_elog (cluster_phase global).  All stubbed below.  The SRF
 * body is never invoked; addresses-only tests + iterator round-trips.
 * ----------
 */
#include "cluster/cluster_conf.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_inject.h"
#include "cluster/cluster_pgstat.h"
#include "cluster/cluster_scn.h"
#include "cluster/cluster_shmem.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/elog.h"


/* cluster_shmem */
ClusterShmemCtl *ClusterShmem = NULL;

/* cluster_guc */
int cluster_node_id = -1;
int cluster_interconnect_tier = 0;
char *cluster_config_file = NULL;
char *cluster_injection_points = NULL;
bool cluster_cf_terminal_authority = false;
int cluster_cf_delayed_cleanout = 1; /* CLUSTER_CF_DELAYED_CLEANOUT_READER */
bool cluster_smart_fusion = false;
int cluster_smart_fusion_tier_min = 3;
int cluster_smart_fusion_commit_brake_timeout_ms = 5000;
int cluster_smart_fusion_origin_durable_gossip_ms = 50;

/* spec-5.59 D1 stubs: cluster_debug.o now carries GUC-gated profiling probes
 * (cluster_xnode_profile.h); the unit harness links neither cluster_guc.o
 * nor cluster_xnode_profile.o, so define the two gate symbols inertly
 * (probes early-return on enabled=false / Ctl=NULL).  dump_xnode_profile
 * also calls cluster_xp_bucket_name; the SRF body that calls it is never
 * invoked by the unit test (same as the file's other SRF stubs). */
bool cluster_xnode_profile_enabled = false;
ClusterXnodeProfileShared *ClusterXnodeProfileCtl = NULL;

const char *
cluster_xp_bucket_name(ClusterXnodeBucket b pg_attribute_unused())
{
	return "stub";
}

/* spec-7.4 D4 stubs: dump_xnode_profile now also builds the commit-latency
 * histogram key labels (component name + μs edge schema); the SRF body that
 * calls them is never invoked by the unit test. */
const char *
cluster_xp_hist_component_name(ClusterXpHistComponent c pg_attribute_unused())
{
	return "stub";
}
const uint32 cluster_xp_hist_edge_us[CLXP_HIST_NEDGES] = { CLXP_HIST_EDGES_US };

/* spec-6.12 stub: dump_xnode_lever reads this pointer (NULL -> all-zero
 * rows); the unit harness does not link cluster_xnode_lever.o. */
ClusterXnodeLeverShared *ClusterXnodeLeverCtl = NULL;

/* spec-6.12d stub: dump_hw reads this pointer (NULL -> lease rows
 * skipped); the unit harness does not link cluster_hw_lease.o. */
ClusterHwLeaseShared *ClusterHwLeaseCtl = NULL;

/* spec-6.15 D6 stub: dump_xid_stripe snapshots the stripe boot face;
 * the unit harness does not link cluster_xid_stripe_boot.o. */
#include "cluster/cluster_xid_stripe_boot.h"
void
cluster_xid_stripe_observe(ClusterXidStripeObs *obs)
{
	memset(obs, 0, sizeof(*obs));
}

/* cluster_ic */
const ClusterICOps *ClusterICOps_Active = NULL;

/* cluster_ic_tier1 — Hardening v1.0.1 F3 listener metadata accessors
 * (cluster_debug.c references these via dump_ic).  Stubs return zeros
 * since this unit test never binds a real listener. */
#include "cluster/cluster_ic_tier1.h"
const ClusterICOps ClusterICOps_Tier1 = { 0 };
pid_t
cluster_ic_tier1_get_listener_pid(void)
{
	return 0;
}
uint64
cluster_ic_tier1_get_listener_incarnation(void)
{
	return 0;
}
int
cluster_ic_tier1_get_listener_port(void)
{
	return -1;
}

/* cluster_inject (armed_count + iterator) */
int cluster_injection_armed_count = 0;

/*
 * Stage 0.30 sweep: cluster_dump_state gained CLUSTER_INJECTION_POINT;
 * cluster_inject.h declares cluster_injection_run extern.
 */
void
cluster_injection_run(const char *name pg_attribute_unused())
{}

int
cluster_injection_get_count(void)
{
	return 0;
}

#include "cluster/cluster_cr_pool.h"			 /* spec-5.51 counter prototypes */
#include "cluster/cluster_cr_admit.h"			 /* spec-5.52 D9 counter prototype */
#include "cluster/cluster_cr_tuple.h"			 /* spec-5.54 D5 counter prototype */
#include "cluster/cluster_cr_coordinator_stat.h" /* spec-5.57 D3 counter prototype */

/*
 * spec-5.52 D9 stub (standalone cluster_debug unit test).  cluster_debug.o reads
 * the admission reason counter for its pg_cluster_state "cr_pool" rows but
 * cluster_cr_admit_stat.o is not linked here.  Link-only no-op stub.  (The
 * spec-5.51 cr_pool counter stubs are defined below next to the dump_cr_pool
 * stub block.)  NOT a substrate / ClusterCRShared change.
 */
uint64
cluster_cr_admit_stat_count(ClusterCRAdmitReason reason pg_attribute_unused())
{
	return 0;
}

/* spec-5.54 D5: tuple-level CR fast-path outcome counter accessor.  Link-only
 * stub (the dump reads it; this test exercises only that the row is emitted). */
uint64
cluster_cr_tuple_stat_count(ClusterCRTupleOutcome outcome pg_attribute_unused())
{
	return 0;
}

/* spec-5.57 D3: cross-instance CR coordinator boundary counter accessor + key.
 * Link-only stubs (the dump reads them; this test only checks the 'cr_coord'
 * rows are emitted).  cluster_cr_coordinator_stat.o is not linked here. */
uint64
cluster_cr_coordinator_stat_count(ClusterCrCoordCounter counter pg_attribute_unused())
{
	return 0;
}

const char *
cluster_cr_coordinator_counter_key(ClusterCrCoordCounter counter)
{
	switch (counter) {
	case CR_COORD_CROSS_INSTANCE_CR_REFUSED:
		return "cross_instance_cr_refused";
	case CR_COORD_REMOTE_UNDO_READ_REFUSED:
		return "remote_undo_read_refused";
	case CR_COORD_MATERIALIZED_REMOTE_SERVED:
		return "materialized_remote_served";
	case CR_COORD_CROSS_INSTANCE_BOUNDARY_PROBE:
		return "cross_instance_boundary_probe";
	case CR_COORD_COUNTER__COUNT:
		break;
	}
	return "unknown";
}

bool
cluster_injection_get_state_at(int idx pg_attribute_unused(),
							   const char **name_out pg_attribute_unused(),
							   ClusterInjectFaultType *type_out pg_attribute_unused(),
							   uint64 *hits_out pg_attribute_unused())
{
	return false;
}

/* cluster_pgstat iterator */
int
cluster_pgstat_get_count(void)
{
	return 0;
}

/* spec-5.6 Dc4: dump_cf reads the CF counters; cluster_cf_stats.o is not
 * linked into this dump test, so a trivial stub satisfies the link. */
uint64
cluster_cf_counter_read(int which)
{
	(void)which;
	return 0;
}

bool
cluster_pgstat_get_at(int idx pg_attribute_unused(), const char **name_out pg_attribute_unused(),
					  uint64 *value_out pg_attribute_unused())
{
	return false;
}

/* cluster_conf */
int
cluster_conf_node_count(void)
{
	return 0;
}

const ClusterNodeInfo *
cluster_conf_lookup_node(int32 node_id pg_attribute_unused())
{
	return NULL;
}

/* cluster_elog */
const char *cluster_phase = "init";

/* cluster_shared_fs (stage 1.1).  cluster_debug.c::dump_shared_fs reads
 * the active vtable and the registered_backends slots; both stubs
 * return NULL so the SRF body's "(none)" / "(empty)" branches fire. */
const struct ClusterSharedFsOps *
cluster_shared_fs_get_active_ops(void)
{
	return NULL;
}

const struct ClusterSharedFsOps *
cluster_shared_fs_get_backend_at(int id pg_attribute_unused())
{
	return NULL;
}

/* Stage 1.2: cluster_smgr accessor + GUC referenced by dump_guc /
 * dump_shared_fs.  cluster_smgr.o is not linked here; provide stubs. */
extern int cluster_smgr_active_relation_count(void);
int
cluster_smgr_active_relation_count(void)
{
	return 0;
}
/* spec-5.2 D1 stub: relsize SMGR-inval broadcast-sent counter. */
uint64
cluster_smgr_get_inval_bcast_sent_count(void)
{
	return 0;
}
bool cluster_smgr_user_relations = false;

/* spec-6.14 D10 stubs: dump_catalog (cluster_debug.o) reads the shared-catalog
 * GUC + the OID-lease and remote-xact side-effect counters; cluster_guc.o,
 * cluster_oid_lease_shmem.o and cluster_remote_xact.o are not linked here. */
bool cluster_shared_catalog = false;
uint64
cluster_oid_lease_acquire_count(void)
{
	return 0;
}
Oid
cluster_oid_lease_remaining(void)
{
	return 0;
}
uint64
cluster_remote_xact_side_effect_record_count(void)
{
	return 0;
}
uint64
cluster_remote_xact_side_effect_drop_count(void)
{
	return 0;
}
/* spec-7.1 D3-a stub: dump_state (cluster_debug.o) reads the striped-multixact
 * origin-derivation observability counters; cluster_multixact.o is not linked
 * here.  Stub each to return 0 (matches the module-disabled path). */
uint64
cluster_multixact_get_mxid_halfspace_refuse_count(void)
{
	return 0;
}
uint64
cluster_multixact_get_mxid_underivable_read_count(void)
{
	return 0;
}
/* spec-6.14 D5 stub: dump_catalog reads the shared relmap authority header;
 * cluster_relmap_authority.o is not linked here.  cluster_shared_catalog is
 * false above, so the read is short-circuited and never called. */
bool
cluster_relmap_authority_read_header(bool shared_map, Oid dbid, ClusterRelmapAuthorityHeader *out)
{
	return false;
}

/* spec-6.15b D7 stubs: dump_catalog reads the XID authority observation
 * keys; this standalone debug unit does not link the file-I/O authority
 * objects. */
bool
cluster_xid_authority_read(ClusterXidAuthorityHeader *out)
{
	return false;
}

bool
cluster_xid_prehistory_was_adopted(void)
{
	return false;
}

/* spec-4.12 D7 + spec-4.12b D6 stubs: dump_write_fence (cluster_debug.o) reads 8
 * counters now, and cluster_startup_phase.o (linked here) references the rejoin
 * self-fence gate.  cluster_write_fence.o is not linked -- provide stubs returning
 * 0 / false. */
uint64 cluster_write_fence_get_hot_gate_blocked(void);
uint64 cluster_write_fence_get_durable_check_blocked(void);
uint64 cluster_write_fence_get_minority_marker_ignored(void);
uint64 cluster_write_fence_get_marker_write_failed(void);
uint64 cluster_write_fence_get_baseline_stale_rejected(void);
uint64 cluster_write_fence_get_baseline_published(void);
bool cluster_write_fence_get_baseline_author_is_self(void);
uint64 cluster_write_fence_get_baseline_authority_age_us(void);
bool cluster_write_fence_startup_self_check(void);
/* spec-5.51 dump_cr_pool stubs: cluster_debug.c's dump_cr_pool reads the CR pool
 * counters, but this standalone binary does not link cluster_cr_pool.o. */
uint64 cluster_cr_pool_current_epoch(void);
uint64 cluster_cr_pool_hit_count(void);
uint64 cluster_cr_pool_miss_count(void);
uint64 cluster_cr_pool_reserve_count(void);
uint64 cluster_cr_pool_publish_count(void);
uint64 cluster_cr_pool_abort_count(void);
uint64 cluster_cr_pool_evict_count(void);
uint64 cluster_cr_pool_epoch_bump_count(void);
uint64 cluster_cr_pool_publish_stale_release_count(void);
uint64 cluster_cr_pool_key_mismatch_count(void);
uint64 cluster_cr_pool_epoch_mismatch_count(void);
uint64 cluster_cr_pool_generation_mismatch_count(void);
uint64 cluster_cr_pool_base_lsn_mismatch_count(void);
uint64 cluster_cr_pool_locator_reuse_reject_count(void);
uint64 cluster_cr_pool_global_epoch_fallback_bump_count(void);
uint64 cluster_cr_pool_rel_gen_bump_count(void);
uint64 cluster_cr_pool_rel_gen_table_overflow_count(void);
uint64 cluster_cr_pool_retention_horizon_advance_noted_count(void);
uint64 cluster_cr_pool_reconfig_intra_survived_count(void);
int cluster_cr_pool_live_entries(void);
uint64
cluster_cr_pool_current_epoch(void)
{
	return 0;
}
uint64
cluster_cr_pool_hit_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_miss_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_reserve_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_publish_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_abort_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_evict_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_epoch_bump_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_publish_stale_release_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_key_mismatch_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_epoch_mismatch_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_generation_mismatch_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_base_lsn_mismatch_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_locator_reuse_reject_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_global_epoch_fallback_bump_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_rel_gen_bump_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_rel_gen_table_overflow_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_retention_horizon_advance_noted_count(void)
{
	return 0;
}
uint64
cluster_cr_pool_reconfig_intra_survived_count(void)
{
	return 0;
}
int
cluster_cr_pool_live_entries(void)
{
	return 0;
}

/* spec-5.55 dump resolver_cache stubs: cluster_debug.c's dump reads the shared
 * resolver cache counters, but this standalone binary does not link
 * cluster_resolver_cache.o. */
uint64 cluster_resolver_cache_lookup_count(void);
uint64 cluster_resolver_cache_key_present_count(void);
uint64 cluster_resolver_cache_epoch_miss_count(void);
uint64 cluster_resolver_cache_hit_count(void);
uint64 cluster_resolver_cache_revalidate_miss_count(void);
uint64 cluster_resolver_cache_acceptance_pass_count(void);
uint64 cluster_resolver_cache_acceptance_failclosed_count(void);
uint64 cluster_resolver_cache_install_count(void);
uint64 cluster_resolver_cache_evict_count(void);
uint64 cluster_resolver_cache_nonown_skip_count(void);
uint64 cluster_resolver_cache_nonterminal_skip_count(void);
int cluster_resolver_cache_live_entries(void);
uint64
cluster_resolver_cache_lookup_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_key_present_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_epoch_miss_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_hit_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_revalidate_miss_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_acceptance_pass_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_acceptance_failclosed_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_install_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_evict_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_nonown_skip_count(void)
{
	return 0;
}
uint64
cluster_resolver_cache_nonterminal_skip_count(void)
{
	return 0;
}
int
cluster_resolver_cache_live_entries(void)
{
	return 0;
}

/* spec-5.7 dump_hw stubs: cluster_debug.c's dump_hw reads the HW counters, but
 * this standalone binary does not link cluster_hw_shmem.o. */
uint64 cluster_hw_alloc_count(void);
uint64 cluster_hw_authority_create_count(void);
uint64 cluster_hw_reserve_wal_count(void);
uint64 cluster_hw_rebuild_count(void);
uint64 cluster_hw_failclosed_count(void);
uint64 cluster_hw_not_ready_count(void);
uint64 cluster_hw_remaster_done_count(void);
uint64 cluster_hw_remaster_blocked_count(void);
uint64 cluster_hw_remaster_retry_count(void);
uint64 cluster_hw_remaster_retry_exhausted_count(void);
bool cluster_hw_remaster_recoverable(void);
uint64
cluster_hw_alloc_count(void)
{
	return 0;
}
uint64
cluster_hw_authority_create_count(void)
{
	return 0;
}
uint64
cluster_hw_reserve_wal_count(void)
{
	return 0;
}
uint64
cluster_hw_rebuild_count(void)
{
	return 0;
}
uint64
cluster_hw_failclosed_count(void)
{
	return 0;
}
uint64
cluster_hw_not_ready_count(void)
{
	return 0;
}
uint64
cluster_hw_remaster_done_count(void)
{
	return 0;
}
uint64
cluster_hw_remaster_blocked_count(void)
{
	return 0;
}
uint64
cluster_hw_remaster_retry_count(void)
{
	return 0;
}
uint64
cluster_hw_remaster_retry_exhausted_count(void)
{
	return 0;
}
bool
cluster_hw_remaster_recoverable(void)
{
	return true;
}

/* spec-5.7 D4 dump_dl stubs (cluster_dl.c not linked in this binary). */
uint64 cluster_dl_lease_count(void);
uint64 cluster_dl_native_count(void);
uint64 cluster_dl_failclosed_count(void);
uint64 cluster_dl_release_count(void);
uint64
cluster_dl_lease_count(void)
{
	return 0;
}
uint64
cluster_dl_native_count(void)
{
	return 0;
}
uint64
cluster_dl_failclosed_count(void)
{
	return 0;
}
uint64
cluster_dl_release_count(void)
{
	return 0;
}

/* spec-5.7 D8 dump_ir stubs (cluster_ir_lock.c not linked in this binary). */
uint64 cluster_ir_owner_count(void);
uint64 cluster_ir_native_count(void);
uint64 cluster_ir_conflict_count(void);
uint64 cluster_ir_release_count(void);
uint64
cluster_ir_owner_count(void)
{
	return 0;
}
uint64
cluster_ir_native_count(void)
{
	return 0;
}
uint64
cluster_ir_conflict_count(void)
{
	return 0;
}
uint64
cluster_ir_release_count(void)
{
	return 0;
}

/* spec-5.7 D5 dump_ts stubs (cluster_ts_lock.c not linked in this binary). */
uint64 cluster_ts_x_count(void);
uint64 cluster_ts_s_count(void);
uint64 cluster_ts_native_count(void);
uint64 cluster_ts_failclosed_count(void);
uint64
cluster_ts_x_count(void)
{
	return 0;
}
uint64
cluster_ts_s_count(void)
{
	return 0;
}
uint64
cluster_ts_native_count(void)
{
	return 0;
}
uint64
cluster_ts_failclosed_count(void)
{
	return 0;
}

/* spec-5.7 D6 dump_ko stubs (cluster_ko_lock.c not linked in this binary). */
uint64 cluster_ko_flush_count(void);
uint64 cluster_ko_ack_received_count(void);
uint64 cluster_ko_failclosed_count(void);
uint64 cluster_ko_native_count(void);
uint64 cluster_ko_lockfail_count(void);
uint64 cluster_ko_peer_apply_count(void);
uint64 cluster_ko_inbound_full_count(void);
uint64
cluster_ko_flush_count(void)
{
	return 0;
}
uint64
cluster_ko_lockfail_count(void)
{
	return 0;
}
uint64
cluster_ko_ack_received_count(void)
{
	return 0;
}
uint64
cluster_ko_failclosed_count(void)
{
	return 0;
}
uint64
cluster_ko_native_count(void)
{
	return 0;
}
uint64
cluster_ko_peer_apply_count(void)
{
	return 0;
}
uint64
cluster_ko_inbound_full_count(void)
{
	return 0;
}

uint64
cluster_write_fence_get_hot_gate_blocked(void)
{
	return 0;
}
uint64
cluster_write_fence_get_durable_check_blocked(void)
{
	return 0;
}
uint64
cluster_write_fence_get_minority_marker_ignored(void)
{
	return 0;
}
uint64
cluster_write_fence_get_marker_write_failed(void)
{
	return 0;
}
uint64
cluster_write_fence_get_baseline_stale_rejected(void)
{
	return 0;
}
uint64
cluster_write_fence_get_baseline_published(void)
{
	return 0;
}
bool
cluster_write_fence_get_baseline_author_is_self(void)
{
	return false;
}
uint64
cluster_write_fence_get_baseline_authority_age_us(void)
{
	return 0;
}
bool
cluster_write_fence_startup_self_check(void)
{
	return false;
}

/* Stage 1.3: cluster_debug.c::dump_shmem now reads from the cluster
 * shmem region registry (region_count + total_bytes + per-region
 * iter).  cluster_shmem.o is not linked here; provide stubs that
 * mimic an empty registry. */
int cluster_shmem_max_regions = 80; /* spec-5.56: default raised 64 -> 80 */
int
cluster_shmem_get_region_count(void)
{
	return 0;
}

/*
 * Stage 1.7: cluster_debug.c::dump_pcm calls cluster_pcm_grd_count() +
 * cluster_pcm_grd_shmem_size() + reads cluster_pcm_grd_max_entries
 * (defined in cluster_pcm_lock.c).  cluster_pcm_lock.o is not linked
 * here; provide stubs returning the same defaults the real
 * implementation returns when GUC=0.
 *
 * spec-2.30 D9 (R10 stub audit):  dump_pcm calls 9 NEW transition
 * counter accessors;  stub each to return 0 (matches GUC=0 disable path).
 */
int cluster_pcm_grd_max_entries = 0;

int
cluster_pcm_grd_count(void)
{
	return 0;
}

Size
cluster_pcm_grd_shmem_size(void)
{
	return 0;
}

void
cluster_pcm_grd_get_summary(int *n_count, int *s_count, int *x_count, int *pi_holders_total,
							int *convert_queue_active)
{
	*n_count = 0;
	*s_count = 0;
	*x_count = 0;
	*pi_holders_total = 0;
	*convert_queue_active = 0;
}

/* PGRAC spec-2.30 D9 R10 stub audit — 9 transition counter accessors. */
uint64
cluster_pcm_get_trans_n_to_s_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_n_to_x_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_x_upgrade_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_x_to_s_downgrade_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_x_to_n_downgrade_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_x_to_n_release_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_n_invalidate_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_n_release_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_trans_s_to_x_cleanout_count(void)
{
	return 0;
}

/* PGRAC spec-2.32 D8 stub audit — 12 GCS accessor stubs + api_state. */
uint64
cluster_gcs_get_lookup_master_self_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_lookup_master_remote_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_send_request_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_handle_request_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_handle_reply_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_reply_late_drop_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_reply_timeout_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_encode_payload_bytes(void)
{
	return 0;
}
uint64
cluster_gcs_get_decode_payload_bytes(void)
{
	return 0;
}
uint64
cluster_gcs_get_dispatch_loop_iterations(void)
{
	return 0;
}
uint64
cluster_gcs_get_outstanding_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_max_outstanding(void)
{
	return 0;
}
const char *
cluster_gcs_get_api_state(void)
{
	return "stub";
}

/* spec-2.33 D10 stubs: 8 NEW block-plane counter accessors referenced by
 * cluster_dump_state (cluster_debug.c dump_gcs).  Return 0 for all. */
uint64
cluster_gcs_get_block_request_count(void)
{
	return 0;
}
/* spec-7.2 D6 stubs: ship-latency histogram accessors. */
uint64
cluster_gcs_block_ship_hist_bound_us(int bucket)
{
	return (bucket == 15) ? UINT64_MAX : (uint64)(bucket + 1) * 1000;
}
uint64
cluster_gcs_block_ship_hist_count(int bucket)
{
	(void)bucket;
	return 0;
}
/* spec-7.2 flip stubs: plane facts. */
bool
cluster_gcs_block_family_on_data_plane(void)
{
	return false;
}
uint64
cluster_ic_tier1_get_plane_misroute_reject(ClusterICPlane plane)
{
	(void)plane;
	return 0;
}
uint64
cluster_gcs_get_block_reply_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_timeout_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_checksum_fail_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_storage_fallback_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_master_not_holder_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_wal_flush_before_ship_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_ship_bytes_total(void)
{
	return 0;
}
uint64
cluster_gcs_get_scratch_copy_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_live_sge_send_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_live_sge_fallback_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_direct_install_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_direct_install_abort_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_install_copy_count(void)
{
	return 0;
}

/* spec-2.34 D10 stubs: 9 NEW reliability hardening counter accessors. */
uint64
cluster_gcs_get_block_retransmit_attempt_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_retransmit_send_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_retransmit_exhausted_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_hit_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_miss_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_collision_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_full_count(void)
{
	return 0;
}

/* spec-7.2a D5 stubs: 3 NEW dedup capacity/occupancy accessors. */
uint64
cluster_gcs_get_block_dedup_entry_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_evict_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_dedup_max_entries(void)
{
	return 0;
}
uint64
cluster_gcs_block_dedup_get_misroute_failclosed_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_epoch_invalidate_wake_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_stale_reply_drop_count(void)
{
	return 0;
}

/* spec-2.35 D12 stubs: 7 NEW CF 2-way protocol counter accessors. */
uint64
cluster_gcs_get_block_forward_sent_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_forward_received_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_from_holder_ship_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_forward_holder_evicted_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_s_holders_bitmap_redirect_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_master_holder_lifecycle_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_forward_replay_count(void)
{
	return 0;
}

/* spec-2.36 D10 stubs: 6 NEW CF 3-way protocol counter accessors. */
uint64
cluster_gcs_get_block_invalidate_broadcast_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_invalidate_ack_received_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_invalidate_timeout_count(void)
{
	return 0;
}

/* PGRAC: spec-6.14a D5 stubs — dump_gcs/dump_pcm read the X-vs-S arm
 * counters; the real modules are not linked here. */
uint64
cluster_gcs_get_local_s_upgrade_grant_count(void)
{
	return 0;
}

uint64
cluster_gcs_get_x_vs_s_nonholder_grant_count(void)
{
	return 0;
}

uint64
cluster_gcs_get_x_vs_s_no_carrier_denied_count(void)
{
	return 0;
}

uint64
cluster_pcm_get_local_s_revoke_nonholder_failclosed_count(void)
{
	return 0;
}
uint64
cluster_pcm_get_evict_release_deferred_aux_count(void)
{
	return 0;
}
/* spec-6.14 D10b catalog counters (cluster_catalog_stats.o not linked) */
uint64
cluster_catalog_stats_vis_resolve_count(void)
{
	return 0;
}
uint64
cluster_catalog_stats_vis_unknown_count(void)
{
	return 0;
}
uint64
cluster_catalog_stats_buf_hit_count(void)
{
	return 0;
}
uint64
cluster_catalog_stats_buf_miss_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_x_forward_sent_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_x_granted_from_holder_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_starvation_denied_pending_x_count(void)
{
	return 0;
}

/* spec-2.37 D12 stubs: 4 NEW PI watermark + lost-write counter accessors. */
uint64
cluster_gcs_get_pi_watermark_advance_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_pi_watermark_retire_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_lost_write_detected_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_lost_write_avoid_count(void)
{
	return 0;
}
/* spec-2.41 D7 stubs: SCN detector + redo-coverage observability accessors. */
uint64
cluster_gcs_get_lost_write_invalidscn_failclosed_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_lost_write_not_scn_tracked_skip_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_redo_coverage_required_lsn_zero_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_redo_coverage_gate_block_count(void)
{
	return 0;
}
/* spec-5.2 D2 stub: X-holder read-image ship counter accessor. */
uint64
cluster_gcs_get_cf_xheld_read_ship_count(void)
{
	return 0;
}
/* spec-5.2 D11 stubs: writer-transfer-revoke ship counter accessors (path A/B). */
uint64
cluster_gcs_get_block_x_transfer_ship_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_block_x_self_ship_count(void)
{
	return 0;
}
/* spec-5.2a D6 stubs: 5 NEW clean-page X-transfer enabler counter accessors. */
uint64
cluster_gcs_get_clean_page_xfer_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_clean_page_xfer_storage_fallback_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_clean_page_xfer_fail_closed_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_clean_page_xfer_stale_holder_recover_count(void)
{
	return 0;
}
uint64
cluster_gcs_get_clean_page_xfer_third_party_denied_count(void)
{
	return 0;
}

/* spec-5.5 D8 stub: UL advisory counter accessor (cluster_advisory.o is not
 * linked into this standalone unit). */
uint64
cluster_advisory_counter_read(int which pg_attribute_unused())
{
	return 0;
}

/* spec-5.4 D9 stubs: 6 SQ sequence counter accessors (cluster_sequence_shmem.o
 * is not linked into this standalone unit). */
uint64
cluster_sq_refill_count(void)
{
	return 0;
}
uint64
cluster_sq_refill_wait_count(void)
{
	return 0;
}
uint64
cluster_sq_dup_guard_fail_count(void)
{
	return 0;
}
uint64
cluster_sq_failover_fail_closed_count(void)
{
	return 0;
}
uint64
cluster_sq_page_writeback_count(void)
{
	return 0;
}
uint64
cluster_sq_cycle_rejected_count(void)
{
	return 0;
}

/* spec-4.7 D6 stubs: 8 NEW GCS/PCM warm-recovery counter accessors. */
uint64
cluster_gcs_get_recovery_block_resources_recovering(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_buffers_redeclared(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_block_state_rebuilt(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_redo_boundary_waits(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_redo_boundary_reached(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_stale_block_drop(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_ambiguous_owner_failclosed(void)
{
	return 0;
}
uint64
cluster_gcs_get_recovery_before_boundary_failclosed(void)
{
	return 0;
}

/* spec-2.38 D10 stubs: 9 NEW SI Broadcaster counter accessors. */
uint64
cluster_sinval_get_broadcast_send_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_broadcast_receive_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_inject_local_queue_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_outbound_queue_full_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_inbound_queue_full_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_inbound_overflow_reset_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_validation_drop_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_stale_epoch_drop_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_echo_dropped_count(void)
{
	return 0;
}
/* spec-2.39 D8/D9:  6 NEW counter stubs. */
uint64
cluster_sinval_get_fanout_would_block_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_hard_error_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_fanout_peer_down_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_received_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_timeout_count(void)
{
	return 0;
}
uint64
cluster_sinval_get_ack_orphan_count(void)
{
	return 0;
}
/* spec-5.2 D1 stub: relsize SMGR-inval apply barrier counter. */
uint64
cluster_sinval_get_smgr_inval_applied_count(void)
{
	return 0;
}

/* spec-3.1 D9:  7 NEW TT status counter stubs. */
uint64
cluster_tt_status_get_install_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_lookup_hit_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_lookup_miss_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_evict_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_flush_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_self_consumer_hit_count(void)
{
	return 0;
}
uint64
cluster_tt_status_get_evict_fail_count(void)
{
	return 0;
}

/* spec-3.2 D8:  6 NEW TT status hint counter stubs. */
uint64
cluster_tt_status_hint_get_emit_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_receive_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_invalid_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_stale_epoch_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_unknown_version_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_install_count(void)
{
	return 0;
}
uint64
cluster_tt_status_hint_get_drop_v1_compat_count(void)
{
	return 0;
}

/* spec-3.7 D10 stubs: cluster_debug dump_undo() references 5 counter
 * accessors from cluster_undo_record.o which test_cluster_debug doesn't link. */
uint64
cluster_undo_record_alloc_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_claim_count(void)
{
	return 0;
}
/* spec-3.18 D7: dump_undo now reads the extent + undo-buffer counters. */
uint64
cluster_undo_extent_claim_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_hit_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_miss_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_writeback_count(void)
{
	return 0;
}
/* spec-4.8ab D7: checkpoint-writeback boundary counter stubs. */
uint64
cluster_undo_buf_get_writeback_held_wal_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_writeback_held_evidence_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_boundary_violation_count(void)
{
	return 0;
}
uint64
cluster_undo_buf_get_remote_evidence_hold_count(void)
{
	return 0;
}
uint64
cluster_undo_block_write_count(void)
{
	return 0;
}
uint64
cluster_undo_block_flush_count(void)
{
	return 0;
}
uint64
cluster_undo_reader_lookup_count(void)
{
	return 0;
}
/* spec-5.22b D2-6 stubs: 6 undo GCS grant-plane counters referenced by dump_undo. */
uint64
cluster_undo_gcs_grant_shared_count(void)
{
	return 0;
}
uint64
cluster_undo_gcs_grant_exclusive_count(void)
{
	return 0;
}
uint64
cluster_undo_gcs_ship_bytes(void)
{
	return 0;
}
uint64
cluster_undo_gcs_invalidate_notify_count(void)
{
	return 0;
}
uint64
cluster_undo_gcs_remaster_deny_count(void)
{
	return 0;
}
uint64
cluster_undo_gcs_local_fast_path_count(void)
{
	return 0;
}
/* spec-3.8 D11 stubs: 4 NEW lifecycle counters referenced by dump_undo. */
uint64
cluster_undo_autoextend_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_switch_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_create_fail_count(void)
{
	return 0;
}
uint64
cluster_undo_segment_hard_cap_fail_count(void)
{
	return 0;
}
/* P0 perf hardening: per-commit fsync + smgr syscall counter accessors
 * (dump_undo emits these; cluster_undo_record.o not linked here). */
uint64
cluster_undo_commit_fsync_count(void)
{
	return 0;
}
uint64
cluster_undo_commit_fsync_segment_count(void)
{
	return 0;
}
uint64
cluster_undo_commit_fsync_failure_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_open_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_close_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_pread_count(void)
{
	return 0;
}
uint64
cluster_undo_smgr_pwrite_count(void)
{
	return 0;
}

/* spec-3.9 D8 stubs: dump_cr() references 9 cluster_cr counter accessors
 * from cluster_cr.o which test_cluster_debug doesn't link. */
uint64
cluster_cr_construct_count(void)
{
	return 0;
}
uint64
cluster_cr_snapshot_too_old_count(void)
{
	return 0;
}
uint64
cluster_cr_cross_instance_unsupported_count(void)
{
	return 0;
}
uint64
cluster_cr_corruption_count(void)
{
	return 0;
}
uint64
cluster_cr_chain_walk_steps_sum(void)
{
	return 0;
}
uint64
cluster_cr_inverse_insert_count(void)
{
	return 0;
}
uint64
cluster_cr_inverse_update_count(void)
{
	return 0;
}
uint64
cluster_cr_inverse_delete_count(void)
{
	return 0;
}
uint64
cluster_cr_inverse_itl_count(void)
{
	return 0;
}
/* spec-3.10 D5 cache counters: cluster_debug.o emits these rows; cluster_cr.o
 * is not linked here so they are stubbed too. */
uint64
cluster_cr_cache_hit_count(void)
{
	return 0;
}
uint64
cluster_cr_cache_miss_count(void)
{
	return 0;
}
uint64
cluster_cr_cache_evict_count(void)
{
	return 0;
}
uint64
cluster_cr_cache_install_count(void)
{
	return 0;
}
/* spec-6.12b: CR-server data plane counters. */
uint64
cluster_cr_remote_full_count(void)
{
	return 0;
}
uint64
cluster_cr_remote_partial_count(void)
{
	return 0;
}
uint64
cluster_cr_remote_failed_count(void)
{
	return 0;
}
uint64
cluster_cr_server_full_count(void)
{
	return 0;
}
uint64
cluster_cr_server_partial_count(void)
{
	return 0;
}
uint64
cluster_cr_server_denied_count(void)
{
	return 0;
}
/* spec-6.12i: undo-TT fetch data plane counters. */
uint64
cluster_rtvis_undo_fetch_wire_count(void)
{
	return 0;
}
uint64
cluster_rtvis_undo_fetch_cache_hit_count(void)
{
	return 0;
}
uint64
cluster_rtvis_undo_fetch_failclosed_count(void)
{
	return 0;
}
uint64
cluster_cr_server_undo_served_count(void)
{
	return 0;
}
uint64
cluster_cr_server_undo_denied_count(void)
{
	return 0;
}
/* spec-6.12i CP3: resolution-verdict counters. */
uint64
cluster_rtvis_resolve_committed_count(void)
{
	return 0;
}
uint64
cluster_rtvis_resolve_aborted_count(void)
{
	return 0;
}
uint64
cluster_rtvis_resolve_failclosed_count(void)
{
	return 0;
}
/* spec-6.12i CP5 (D-i4) / spec-6.15 D4: origin-verdict leg counters. */
uint64
cluster_rtvis_verdict_wire_count(void)
{
	return 0;
}
uint64
cluster_rtvis_verdict_failclosed_count(void)
{
	return 0;
}
uint64
cluster_rtvis_verdict_exact_count(void)
{
	return 0;
}
uint64
cluster_rtvis_verdict_below_horizon_count(void)
{
	return 0;
}
uint64
cluster_rtvis_verdict_inadmissible_count(void)
{
	return 0;
}
uint64
cluster_cr_server_verdict_served_count(void)
{
	return 0;
}
uint64
cluster_cr_server_verdict_denied_count(void)
{
	return 0;
}
uint64
cluster_cr_server_multi_verdict_served_count(void)
{
	return 0;
}
uint64
cluster_cr_server_multi_verdict_denied_count(void)
{
	return 0;
}
uint64
cluster_cr_server_fence_refused_count(void)
{
	return 0;
}
uint64
cluster_rtvis_underivable_failclosed_count(void)
{
	return 0;
}
/* GCS-race round-2 RC-E: native-prehistory coverage latch + LOCAL counter. */
uint64
cluster_cr_native_prehistory_covered_hw(void)
{
	return 0;
}
uint64
cluster_rtvis_native_prehistory_local_count(void)
{
	return 0;
}
/* spec-5.22f D6-3: fresh-remote-ITL-ref widening outcome counters. */
uint64
cluster_vis_freshref_verdict_resolved_count(void)
{
	return 0;
}
uint64
cluster_vis_freshref_verdict_failclosed_count(void)
{
	return 0;
}
/* spec-5.22d D4-4/D4-5: dead-owner authority block0 serve counters. */
uint64
cluster_undo_authority_serve_hit_count(void)
{
	return 0;
}
uint64
cluster_undo_authority_fail_closed_count(void)
{
	return 0;
}
uint64
cluster_undo_authority_epoch_stale_reject_count(void)
{
	return 0;
}
/* spec-5.22d A1 (D4-8): complete-scan refusal attribution. */
uint64
cluster_undo_authority_scan_incomplete_reject_count(void)
{
	return 0;
}
uint64
cluster_undo_authority_multi_match_reject_count(void)
{
	return 0;
}
/* spec-7.1 D0/D5: 53R97 per-leg attribution counters. */
uint64
cluster_vis53r97_leg_invalid_scn_refuse_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_zero_match_refuse_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_srv_other_refuse_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_covers_refuse_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_multi_unresolvable_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_xmax_unprovable_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_xmin_overlay_verdict_ask_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_xmin_overlay_verdict_hit_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_multi_member_serve_ask_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_multi_member_serve_hit_count(void)
{
	return 0;
}
uint64
cluster_vis53r97_leg_live_upgrade_hit_count(void)
{
	return 0;
}
/* spec-3.22 D3: xmax recycled-slot resolve outcome buckets. */
uint64
cluster_cr_xmax_resolved_count(void)
{
	return 0;
}
uint64
cluster_cr_xmax_recycled_invisible_count(void)
{
	return 0;
}
uint64
cluster_cr_xmax_invalid_or_ambiguous_count(void)
{
	return 0;
}
uint64
cluster_cr_xmax_scan_unavail_or_no_proof_count(void)
{
	return 0;
}

/* spec-4.1 D7 + spec-4.2 D5: per-thread WAL routing / WAL-state registry
 * accessors (cluster_wal_thread.c / cluster_wal_state.c) are not linked
 * here; stub everything dump_wal_thread reads (L104). */
#include "cluster/cluster_wal_state.h"
bool
cluster_wal_state_registry_ready(void)
{
	return false;
}
ClusterWalSlotVerdict
cluster_wal_state_read_slot(uint16 thread_id pg_attribute_unused(),
							ClusterWalStateSlot *slot_out pg_attribute_unused())
{
	return CLUSTER_WAL_SLOT_EMPTY;
}
void
cluster_wal_state_publish_active(void)
{}
void
cluster_wal_state_refresh_own_slot(void)
{}

/* spec-4.3 D5 stub: recovery plan snapshot (cluster_recovery_plan.c
 * not linked; dump_recovery's plan keys read '-' / none). */
#include "cluster/cluster_recovery_plan.h"
bool
cluster_recovery_plan_snapshot(ClusterRecoveryPlan *out pg_attribute_unused())
{
	return false;
}

/* spec-4.4 D6 stub: worker pool snapshot (cluster_recovery_worker.c
 * not linked; dump_recovery's worker keys read '-'). */
#include "cluster/cluster_recovery_worker.h"
bool
cluster_recovery_worker_pool_snapshot(ClusterRecoveryWorkerPool *out pg_attribute_unused())
{
	return false;
}

uint64
cluster_wal_thread_page_stamp_count(void)
{
	return 0;
}
uint16
cluster_wal_thread_dump_thread_id(void)
{
	return 0;
}
bool
cluster_wal_thread_dir_configured(void)
{
	return false;
}
bool
cluster_wal_thread_dir_validated(void)
{
	return false;
}
bool
cluster_wal_thread_claim_created(void)
{
	return false;
}

/* spec-3.11 D8: durable TT slot counters (cluster_tt_durable_stat.c) are not
 * linked here; stub the 5 accessors dump_undo reads. */
uint64
cluster_tt_durable_commit_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_lookup_hit_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_lookup_miss_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_by_xid_scan_count(void)
{
	return 0;
}
uint64
cluster_tt_durable_redo_apply_count(void)
{
	return 0;
}
/* spec-6.2: terminal authority counters share the durable-TT stat region in
 * the backend; this standalone dump unit stubs the accessors. */
uint64
cluster_terminal_authority_check_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_ok_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_failclosed_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_epoch_failclosed_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_ownership_failclosed_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_unknown_failclosed_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_nonterminal_failclosed_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_durable_failclosed_count(void)
{
	return 0;
}
uint64
cluster_terminal_authority_retention_failclosed_count(void)
{
	return 0;
}
/* spec-6.2 D10: Smart Fusion dependency counters (cluster_sf_dep.c) are not
 * linked here; stub the accessors that dump_smart_fusion reads. */
uint64
cluster_sf_dep_install_count(void)
{
	return 0;
}
uint64
cluster_sf_dep_touch_count(void)
{
	return 0;
}
uint64
cluster_sf_dep_dbwr_brake_count(void)
{
	return 0;
}
uint64
cluster_sf_dep_commit_brake_count(void)
{
	return 0;
}
uint64
cluster_sf_dep_commit_brake_wait_us(void)
{
	return 0;
}
uint64
cluster_sf_dep_origin_suspect_count(void)
{
	return 0;
}
uint64
cluster_sf_dep_lost_failclosed_count(void)
{
	return 0;
}
uint64
cluster_sf_dep_retry_failclosed_count(void)
{
	return 0;
}
/* spec-2.2 additive amendment (spec-5.22e D5 prereq): the ic dump rows read
 * the per-peer capability summary + PEER_CAPS_REPLY reject counter from
 * cluster_sf_dep.c, which is not linked here; stub both. */
const char *
cluster_sf_peer_capabilities_summary(void)
{
	return "";
}
uint64
cluster_sf_caps_reply_reject_count(void)
{
	return 0;
}
/* spec-4.8: tt_recovery counter accessors (cluster_tt_durable_stat.c) are not
 * linked here; stub the 8 accessors the tt_recovery dump rows read. */
uint64
cluster_tt_recovery_active_resolved_aborted_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_remote_active_failclosed_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_wrap_generation_disambiguated_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_recycled_liveness_relaxed_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_scn_highwater_recovered_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_recovery_verdict_failclosed_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_heap_tuples_physically_reverted_count(void)
{
	return 0;
}
uint64
cluster_tt_recovery_undo_revert_failclosed_count(void)
{
	return 0;
}
/* spec-4.10 D6 stubs: block-recovery counter accessors (dump_recovery rows). */
uint64
cluster_block_recovery_get_blocks_recovered(void)
{
	return 0;
}
uint64
cluster_block_recovery_get_failclosed(void)
{
	return 0;
}
/* spec-4.11 D5 stubs: online thread-recovery counter accessors (dump_recovery rows). */
const char *
cluster_thread_recovery_state_name(void)
{
	return "stub";
}
uint64
cluster_thread_recovery_get_threads_recovered(void)
{
	return 0;
}
uint64
cluster_thread_recovery_get_replay_failclosed(void)
{
	return 0;
}
XLogRecPtr
cluster_thread_recovery_get_recovered_through(void)
{
	return 0;
}
/* spec-7.1a D6 write-write chaining counter accessor stubs. */
uint64
cluster_vis_get_writer_chain_resolved_count(void)
{
	return 0;
}
uint64
cluster_vis_get_writer_chain_failclosed_count(void)
{
	return 0;
}
uint64
cluster_vis_get_xmax_resolved_count(void)
{
	return 0;
}
uint64
cluster_vis_get_overlay_refresh_count(void)
{
	return 0;
}
uint64
cluster_vis_get_covers_scn_refuse_count(void)
{
	return 0;
}
/* spec-3.16 D5 recovery counter accessor stubs (dump_recovery rows). */
uint64
cluster_vis_get_recovery_undo_redo_applies(void)
{
	return 0;
}
uint64
cluster_vis_get_recovery_undo_redo_skips(void)
{
	return 0;
}
uint64
cluster_vis_get_recovery_2pc_standby_rebuilds(void)
{
	return 0;
}
uint64
cluster_vis_get_recovery_overlay_rebuild_count(void)
{
	return 0;
}

/* spec-4.5a D11 merged-replay / remote-read accessor stubs (dump_recovery
 * rows; the real counters live in cluster_tt_status.c / cluster_remote_xact.c
 * / cluster_recovery_merge.c, none of which are linked here). */
uint64
cluster_vis_get_merged_records_applied(void)
{
	return 0;
}
uint64
cluster_vis_get_merged_skipped_local(void)
{
	return 0;
}
uint64
cluster_vis_get_merged_own_bound_skips(void)
{
	return 0;
}
uint64
cluster_vis_get_remote_uba_resolved(void)
{
	return 0;
}
uint64
cluster_remote_xact_diverted_commit_count(void)
{
	return 0;
}
uint64
cluster_remote_xact_diverted_abort_count(void)
{
	return 0;
}
uint64
cluster_remote_xact_outcome_indoubt_count(void)
{
	return 0;
}
bool
cluster_merged_instance_is_materialized(int origin_node)
{
	(void)origin_node;
	return false;
}

/* spec-3.15 D9 2PC counter accessor stubs (dump_tt_2pc rows). */
uint64
cluster_vis_get_twopc_prepare_records(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_prepare_undo_flushes(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_postprepare_transfers(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_prefinish_commits(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_prefinish_aborts(void)
{
	return 0;
}
uint64
cluster_vis_get_twopc_recover_rebinds(void)
{
	return 0;
}

/* spec-3.14 D8 visibility counter accessor stubs (dump_visibility rows). */
uint64
cluster_vis_get_vis_update_fork_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_dirty_fork_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_selftoast_fork_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_conflict_failclosed_count(void)
{
	return 0;
}
uint64
cluster_vis_get_prune_remote_keep_count(void)
{
	return 0;
}
uint64
cluster_vis_get_vis_variant_unknown_failclosed_count(void)
{
	return 0;
}
/* spec-3.13 D6 counter accessor stubs (dump_undo new rows). */
uint64
cluster_undo_cleaner_pass_count(void)
{
	return 0;
}
/* spec-5.22e D5-5 stubs: retention brake observability rows. */
uint64
cluster_undo_cleaner_header_tt_slots_below_horizon(void)
{
	return 0;
}
uint64
cluster_undo_horizon_stall_count(void)
{
	return 0;
}
uint64
cluster_undo_horizon_peer_stale_count(void)
{
	return 0;
}
uint64
cluster_undo_horizon_pass_abort_count(void)
{
	return 0;
}
uint64
cluster_undo_horizon_wire_reject_count(void)
{
	return 0;
}
uint64
cluster_undo_horizon_admission_refuse_count(void)
{
	return 0;
}
SCN
cluster_undo_horizon_last_floor(void)
{
	return (SCN)0;
}
const char *
cluster_undo_horizon_peer_reports_summary(void)
{
	return "";
}
uint64
cluster_undo_cleaner_shmem_tt_slots_gcd(void)
{
	return 0;
}
uint64
cluster_undo_cleaner_segments_marked_recyclable(void)
{
	return 0;
}
uint64
cluster_undo_cleaner_stale_active_skipped(void)
{
	return 0;
}
uint64
cluster_undo_segment_reuse_count(void)
{
	return 0;
}
uint64
cluster_tt_slot_wrap_retired_count(void)
{
	return 0;
}

/* spec-4.12a D5 record-segment drain counter stubs (dump_undo new rows;
 * cluster_undo_record.o not linked into this test). */
uint64
cluster_undo_record_segments_committed_count(void)
{
	return 0;
}
uint64
cluster_undo_record_seg_commit_skipped_inflight_count(void)
{
	return 0;
}
uint64
cluster_undo_record_seg_residual_revalidate_drop_count(void)
{
	return 0;
}

/* spec-3.13 D1 Undo Cleaner accessor stubs (dump_undo_cleaner references). */
#include "cluster/cluster_undo_cleaner.h"
UndoCleanerStatus
cluster_undo_cleaner_status(void)
{
	return UNDO_CLEANER_NOT_STARTED;
}
const char *
cluster_undo_cleaner_status_to_string(UndoCleanerStatus st)
{
	(void)st;
	return "not_started";
}
pid_t
cluster_undo_cleaner_pid(void)
{
	return 0;
}
TimestampTz
cluster_undo_cleaner_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_undo_cleaner_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_undo_cleaner_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_undo_cleaner_main_loop_iters(void)
{
	return 0;
}
/* spec-3.12 D5 retention counter stubs (dump_undo references these). */
uint64
cluster_tt_slot_retention_horizon_scn(void)
{
	return 0;
}
uint64
cluster_tt_slot_retain_skip_count(void)
{
	return 0;
}
uint64
cluster_tt_slot_retention_recycle_count(void)
{
	return 0;
}
uint64
cluster_tt_slot_retention_off_recycle_count(void) /* spec-3.22 */
{
	return 0;
}
SCN
cluster_tt_slot_max_recycle_horizon(void) /* spec-6.12i CP5 (D-i4) */
{
	return InvalidScn;
}
uint64
cluster_undo_segment_retain_skip_count(void)
{
	return 0;
}
uint64
cluster_undo_tt_retention_rollover_count(void)
{
	return 0;
}

Size
cluster_shmem_get_total_bytes(void)
{
	return 0;
}
bool
cluster_shmem_iter_regions(int *idx pg_attribute_unused(),
						   ClusterShmemRegion *out pg_attribute_unused())
{
	return false;
}

/* StringInfo + pfree stubs for dump_shared_fs / dump_shmem (stage 1.3).
 * No-op pointers; SRF body is never invoked by this unit test. */
#include "lib/stringinfo.h"
void
initStringInfo(StringInfo str)
{
	str->data = NULL;
	str->len = 0;
	str->maxlen = 0;
	str->cursor = 0;
}
void
appendStringInfoChar(StringInfo str pg_attribute_unused(), char ch pg_attribute_unused())
{}
void
appendStringInfoString(StringInfo str pg_attribute_unused(), const char *s pg_attribute_unused())
{}
void
appendStringInfo(StringInfo str pg_attribute_unused(), const char *fmt pg_attribute_unused(), ...)
{}
void
resetStringInfo(StringInfo str pg_attribute_unused())
{}
void
pfree(void *pointer pg_attribute_unused())
{}


/* PG backend stubs */
void
ExceptionalCondition(const char *conditionName pg_attribute_unused(),
					 const char *fileName pg_attribute_unused(),
					 int lineNumber pg_attribute_unused())
{
	abort();
}

/*
 * Spec-1.10 stubs needed because cluster_debug.o now pulls in
 * cluster_startup_phase.o (D6 dump_phase emits 4 new keys backed by
 * cluster_phase_started_at / cluster_phase_elapsed_seconds /
 * cluster_phase_history_format), which transitively references
 * GetCurrentTimestamp + TimestampDifference + timestamptz_to_str +
 * IsUnderPostmaster.  The unit test never invokes the dump path so
 * these are address-only -- harmless to stub to no-ops.
 */
bool IsUnderPostmaster = false;

TimestampTz
GetCurrentTimestamp(void)
{
	return 0;
}

void
TimestampDifference(TimestampTz start_time pg_attribute_unused(),
					TimestampTz stop_time pg_attribute_unused(), long *secs, int *microsecs)
{
	*secs = 0;
	*microsecs = 0;
}

bool
TimestampDifferenceExceeds(TimestampTz start_time pg_attribute_unused(),
						   TimestampTz stop_time pg_attribute_unused(),
						   int msec pg_attribute_unused())
{
	return false;
}

const char *
timestamptz_to_str(TimestampTz dt pg_attribute_unused())
{
	return "(stub)";
}

int
errdetail(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errmsg_internal(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

int
errhint(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

/*
 * pg_snprintf stub: cluster_startup_phase.c uses snprintf which is
 * macro'd to pg_snprintf in PG.  Forward to libc snprintf in unit
 * test.  Variadic forwarding via vsnprintf.
 */
#include <stdarg.h>
int
pg_snprintf(char *str, size_t count, const char *fmt, ...)
{
	int n;
	va_list ap;

	va_start(ap, fmt);
	n = vsnprintf(str, count, fmt, ap);
	va_end(ap);
	return n;
}

bool
errstart(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

bool
errstart_cold(int elevel pg_attribute_unused(), const char *domain pg_attribute_unused())
{
	return false;
}

int
errcode(int sqlerrcode pg_attribute_unused())
{
	return 0;
}

int
errmsg(const char *fmt pg_attribute_unused(), ...)
{
	return 0;
}

void
errfinish(const char *filename pg_attribute_unused(), int lineno pg_attribute_unused(),
		  const char *funcname pg_attribute_unused())
{}

void
InitMaterializedSRF(FunctionCallInfo fcinfo pg_attribute_unused(),
					bits32 flags pg_attribute_unused())
{}

void
tuplestore_putvalues(Tuplestorestate *state pg_attribute_unused(),
					 TupleDesc tdesc pg_attribute_unused(), Datum *values pg_attribute_unused(),
					 bool *isnull pg_attribute_unused())
{}

text *
cstring_to_text(const char *s pg_attribute_unused())
{
	return NULL;
}

char *
psprintf(const char *fmt pg_attribute_unused(), ...)
{
	return (char *)"";
}

char *
pstrdup(const char *in pg_attribute_unused())
{
	return (char *)"";
}

Datum
DirectFunctionCall1Coll(PGFunction func pg_attribute_unused(), Oid collation pg_attribute_unused(),
						Datum arg1 pg_attribute_unused())
{
	return (Datum)0;
}

Datum
timestamptz_out(PG_FUNCTION_ARGS)
{
	return (Datum)0;
}


/*
 * Spec-1.10.1 D1 F1 stubs: cluster_startup_phase.o now references
 * the LWLock + ShmemInitStruct API (phase state migrated to shmem)
 * plus the cluster.phase{1..4}_timeout GUC variables (D2 F2 driver
 * elapsed check) plus cluster_shmem_register_region (region registry).
 * The unit test never invokes the runtime paths -- these are
 * address-only / NULL stubs so cluster_debug.o links standalone.
 */
#include "storage/lwlock.h"
#include "storage/shmem.h"

void
LWLockInitialize(LWLock *lock pg_attribute_unused(), int tranche_id pg_attribute_unused())
{}

bool
LWLockAcquire(LWLock *lock pg_attribute_unused(), LWLockMode mode pg_attribute_unused())
{
	return true;
}

void
LWLockRelease(LWLock *lock pg_attribute_unused())
{}

void *
ShmemInitStruct(const char *name pg_attribute_unused(), Size size pg_attribute_unused(),
				bool *foundPtr)
{
	if (foundPtr != NULL)
		*foundPtr = false;
	return NULL;
}

int cluster_phase1_timeout = 60;
int cluster_phase2_timeout = 30;
int cluster_phase3_timeout = 600;
int cluster_phase4_timeout = 30;
/* Spec-1.11 Sprint B: cluster_startup_phase.c references cluster_enabled */
bool cluster_enabled = true;
/* Spec-2.1 D1: cluster_startup_phase.c + cluster_conf.c reference allow_single_node */
bool cluster_allow_single_node = true;
/* spec-2.6 Q7 validator: cluster_startup_phase.c reads cluster_voting_disks */
char *cluster_voting_disks = NULL;

#include "cluster/cluster_shmem.h"
void
cluster_shmem_register_region(const ClusterShmemRegion *region pg_attribute_unused())
{}

/*
 * Spec-1.11 Sprint A stubs: cluster_startup_phase.o now references
 * cluster_lmon_start + cluster_lmon_wait_for_ready (phase_1_handler
 * spawn + sync wait).  test_cluster_debug never invokes phase_1_handler
 * so these are address-only no-op stubs.
 */
int
cluster_lmon_start(void)
{
	return 0;
}

bool
cluster_lmon_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/*
 * Spec-1.11.1 F11 stubs: dump_lmon now reads 5 new accessors; test
 * harness never invokes runtime path so address-only no-op stubs.
 */
pid_t
cluster_lmon_pid(void)
{
	return 0;
}
TimestampTz
cluster_lmon_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_lmon_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_lmon_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_lmon_main_loop_iters(void)
{
	return 0;
}
uint64
cluster_lmon_last_iter_us(void)
{
	return 0;
}
uint64
cluster_lmon_max_iter_us(void)
{
	return 0;
}
uint64
cluster_lmon_slow_iter_count(void)
{
	return 0;
}
int
cluster_lmon_status(void)
{
	return 0;
}
const char *
cluster_lmon_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/* Spec-1.12 D6+D12 stubs: cluster_startup_phase.o now references
 * cluster_lck_start + cluster_lck_wait_for_ready; cluster_debug.o
 * dump_lck now references 6 lck_* accessors. */
int
cluster_lck_start(void)
{
	return 0;
}
bool
cluster_lck_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_lck_status(void)
{
	return 0;
}
const char *
cluster_lck_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_lck_pid(void)
{
	return 0;
}
TimestampTz
cluster_lck_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_lck_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_lck_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_lck_main_loop_iters(void)
{
	return 0;
}

/* Spec-1.13 D6+D12 stubs: cluster_startup_phase.o references
 * cluster_diag_start + cluster_diag_wait_for_ready; cluster_debug.o
 * dump_diag references 7 diag_* accessors. */
int
cluster_diag_start(void)
{
	return 0;
}
bool
cluster_diag_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_diag_status(void)
{
	return 0;
}
const char *
cluster_diag_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/*
 * spec-5.11: dump_hang dependencies.  The Hang Manager runtime + GUCs live in
 * cluster_hang.c / cluster_guc.c; here we only need them to resolve at link
 * time (dump_hang short-circuits on an unavailable DIAG region).
 */
bool cluster_hang_manager_enabled = false;
bool cluster_hang_dump_enabled = false;
int cluster_hang_threshold_ms = 60000;
int cluster_hang_sample_interval_ms = 10000;
int cluster_hang_max_sampled = 64;
void
cluster_hang_get_dump_data(ClusterHangDumpData *out)
{
	out->available = false;
}
const char *
cluster_hang_wait_source_str(uint8 s pg_attribute_unused())
{
	return "(stub)";
}
const char *
cluster_hang_quality_str(uint8 q pg_attribute_unused())
{
	return "(stub)";
}

/* spec-5.12 D8: dump_hang now also reads the disposition counters / mode. */
int cluster_hang_resolution_mode = 0;
void
cluster_hang_resolve_get_counters(ClusterHangResolveCounters *out)
{
	memset(out, 0, sizeof(*out));
}
const char *
cluster_hang_resolve_mode_str(int mode pg_attribute_unused())
{
	return "(stub)";
}
const char *
cluster_hang_action_tier_str(int tier pg_attribute_unused())
{
	return "(stub)";
}

pid_t
cluster_diag_pid(void)
{
	return 0;
}
TimestampTz
cluster_diag_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_diag_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_diag_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_diag_main_loop_iters(void)
{
	return 0;
}

/* Spec-1.14 D6+D12 stubs: cluster_startup_phase.o references
 * cluster_stats_start + cluster_stats_wait_for_ready; cluster_debug.o
 * dump_cluster_stats references 7 cluster_stats_* accessors. */
int
cluster_stats_start(void)
{
	return 0;
}
bool
cluster_stats_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/* Spec-2.5 D4 stubs: same for CSSD. */
int
cluster_cssd_start(void)
{
	return 0;
}
bool
cluster_cssd_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/* spec-2.6 Sprint A Step 3 D7 stubs: same for QVOTEC. */
pid_t
cluster_qvotec_start(void)
{
	return 0;
}
bool
cluster_qvotec_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}

/* spec-2.18 Sprint A stubs: same for LMS. */
int
cluster_lms_start(void)
{
	return 0;
}
bool
cluster_lms_wait_for_ready(int timeout_ms pg_attribute_unused())
{
	return false;
}
int
cluster_stats_status(void)
{
	return 0;
}
const char *
cluster_stats_status_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_stats_pid(void)
{
	return 0;
}
TimestampTz
cluster_stats_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_stats_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_stats_last_liveness_tick_at(void)
{
	return 0;
}
int64
cluster_stats_main_loop_iters(void)
{
	return 0;
}

/* Spec-2.5 D12 stubs: cluster_debug.c calls cluster_cssd_* accessors
 * via dump_cluster_cssd();test stubs return 0 / sentinel. */
#include "cluster/cluster_cssd.h"
ClusterCssdStatus
cluster_cssd_get_status(void)
{
	return CLUSTER_CSSD_STARTING;
}
const char *
cluster_cssd_status_to_string(ClusterCssdStatus s pg_attribute_unused())
{
	return "(stub)";
}
pid_t
cluster_cssd_get_pid(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_spawned_at(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_ready_at(void)
{
	return 0;
}
TimestampTz
cluster_cssd_get_last_liveness_tick_at(void)
{
	return 0;
}
uint64
cluster_cssd_get_main_loop_iters(void)
{
	return 0;
}

/* spec-2.5 Hardening v1.0.3 stubs:  cluster_debug.c dump_cluster_cssd
 * references declared_alive aggregate accessors;test_cluster_debug
 * standalone link must resolve.  Stub returns 0 / zero bitmap. */
int
cluster_cssd_get_declared_alive_count(void)
{
	return 0;
}
void
cluster_cssd_get_declared_alive_bitmap(uint8 out_bitmap[CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES])
{
	if (out_bitmap != NULL)
		memset(out_bitmap, 0, CLUSTER_CSSD_PEER_ALIVE_BITMAP_BYTES);
}

/* Spec-1.15 SCN encoding-layer stubs (cluster_debug.c references the
 * 7-key dump set; address-only in unit tests). */
SCN
cluster_scn_current(void)
{
	return 0;
}
NodeId
cluster_scn_node_id(void)
{
	return 0;
}
uint64
cluster_scn_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_max_observed_remote(void)
{
	return 0;
}
TimestampTz
cluster_scn_initialized_at(void)
{
	return 0;
}
TimestampTz
cluster_scn_last_advance_at(void)
{
	return 0;
}

/* spec-1.16 stat accessor stubs (cluster_debug references the 3 new
 * counter accessors for dump_scn 7 -> 10 keys). */
uint64
cluster_scn_commit_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_abort_advance_count(void)
{
	return 0;
}
uint64
cluster_scn_observe_bump_count(void)
{
	return 0;
}
/* Spec-1.17 BOC stat accessor stubs. */
uint64
cluster_scn_boc_sweep_count(void)
{
	return 0;
}
TimestampTz
cluster_scn_boc_last_sweep_at(void)
{
	return 0;
}
uint64
cluster_scn_boc_pending_at_last_sweep(void)
{
	return 0;
}
uint64
cluster_scn_boc_max_batch_size(void)
{
	return 0;
}
/* spec-2.10 D5 / L104 stub: cluster_debug emit_row references new
 * cluster_scn module accessor;test_cluster_debug standalone binary
 * doesn't link cluster_scn.o,vacuous stub. */
uint64
cluster_scn_boc_broadcast_fanout_count(void)
{
	return 0;
}

/* spec-7.4 D4 stubs: cluster_debug emit_row references the event-vs-sweep
 * balance accessors; test_cluster_debug does not link cluster_scn.o. */
uint64
cluster_scn_boc_event_publish_count(void)
{
	return 0;
}
uint64
cluster_scn_boc_sweep_fallback_count(void)
{
	return 0;
}

/* spec-2.11 D5 / L104 stub: cluster_debug emit_row references new
 * spec-2.11 cluster_scn module accessor;test_cluster_debug standalone
 * binary doesn't link cluster_scn.o,vacuous stub. */
uint64
cluster_scn_commit_lookup_defer_count(void)
{
	return 0;
}

/* spec-2.12 D4 / D5 / L104 stubs: cluster_debug emit_row references 2
 * new spec-2.12 cluster_scn module accessors (scn_last_observe_at +
 * scn_observed_max_observe_gap_ms);test_cluster_debug standalone
 * binary doesn't link cluster_scn.o,vacuous stubs. */
TimestampTz
cluster_scn_last_observe_at(void)
{
	return 0;
}

/* Spec-7.4 D1 durable frontier / BOC payload accessor stubs. */
SCN
cluster_scn_durable_safe_scn(void)
{
	return 0;
}
uint64
cluster_scn_durable_pending_count(void)
{
	return 0;
}
bool
cluster_scn_durable_frontier_frozen(void)
{
	return false;
}
uint64
cluster_scn_durable_frontier_overflow_count(void)
{
	return 0;
}
uint64
cluster_scn_durable_frontier_regression_count(void)
{
	return 0;
}
uint64
cluster_scn_boc_payload_accept_count(void)
{
	return 0;
}
uint64
cluster_scn_boc_payload_bad_length_count(void)
{
	return 0;
}
uint64
cluster_scn_boc_payload_node_mismatch_count(void)
{
	return 0;
}
uint64
cluster_scn_boc_payload_regression_count(void)
{
	return 0;
}
bool
cluster_scn_remote_durable_safe(NodeId origin pg_attribute_unused(),
								uint64 *epoch_out pg_attribute_unused(),
								SCN *scn_out pg_attribute_unused())
{
	return false;
}
uint64
cluster_scn_observed_max_observe_gap_ms(void)
{
	return 0;
}

/* spec-2.13 D8 / L104 stubs: cluster_debug dump_ges references 2 new
 * spec-2.13 cluster_ges module accessors;  test_cluster_debug standalone
 * binary doesn't link cluster_ges.o,  vacuous stubs. */
uint64
cluster_ges_request_defer_count(void)
{
	return 0;
}

uint64
cluster_ges_reply_defer_count(void)
{
	return 0;
}

/* spec-2.14 D12 / L104 stubs: cluster_debug dump_grd references 7 new
 * spec-2.14 cluster_grd module accessors;  test_cluster_debug standalone
 * binary doesn't link cluster_grd.o,  vacuous stubs. */
uint32
cluster_grd_local_master_count(void)
{
	return 0;
}

uint32
cluster_grd_remote_master_count(void)
{
	return 0;
}

uint64
cluster_grd_shard_lookup_count(void)
{
	return 0;
}

uint64
cluster_grd_local_master_lookup_count(void)
{
	return 0;
}

uint64
cluster_grd_remote_master_lookup_count(void)
{
	return 0;
}

uint64
cluster_grd_resid_encode_count(void)
{
	return 0;
}

uint64
cluster_grd_master_map_refresh_count_get(void)
{
	return 0;
}

/* spec-2.15 D12 L104 stubs:  6 NEW accessor for dump_grd 6 NEW emit_row. */
int
cluster_grd_max_entries_get(void)
{
	return 0;
}

int
cluster_grd_entry_count(void)
{
	return 0;
}

Size
cluster_grd_allocated_bytes(void)
{
	return 0;
}

uint64
cluster_grd_entry_create_count(void)
{
	return 0;
}

uint64
cluster_grd_entry_lookup_hit_count(void)
{
	return 0;
}

uint64
cluster_grd_entry_full_count(void)
{
	return 0;
}

uint64
cluster_grd_entries_reclaimed_count(void)
{
	return 0;
}

uint64
cluster_grd_reclaim_skipped_pinned_count(void)
{
	return 0;
}

uint64
cluster_grd_pin_high_water(void)
{
	return 0;
}

uint64
cluster_grd_sweep_runs(void)
{
	return 0;
}

uint64
cluster_grd_holders_full_count(void)
{
	return 0;
}
uint64
cluster_grd_waiters_full_count(void)
{
	return 0;
}
uint64
cluster_grd_converts_full_count(void)
{
	return 0;
}
/* spec-5.10 D7 — GES lock-starvation counter stubs (dump_grd refs). */
uint64
cluster_grd_starvation_boost_count(void)
{
	return 0;
}
uint64
cluster_grd_starvation_barrier_enqueued_count(void)
{
	return 0;
}
uint64
cluster_grd_starvation_barrier_publish_fail_count(void)
{
	return 0;
}
uint64
cluster_grd_starvation_max_skip_observed(void)
{
	return 0;
}

/* spec-5.1b D9 — convert state-machine counter stubs (dump_grd refs). */
uint64
cluster_grd_convert_granted_inplace_count(void)
{
	return 0;
}
uint64
cluster_grd_convert_enqueued_count(void)
{
	return 0;
}
uint64
cluster_grd_convert_illegal_count(void)
{
	return 0;
}
uint64
cluster_grd_ngranted_promoted_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_work_queue_full_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_cleanup_deferred_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_inbound_validation_fail_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_reply_deferred_count(void)
{
	return 0;
}
uint64
cluster_grd_ges_reply_dropped_count(void)
{
	return 0;
}

/* spec-2.17 D27 — 9 NEW counter stubs(BAST 6 + deadlock 3). */
uint64
cluster_grd_bast_sent_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_received_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_ack_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_retry_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_reject_count(void)
{
	return 0;
}
uint64
cluster_grd_bast_stale_drop_count(void)
{
	return 0;
}
uint64
cluster_grd_deadlock_probe_drop_count(void)
{
	return 0;
}
uint64
cluster_grd_deadlock_probe_collision_drop_count(void)
{
	return 0;
}
uint64
cluster_grd_deadlock_chunk_oo_buffer_overflow_count(void)
{
	return 0;
}

uint32
cluster_grd_recovery_state_value(void)
{
	return (uint32)GRD_RECOVERY_IDLE;
}

const char *
cluster_grd_recovery_state_name(uint32 state pg_attribute_unused())
{
	return "idle";
}

uint64
cluster_grd_recovery_last_event_id(void)
{
	return 0;
}

uint64
cluster_grd_recovery_event_old_epoch(void)
{
	return 0;
}

uint64
cluster_grd_recovery_episode_epoch_value(void)
{
	return 0;
}

uint32
cluster_grd_recovery_event_coordinator(void)
{
	return 0;
}

uint64
cluster_grd_recovery_done_epoch_for(int32 node pg_attribute_unused())
{
	return 0;
}

uint64
cluster_grd_recovery_done_bitmap_hash_for(int32 node pg_attribute_unused())
{
	return 0;
}

int
cluster_grd_recovery_block_redeclare_cursor(void)
{
	return 0;
}

uint64
cluster_grd_recovery_block_redeclare_epoch(void)
{
	return 0;
}

bool
cluster_grd_recovery_block_redeclare_done(void)
{
	return true;
}

/* spec-4.6 D5 stub: dump_grd_recovery consumes the bulk counter snapshot. */
void
cluster_grd_recovery_counters_snapshot(ClusterGrdRecoveryCounters *out)
{
	memset(out, 0, sizeof(*out));
}

uint32
cluster_grd_outbound_ring_depth(void)
{
	return 0;
}
uint32
cluster_grd_outbound_reply_dirty_depth(void)
{
	return 0;
}
uint32
cluster_grd_outbound_cleanup_dirty_depth(void)
{
	return 0;
}
uint32
cluster_grd_work_queue_depth(void)
{
	return 0;
}
uint64
cluster_grd_pending_count(void)
{
	return 0;
}


UT_DEFINE_GLOBALS();

/* spec-5.14 D6: link-only stubs for the touched_peers counters/hex that
 * dump_reconfig_touched emits.  The real getters live in cluster_reconfig.o
 * (not linked here); their behaviour is covered by cluster_tap t/307. */
uint64
cluster_reconfig_get_touched_abort_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_touched_stamp_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_touched_stamp_by_kind(int kind pg_attribute_unused())
{
	return 0;
}
uint64
cluster_reconfig_get_clean_leave_rejected_count(void)
{
	return 0;
}
/* spec-5.15 D6 — online-join counter stubs (dump_reconfig_join). */
uint64
cluster_reconfig_get_join_pending_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_apply_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_reject_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_join_timeout_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_clean_departed_cleared_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_marker_slow_ack_count(void)
{
	return 0;
}
uint64
cluster_reconfig_get_marker_timeout_count(void)
{
	return 0;
}
void
cluster_touched_peers_self_hex(char *buf, Size buflen)
{
	snprintf(buf, buflen, "0x0000000000000000");
}


/* ============================================================
 * SRF entry-point linkability.
 * ============================================================ */

UT_TEST(test_debug_dump_srf_linkable)
{
	UT_ASSERT_NOT_NULL((void *)cluster_dump_state);
}


/* ============================================================
 * Iterator API on cluster_inject (added in spec-0.29 §1.4).
 * ============================================================ */

UT_TEST(test_debug_inject_get_count_callable)
{
	/* Stub returns 0; verifies the symbol is reachable. */
	UT_ASSERT_EQ(cluster_injection_get_count(), 0);
}

UT_TEST(test_debug_inject_get_state_at_out_of_range)
{
	const char *name = NULL;
	ClusterInjectFaultType type = CLUSTER_FAULT_NONE;
	uint64 hits = 0;

	/* Stub always returns false; real impl returns false when idx<0
	 * or idx>=count.  The contract is the same: out-of-range yields
	 * false, output args untouched. */
	UT_ASSERT_EQ(cluster_injection_get_state_at(-1, &name, &type, &hits), false);
	UT_ASSERT_EQ(cluster_injection_get_state_at(1000, &name, &type, &hits), false);
}

UT_TEST(test_debug_inject_get_state_at_null_outs)
{
	/* Out-pointers may be NULL; helper must not crash. */
	(void)cluster_injection_get_state_at(0, NULL, NULL, NULL);
	UT_ASSERT(true);
}


/* ============================================================
 * Iterator API on cluster_pgstat (added in spec-0.29 §1.4).
 * ============================================================ */

UT_TEST(test_debug_pgstat_get_count_callable)
{
	UT_ASSERT_EQ(cluster_pgstat_get_count(), 0);
}

UT_TEST(test_debug_pgstat_get_at_out_of_range)
{
	const char *name = NULL;
	uint64 value = 0;

	UT_ASSERT_EQ(cluster_pgstat_get_at(-1, &name, &value), false);
	UT_ASSERT_EQ(cluster_pgstat_get_at(1000, &name, &value), false);
}

UT_TEST(test_debug_pgstat_get_at_null_outs)
{
	(void)cluster_pgstat_get_at(0, NULL, NULL);
	UT_ASSERT(true);
}


/* ============================================================
 * Cross-module symbol resolution checks.
 *
 *	If any cluster_*.h public API drifts (rename / removal),
 *	cluster_debug.c will fail to link, which is what we want to
 *	catch at compile time.  The tests below address-take symbols
 *	cluster_debug.c references to surface link-time breakage early.
 * ============================================================ */

UT_TEST(test_debug_links_against_inject_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_injection_get_count);
	UT_ASSERT_NOT_NULL((void *)cluster_injection_get_state_at);
}

/*
 * spec-2.18 Sprint A Step 4 D10 L104 stubs: dump_cluster_lms references
 * cluster_lms_* accessors via cluster_debug.o; standalone test
 * harness must provide local zero-returning stubs.
 */
int
cluster_lms_get_state(void)
{
	return 0;
}
uint64
cluster_lms_get_started_count(void)
{
	return 0;
}
uint64
cluster_lms_get_work_drained_count(void)
{
	return 0;
}
/*
 * spec-2.20 D9 — 3 NEW LMS decision counter stubs (replacing single
 * lms_decision_count).  Each grant body inc exactly one (mutually
 * exclusive).
 */
uint64
cluster_lms_get_decision_grant_count(void)
{
	return 0;
}
uint64
cluster_lms_get_decision_reject_count(void)
{
	return 0;
}
uint64
cluster_lms_get_decision_convert_count(void)
{
	return 0;
}
uint64
cluster_lms_get_drain_empty_count(void)
{
	return 0;
}
uint64
cluster_lms_get_error_count(void)
{
	return 0;
}
/* spec-2.25 D13 R10 stub audit — 7 NEW native-lock probe counter accessors. */
uint64
cluster_lms_get_native_probe_sent_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_reply_recv_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_collector_slot_full_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_aggregate_holder_conflict_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_aggregate_waiter_conflict_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_retry_count(void)
{
	return 0;
}
uint64
cluster_lms_get_native_probe_timeout_count(void)
{
	return 0;
}
/* spec-2.27 D7 R10 stub audit — priority starvation observability counter. */
uint64
cluster_lms_get_priority_starvation_observed_count(void)
{
	return 0;
}
/* spec-7.3 D8 — per-worker observability stubs (+ the pool-size GUC the
 * dump uses to bound the per-worker rows). */
int cluster_lms_workers = 2;
uint64
cluster_lms_obs_get_dispatch_count(int worker_id pg_attribute_unused())
{
	return 0;
}
uint64
cluster_lms_obs_get_direct_reply_count(int worker_id pg_attribute_unused())
{
	return 0;
}
uint64
cluster_lms_obs_get_conn_reset_count(int worker_id pg_attribute_unused())
{
	return 0;
}
uint64
cluster_lms_obs_get_inline_serve_count(int worker_id pg_attribute_unused())
{
	return 0;
}
uint64
cluster_lms_obs_get_serve_hist(int worker_id pg_attribute_unused(),
							   int bucket pg_attribute_unused())
{
	return 0;
}
uint64
cluster_lms_obs_serve_hist_bound_us(int bucket)
{
	static const uint64 bounds[15] = { 50,	  100,	 200,	 500,	 1000,	 2000,	  5000,	  10000,
									   20000, 50000, 100000, 200000, 500000, 1000000, 5000000 };

	if (bucket < 0 || bucket >= 15)
		return UINT64_MAX;
	return bounds[bucket];
}
const char *
cluster_lms_state_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/*
 * spec-2.19 Sprint A Step 4 D10 L104 stubs: dump_lmd references
 * cluster_lmd_* accessors via cluster_debug.o; standalone test
 * harness must provide local zero-returning stubs.
 */
int
cluster_lmd_get_state(void)
{
	return 0;
}
uint64
cluster_lmd_get_started_count(void)
{
	return 0;
}
TimestampTz
cluster_lmd_get_ready_at(void)
{
	return 0;
}
uint64
cluster_lmd_get_edge_submission_count(void)
{
	return 0;
}
uint64
cluster_lmd_get_wake_count(void)
{
	return 0;
}
uint64
cluster_lmd_get_idle_count(void)
{
	return 0;
}
uint64
cluster_lmd_get_error_count(void)
{
	return 0;
}
const char *
cluster_lmd_state_to_string(int s pg_attribute_unused())
{
	return "(stub)";
}

/* spec-2.22 D12 — LMD Tarjan + graph counter stubs (9 NEW). */
uint64
cluster_lmd_wait_edge_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_wait_edge_full_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_graph_generation_get(void)
{
	return 0;
}
uint64
cluster_lmd_tarjan_scan_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cycle_detected_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_victim_cancel_sent_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_revalidate_fail_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cross_node_victim_pending_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_inject_call_count_get(void)
{
	return 0;
}
/* spec-2.23 D13 stub audit — new dump_ges / dump_lmd counter rows. */
uint64
cluster_ges_reply_wait_table_active_count(void)
{
	return 0;
}
uint64
cluster_ges_reply_late_drop_count(void)
{
	return 0;
}
uint64
cluster_ges_release_ack_count(void)
{
	return 0;
}
/* spec-5.2 D4/D6 stubs: TX enqueue completion-wait counters. */
uint64
cluster_txw_get_wait_count(void)
{
	return 0;
}
uint64
cluster_txw_get_wakeup_count(void)
{
	return 0;
}
uint64
cluster_txw_get_timeout_count(void)
{
	return 0;
}
uint64
cluster_lmd_probe_broadcast_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_probe_partial_count_get(void)
{
	return 0;
}
/* spec-2.24 D13 stub audit — 6 NEW lmd counters + 1 NEW grd counter. */
uint64
cluster_lmd_cross_node_victim_cancel_sent_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cross_node_cancel_received_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cross_node_cancel_queue_full_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cleanup_on_backend_exit_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cleanup_lmd_sweep_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cleanup_skip_other_owner_count_get(void)
{
	return 0;
}
/* spec-5.8 D6 stub audit — 3 NEW coordinator confirm / reconfig-gate counters. */
uint64
cluster_lmd_deadlock_confirmed_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_confirm_unconfirmed_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_reconfig_discard_count_get(void)
{
	return 0;
}
/* spec-5.8 Hardening v1.0.1 stub — FC1 acting-gate counter. */
uint64
cluster_lmd_member_incomplete_count_get(void)
{
	return 0;
}
/* spec-5.9 D10 stub — 13 NEW victim-policy + cancel-robustness counters. */
uint64
cluster_lmd_victim_protected_skip_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_victim_repeat_avoided_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_token_installed_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_consumed_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_stale_cleared_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_wait_stale_rejected_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_ack_received_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_retransmit_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_escalated_alternate_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_exhausted_timeout_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cancel_no_safe_victim_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_cleanup_orphan_edge_swept_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_reconfig_cancel_discarded_count_get(void)
{
	return 0;
}
/* spec-5.9 Hardening v1.0.1 (P1#1) stub — CANCEL_ACK victim/wait_seq mismatch. */
uint64
cluster_lmd_cancel_ack_mismatch_count_get(void)
{
	return 0;
}
/* spec-5.8 D8 stub audit — 5 NEW shmem REPORT-collector counters. */
uint64
cluster_lmd_probe_report_enqueue_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_probe_drop_stale_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_probe_drop_duplicate_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_probe_queue_full_count_get(void)
{
	return 0;
}
uint64
cluster_lmd_probe_partial_report_count_get(void)
{
	return 0;
}
uint64
cluster_grd_cleanup_skip_stale_cancel_count(void)
{
	return 0;
}
/* spec-2.25 D13 R10 stub audit — RELATION + OBJECT gate hit counter. */
uint64
cluster_grd_relation_object_cluster_path_count(void)
{
	return 0;
}
void
cluster_grd_inc_relation_object_cluster_path(void)
{}

/* spec-2.26 D5 R10 stub audit — TRANSACTION gate hit counter. */
uint64
cluster_grd_transaction_cluster_path_count(void)
{
	return 0;
}
void
cluster_grd_inc_transaction_cluster_path(void)
{}

UT_TEST(test_debug_links_against_pgstat_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_pgstat_get_count);
	UT_ASSERT_NOT_NULL((void *)cluster_pgstat_get_at);
}

UT_TEST(test_debug_links_against_conf_module)
{
	UT_ASSERT_NOT_NULL((void *)cluster_conf_node_count);
	UT_ASSERT_NOT_NULL((void *)cluster_conf_lookup_node);
}

UT_TEST(test_debug_phase_symbol_present)
{
	UT_ASSERT_NOT_NULL(cluster_phase);
}


int
main(void)
{
	UT_PLAN(11);
	UT_RUN(test_debug_dump_srf_linkable);
	UT_RUN(test_debug_inject_get_count_callable);
	UT_RUN(test_debug_inject_get_state_at_out_of_range);
	UT_RUN(test_debug_inject_get_state_at_null_outs);
	UT_RUN(test_debug_pgstat_get_count_callable);
	UT_RUN(test_debug_pgstat_get_at_out_of_range);
	UT_RUN(test_debug_pgstat_get_at_null_outs);
	UT_RUN(test_debug_links_against_inject_module);
	UT_RUN(test_debug_links_against_pgstat_module);
	UT_RUN(test_debug_links_against_conf_module);
	UT_RUN(test_debug_phase_symbol_present);
	UT_DONE();
	return ut_failed_count == 0 ? 0 : 1;
}
