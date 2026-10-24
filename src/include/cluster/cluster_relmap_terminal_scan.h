/* Original retained native terminal observations, not release authority.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#ifndef CLUSTER_RELMAP_TERMINAL_SCAN_H
#define CLUSTER_RELMAP_TERMINAL_SCAN_H
#ifdef USE_PGRAC_CLUSTER
#include "access/clog.h"
#include "access/transam.h"
#include "cluster/cluster_relmap_checkpoint.h"
#include "cluster/cluster_wal_inputs.h"
typedef struct ClusterRelmapTerminalScanV1 ClusterRelmapTerminalScanV1;
struct ClusterRelmapRetentionDirectory;
struct ClusterRelmapRecoverySourceObservationV1;
/* Dormant Startup-only checkpoint input to a NEW, entirely zeroed and
 * exclusively owned directory. Select one physical checkpoint for origin;
 * require the original cold scope and its unchanged serial, then validate
 * the complete selected retained source and exact native checkpoint/window.
 * Derive BUILDING generation/floor/actual-last-issued from those originals;
 * no caller supplies these values. Output is published once, after source,
 * whole ROOT/pin and original-round rechecks. False/ERROR leaves it unchanged.
 * Nonzero/overlapping storage is refused, never reset or reused.
 *
 * These are checkpoint observations, not a bound on later suffix issuance,
 * qualified native base/lifetime/era, full semantic fold or complete origins.
 * Original cold scope/inputs/ResourceOwner must remain held through later
 * census use. Physical census/pending restore and final owner qualification
 * remain required. No target/native write, COMPLETE, pin release, allocator
 * admission, new scope or activation; do not hold a retention lock over I/O. */
extern bool
cluster_relmap_recovery_retention_build_start_v1(ClusterWalInputsV1 *inputs, uint16 origin,
												 struct ClusterRelmapRetentionDirectory *out);
/* Same provisional checkpoint BUILDING projection under the original cold
 * execution owner, after its census read scope has been released. Uses only
 * that owner's exact ROOT/source/serial and complete retained WAL visit; no
 * read pin or replacement scope is acquired. out must be NEW, entirely zero
 * exclusive storage. False/ERROR leaves it unchanged. All qualification and
 * lifetime limits above still apply; original owner/pins remain caller-owned. */
extern bool
cluster_relmap_recovery_retention_build_start_cold_v1(uint16 origin,
													  struct ClusterRelmapRetentionDirectory *out);
/* Same checkpoint projection, with its original source/full ROOT/redo/serial
 * and the original single-source guard. Both outputs must be NEW, entirely
 * zero, exclusive and disjoint; false/ERROR leaves both unchanged. Publication
 * has no fallible work between the two assignments. This snapshot grants no
 * additional native/history/lifetime/origin-set qualification or authority. */
extern bool cluster_relmap_recovery_retention_build_start_cold_source_v1(
	uint16 origin, struct ClusterRelmapRetentionDirectory *directory,
	struct ClusterRelmapRecoverySourceObservationV1 *source);
/* Dormant original cold execution owner only, Startup/recovery/no-CRIT with
 * shared-catalog/sole-merger/namespace X. Authenticate the supplied original
 * source/full ROOT/redo/nonreused serial before and after original storage
 * namespace I/O. Return only a new tracked read-only directory fd, or -1;
 * refusal/ERROR closes local fds, never original protections. It is a physical
 * input, not isolation/native/lifetime/all-origin/ROOT authority. Retain the
 * exact original owner/protections through all later ROOT/census use, destroy
 * all dependent cursors and CloseTransientFile on every exit, including
 * PG_FINALLY and before the original fd.c lifetime ends. No live caller. */
extern int cluster_relmap_recovery_root_open_cold_v1(
	const struct ClusterRelmapRecoverySourceObservationV1 *source);
typedef struct ClusterRelmapTerminalObservationV1 {
	ClusterWalSourceRef source;
	ClusterRelmapCheckpointWindow window;
	uint64 requested_top_full_xid; /* Query identity, NOT certified terminal era. */
	XidStatus status;
	XLogRecPtr terminal_start, terminal_end, cut;
	/* Nonzero only for a v2 prefix scan; never a bound on the later suffix. */
	uint64 post_flush_issued;
	XLogRecPtr prefix_end;
	uint64 records;
	uint8 fingerprint[32];
} ClusterRelmapTerminalObservationV1;
/* A single-call native image bound to the original retained prefix. This is
 * an observation, not a retained lifetime or a certified FullXid era. */
typedef struct ClusterRelmapTerminalBaseObservationV1 {
	ClusterRelmapTerminalObservationV1 terminal;
	ClusterClogNativeBaseV1 native_base;
	uint8 fingerprint[32];
} ClusterRelmapTerminalBaseObservationV1;
/* Dormant full original retained scan; one physical input for this origin
 * only. Complete native checkpoint and bounded COMMIT/ABORT carrier required.
 * Every native XACT carrier must be bounded COMMIT/ABORT; unsupported XACT
 * and query-as-child refuse. Missing/duplicate/contradictory target or damaged
 * suffix refuses. The query
 * must lie in the persisted observed window; raw32 match does NOT prove its
 * era/base/retention lifetime or actual effect durability. Not terminal_read.
 * Borrowed inputs and its original ResourceOwner must outlive this object.
 * No source pin is acquired/released here. ERROR propagates with local cleanup.
 * out is a distinct caller-owned pointer variable; refusal sets it NULL. */
extern bool cluster_relmap_recovery_terminal_scan_v1(ClusterWalInputsV1 *inputs, uint32 index,
													 FullTransactionId requested_top,
													 ClusterRelmapTerminalScanV1 **out);
/* Dormant bounded prefix variant. Only a target terminal at/after the
 * selected native redo and BEFORE the exact checkpoint record is accepted.
 * Requires inner v2 actual post-flush issuance bound; legacy windows refuse.
 * The complete original source, including suffix, is still validated.
 * No native base/retention-lifetime/full-history/release qualification. */
extern bool cluster_relmap_recovery_terminal_prefix_scan_v2(ClusterWalInputsV1 *inputs,
															uint32 index,
															FullTransactionId requested_top,
															ClusterRelmapTerminalScanV1 **out);
/* Original scope/current ROOT/pin recheck before observation publish.
 * Nonoverlap refusal clears the full output; input overlap is unchanged.
 * A failed original recheck permanently invalidates this local scan. */
extern bool cluster_relmap_recovery_terminal_scan_get_v1(ClusterRelmapTerminalScanV1 *scan,
														 ClusterRelmapTerminalObservationV1 *out);
/* Dormant original-cold-scope read. Revalidates the borrowed prefix before
 * and after original native base I/O; requires exact origin/raw xid/status.
 * Requires the complete retained native stream to preserve the selected CLOG
 * page after its terminal record; zero/truncate/unknown CLOG carriers refuse.
 * Binds the full disk page into the source fingerprint. No caller-provided
 * image or qualification flag. Once begun, ERROR/refusal invalidates the scan;
 * nonoverlap refusal clears all output, overlap is unchanged. The native
 * truncation lock protects only its original I/O call, not a retained lifetime.
 * Does not certify FullXid era/floor/history/COMPLETE/release. */
extern bool
cluster_relmap_recovery_terminal_prefix_base_read_v1(ClusterRelmapTerminalScanV1 *scan,
													 ClusterRelmapTerminalBaseObservationV1 *out);
/* Caller frees only this private object, never the original owner/pins. */
extern void cluster_relmap_recovery_terminal_scan_release_v1(ClusterRelmapTerminalScanV1 **scan);
#endif
#endif
