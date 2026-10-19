/* Shared heap's conservative native freeze boundary.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_HEAP_HORIZON_H
#define CLUSTER_HEAP_HORIZON_H

#include "access/transam.h"

/* Before heap mutation, cap a local proposal at the immutable activation
 * floor: another slot can still issue an XID below this node's proposal.
 * Not a complete cluster freeze/reclamation horizon. Refusal preserves out.
 * The caller excludes temporary relations. Nonshared mode is native. */
extern bool cluster_heap_freeze_cutoff_v1(TransactionId proposed, TransactionId *out);

/*
 * Frozen 2.14 M1 waterline report.  The wire image is deliberately encoded
 * field-by-field in little-endian order; sizeof(ClusterHeapFreezeReportV2)
 * is not part of the ABI.  The report reuses the undo-horizon interconnect
 * message number while advertising the V2 capability bit.
 */
#define CLUSTER_HEAP_FREEZE_REPORT_V2_VERSION 2
#define CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES 120

#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_VERSION 0
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_LENGTH 2
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FLAGS 4
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SYSID 8
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RECORD_GENERATION 16
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FORMATION_EPOCH 24
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_INCARNATION 32
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REPORT_SEQUENCE 40
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_FUTURE_FULL_XID 48
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ACTIVE_FULL_XID_MIN 56
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_HORIZON_SCN 64
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_REQUESTED_FULL_XID 72
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_ORIGIN 80
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_SENDER_INTERVAL_MS 84
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_F_SAMPLE_ID 88
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_SAMPLE_CLOCK_SCN 96
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_KIND 104
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_RESERVED0 108
#define CLUSTER_HEAP_FREEZE_REPORT_V2_OFF_H_CONTRIBUTOR_TOKEN 112

typedef enum ClusterHeapFreezeReportV2ContributorKind
{
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_UNKNOWN = 0,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_CLOCK = 1,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_SNAPSHOT = 2,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_PREPARED = 3,
	CLUSTER_HEAP_FREEZE_REPORT_V2_CONTRIBUTOR_REPLICATION = 4
} ClusterHeapFreezeReportV2ContributorKind;

typedef struct ClusterHeapFreezeReportV2
{
	uint16 version;
	uint16 length;
	uint32 flags;
	uint64 sysid;
	uint64 record_generation;
	uint64 formation_epoch;
	uint64 incarnation;
	uint64 report_sequence;
	uint64 future_full_xid;
	uint64 active_full_xid_min;
	uint64 horizon_scn;
	uint64 requested_full_xid;
	uint32 origin;
	uint32 sender_interval_ms;
	uint64 f_sample_id;
	uint64 h_sample_clock_scn;
	uint32 h_contributor_kind;
	uint32 reserved0;
	uint64 h_contributor_token;
} ClusterHeapFreezeReportV2;

extern bool cluster_heap_freeze_report_v2_encode(
	const ClusterHeapFreezeReportV2 *report,
	uint8 out[CLUSTER_HEAP_FREEZE_REPORT_V2_BYTES]);
extern bool cluster_heap_freeze_report_v2_decode(
	const uint8 *bytes, size_t length, ClusterHeapFreezeReportV2 *out);

#endif
