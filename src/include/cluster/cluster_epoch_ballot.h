/*-------------------------------------------------------------------------
 *
 * cluster_epoch_ballot.h
 *    Common next-epoch ballot pure codecs (spec-5.15A §2.1A/§2.1A.1).
 *
 *    The structs below are decoded host-order values.  Disk bytes are
 *    accepted and emitted only through the explicit little-endian codecs.
 *    This module owns no mailbox, actor, voting-device, or event-driver I/O.
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_EPOCH_BALLOT_H
#define CLUSTER_EPOCH_BALLOT_H

#include "c.h"

#include "cluster/cluster_conf.h"


#define CLUSTER_EPOCH_BALLOT_MAGIC UINT32_C(0x4c425045)
#define CLUSTER_EPOCH_BALLOT_VERSION UINT16_C(1)
#define CLUSTER_EPOCH_BALLOT_ID_BYTES 32
#define CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES 128
#define CLUSTER_EPOCH_BALLOT_LANE_BYTES 512
#define CLUSTER_EPOCH_BALLOT_BITMAP_BYTES 16
#define CLUSTER_EPOCH_BALLOT_DIGEST_BYTES 16
#define CLUSTER_EPOCH_BALLOT_GRAMMAR_FINGERPRINT UINT64_C(0x8e0dae5b428905e4)

typedef enum ClusterEpochBallotPhase {
	CLUSTER_EPOCH_BALLOT_PHASE_EMPTY = 0,
	CLUSTER_EPOCH_BALLOT_PHASE_PROMISED = 1,
	CLUSTER_EPOCH_BALLOT_PHASE_ACCEPTED = 2,
	CLUSTER_EPOCH_BALLOT_PHASE_SETTLED = 3
} ClusterEpochBallotPhase;

typedef enum ClusterEpochAuthorityTransition {
	CLUSTER_EPOCH_AUTHORITY_GENESIS = 1,
	CLUSTER_EPOCH_AUTHORITY_RESERVE = 2,
	CLUSTER_EPOCH_AUTHORITY_COMMIT = 3,
	CLUSTER_EPOCH_AUTHORITY_ABORT = 4,
	CLUSTER_EPOCH_AUTHORITY_COMMIT_CLOSED = 5,
	CLUSTER_EPOCH_AUTHORITY_ABORT_CLOSED = 6
} ClusterEpochAuthorityTransition;

typedef enum ClusterEpochEventKind {
	CLUSTER_EPOCH_EVENT_GENESIS = 0,
	CLUSTER_EPOCH_EVENT_FAIL_STOP = 1,
	CLUSTER_EPOCH_EVENT_CLEAN_LEAVE = 2,
	CLUSTER_EPOCH_EVENT_ORDINARY_JOIN = 3,
	CLUSTER_EPOCH_EVENT_NODE_REMOVE = 4,
	CLUSTER_EPOCH_EVENT_SAME_NODE_REPLACEMENT = 5
} ClusterEpochEventKind;

#define CLUSTER_EPOCH_AUTHORITY_VALUE_VERSION UINT16_C(1)

typedef struct ClusterEpochBallotId {
	uint64 counter;						  /* 0 */
	int32 proposer_node_id;				  /* 8 */
	uint32 reserved;					  /* 12: zero */
	uint64 proposer_admitted_incarnation; /* 16 */
	uint64 nonce;						  /* 24 */
} ClusterEpochBallotId;

typedef struct ClusterEpochAuthorityValue {
	uint16 value_version;											  /* 0 */
	uint8 transition;												  /* 2 */
	uint8 event_kind;												  /* 3 */
	int32 request_origin_node;										  /* 4 */
	int32 target_node_id;											  /* 8 */
	uint32 reserved0;												  /* 12: zero */
	uint64 authority_generation;									  /* 16 */
	uint64 baseline_epoch;											  /* 24 */
	uint64 reserved_epoch;											  /* 32 */
	uint64 old_incarnation;											  /* 40 */
	uint64 fresh_incarnation;										  /* 48 */
	uint64 request_nonce;											  /* 56 */
	uint8 authority_member_bitmap[CLUSTER_EPOCH_BALLOT_BITMAP_BYTES]; /* 64 */
	uint8 event_subject_bitmap[CLUSTER_EPOCH_BALLOT_BITMAP_BYTES];	  /* 80 */
	uint64 grammar_fingerprint;										  /* 96 */
	uint8 predecessor_digest[CLUSTER_EPOCH_BALLOT_DIGEST_BYTES];	  /* 104 */
	uint8 reserved1[8];												  /* 120: zero */
} ClusterEpochAuthorityValue;

typedef struct ClusterEpochBallotLane {
	uint32 magic;							   /* 0 */
	uint16 version;							   /* 4 */
	uint8 last_write_phase;					   /* 6 */
	uint8 flags;							   /* 7: zero */
	int32 proposer_node_id;					   /* 8 */
	uint32 configured_disk_count;			   /* 12 */
	uint64 proposer_admitted_incarnation;	   /* 16 */
	uint64 lane_generation;					   /* 24 */
	uint64 system_identifier;				   /* 32 */
	uint64 grammar_fingerprint;				   /* 40 */
	ClusterEpochBallotId promised_ballot;	   /* 48 */
	ClusterEpochBallotId accepted_ballot;	   /* 80 */
	ClusterEpochAuthorityValue accepted_value; /* 112 */
	ClusterEpochBallotId settled_ballot;	   /* 240 */
	ClusterEpochAuthorityValue settled_value;  /* 272 */
	uint8 reserved[108];					   /* 400: zero */
	uint32 crc32c;							   /* 508 */
} ClusterEpochBallotLane;

StaticAssertDecl(sizeof(ClusterEpochBallotId) == CLUSTER_EPOCH_BALLOT_ID_BYTES,
				 "epoch ballot id must remain 32 bytes");
StaticAssertDecl(offsetof(ClusterEpochBallotId, counter) == 0,
				 "epoch ballot counter offset must remain 0");
StaticAssertDecl(offsetof(ClusterEpochBallotId, proposer_node_id) == 8,
				 "epoch ballot proposer offset must remain 8");
StaticAssertDecl(offsetof(ClusterEpochBallotId, reserved) == 12,
				 "epoch ballot reserved offset must remain 12");
StaticAssertDecl(offsetof(ClusterEpochBallotId, proposer_admitted_incarnation) == 16,
				 "epoch ballot incarnation offset must remain 16");
StaticAssertDecl(offsetof(ClusterEpochBallotId, nonce) == 24,
				 "epoch ballot nonce offset must remain 24");

StaticAssertDecl(sizeof(ClusterEpochAuthorityValue) == CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES,
				 "epoch authority value must remain 128 bytes");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, value_version) == 0,
				 "authority value version offset must remain 0");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, transition) == 2,
				 "authority transition offset must remain 2");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, event_kind) == 3,
				 "authority event kind offset must remain 3");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, request_origin_node) == 4,
				 "authority request origin offset must remain 4");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, target_node_id) == 8,
				 "authority target offset must remain 8");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, reserved0) == 12,
				 "authority reserved0 offset must remain 12");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, authority_generation) == 16,
				 "authority generation offset must remain 16");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, baseline_epoch) == 24,
				 "authority baseline epoch offset must remain 24");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, reserved_epoch) == 32,
				 "authority reserved epoch offset must remain 32");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, old_incarnation) == 40,
				 "authority old incarnation offset must remain 40");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, fresh_incarnation) == 48,
				 "authority fresh incarnation offset must remain 48");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, request_nonce) == 56,
				 "authority request nonce offset must remain 56");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, authority_member_bitmap) == 64,
				 "authority member bitmap offset must remain 64");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, event_subject_bitmap) == 80,
				 "authority subject bitmap offset must remain 80");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, grammar_fingerprint) == 96,
				 "authority grammar fingerprint offset must remain 96");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, predecessor_digest) == 104,
				 "authority predecessor digest offset must remain 104");
StaticAssertDecl(offsetof(ClusterEpochAuthorityValue, reserved1) == 120,
				 "authority reserved1 offset must remain 120");

StaticAssertDecl(sizeof(ClusterEpochBallotLane) == CLUSTER_EPOCH_BALLOT_LANE_BYTES,
				 "epoch ballot lane must remain 512 bytes");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, magic) == 0,
				 "epoch ballot lane magic offset must remain 0");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, version) == 4,
				 "epoch ballot lane version offset must remain 4");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, last_write_phase) == 6,
				 "epoch ballot lane phase offset must remain 6");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, flags) == 7,
				 "epoch ballot lane flags offset must remain 7");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, proposer_node_id) == 8,
				 "epoch ballot lane proposer offset must remain 8");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, configured_disk_count) == 12,
				 "epoch ballot lane disk count offset must remain 12");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, proposer_admitted_incarnation) == 16,
				 "epoch ballot lane incarnation offset must remain 16");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, lane_generation) == 24,
				 "epoch ballot lane generation offset must remain 24");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, system_identifier) == 32,
				 "epoch ballot lane system id offset must remain 32");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, grammar_fingerprint) == 40,
				 "epoch ballot lane grammar offset must remain 40");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, promised_ballot) == 48,
				 "epoch ballot promised offset must remain 48");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, accepted_ballot) == 80,
				 "epoch ballot accepted offset must remain 80");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, accepted_value) == 112,
				 "epoch ballot accepted value offset must remain 112");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, settled_ballot) == 240,
				 "epoch ballot settled offset must remain 240");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, settled_value) == 272,
				 "epoch ballot settled value offset must remain 272");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, reserved) == 400,
				 "epoch ballot lane reserved offset must remain 400");
StaticAssertDecl(offsetof(ClusterEpochBallotLane, crc32c) == 508,
				 "epoch ballot lane crc offset must remain 508");

extern bool cluster_epoch_ballot_id_is_valid(const ClusterEpochBallotId *ballot);
extern bool cluster_epoch_ballot_id_encode(const ClusterEpochBallotId *ballot,
										   uint8 out[CLUSTER_EPOCH_BALLOT_ID_BYTES]);
extern bool cluster_epoch_ballot_id_decode(const uint8 bytes[CLUSTER_EPOCH_BALLOT_ID_BYTES],
										   ClusterEpochBallotId *out);
extern int cluster_epoch_ballot_id_compare(const ClusterEpochBallotId *a,
										   const ClusterEpochBallotId *b);
extern bool cluster_epoch_ballot_next_counter(uint64 current, uint64 *next);

extern bool cluster_epoch_authority_value_is_valid(const ClusterEpochAuthorityValue *value,
												   uint64 expected_grammar_fingerprint);
extern bool cluster_epoch_authority_value_encode(const ClusterEpochAuthorityValue *value,
												 uint64 expected_grammar_fingerprint,
												 uint8 out[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES]);
extern bool
cluster_epoch_authority_value_decode(const uint8 bytes[CLUSTER_EPOCH_AUTHORITY_VALUE_BYTES],
									 uint64 expected_grammar_fingerprint,
									 ClusterEpochAuthorityValue *out);

extern bool cluster_epoch_ballot_lane_is_valid(const ClusterEpochBallotLane *lane,
											   int32 expected_proposer_node_id,
											   uint32 expected_configured_disk_count,
											   uint64 expected_proposer_admitted_incarnation,
											   uint64 expected_system_identifier,
											   uint64 expected_grammar_fingerprint);
extern bool cluster_epoch_ballot_lane_encode(const ClusterEpochBallotLane *lane,
											 int32 expected_proposer_node_id,
											 uint32 expected_configured_disk_count,
											 uint64 expected_proposer_admitted_incarnation,
											 uint64 expected_system_identifier,
											 uint64 expected_grammar_fingerprint,
											 uint8 out[CLUSTER_EPOCH_BALLOT_LANE_BYTES]);
extern bool cluster_epoch_ballot_lane_decode(const uint8 bytes[CLUSTER_EPOCH_BALLOT_LANE_BYTES],
											 int32 expected_proposer_node_id,
											 uint32 expected_configured_disk_count,
											 uint64 expected_proposer_admitted_incarnation,
											 uint64 expected_system_identifier,
											 uint64 expected_grammar_fingerprint,
											 ClusterEpochBallotLane *out);

#endif /* CLUSTER_EPOCH_BALLOT_H */
