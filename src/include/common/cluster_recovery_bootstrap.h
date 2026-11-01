/* Original creator observations; not checkpoint coverage or publication rights.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_RECOVERY_BOOTSTRAP_H
#define CLUSTER_RECOVERY_BOOTSTRAP_H
#include "cluster/cluster_recovery_file_identity.h"

typedef struct ClusterRecoveryBootFileV2 {
	ClusterRecoveryFileIdentityV2 identity;
	ClusterRecoveryPageIdentityV2 page;
	/* Producer: synchronized length. Read-only API: observed length only. */
	uint64 synced_bytes;
	uint8 image_digest[CLUSTER_RECOVERY_V2_DIGEST_BYTES];
} ClusterRecoveryBootFileV2;

/* F's original file producer. No WAL at BOOT; no registry/ROOT publication.
 * False/ERROR preserves original FD, partial files and original responsibility.
 * Outputs are installed only after complete sync and independent readback. */
extern bool BootStrapCLOGManagedV2(const ClusterRecoveryFileIdentityV2 *,
	FullTransactionId actual_initial_next, ClusterRecoveryBootFileV2 *out);
extern bool cluster_undo_bootstrap_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
	ClusterRecoveryBootFileV2 *out);

/* Original BOOT owner borrows a new, empty O_RDWR regular FD and excludes
 * other I/O/lifetime changes throughout the call. Never closes or unlinks it.
 * These functions do not acquire creator/control authority. */
extern bool cluster_clog_bootstrap_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
	FullTransactionId actual_initial_next, ClusterRecoveryBootFileV2 *out);

/* Read-only observations through the same original owner custody. CLOG0 may
 * contain later valid same-birth writes at the original epoch0/XID3 BOOT page;
 * its source watermark is not coverage.
 * UNDO0 is the original unused initdb seed (owner1), not a runtime segment.
 * Both inspect every byte, refuse partial/extra bytes, and leave out unchanged
 * on failure. Digests describe the observation, not durability of a later write. */
extern bool cluster_clog_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
	ClusterRecoveryBootFileV2 *out);
extern bool cluster_undo_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
	ClusterRecoveryBootFileV2 *out);

#endif
