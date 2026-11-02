/* Original creator observations; not checkpoint coverage or publication rights.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_RECOVERY_BOOTSTRAP_H
#define CLUSTER_RECOVERY_BOOTSTRAP_H
#include "cluster/cluster_recovery_file_identity.h"
#include "cluster/cluster_recovery_format.h"

typedef struct ClusterRecoveryBootFileV2 {
	ClusterRecoveryFileIdentityV2 identity;
	ClusterRecoveryPageIdentityV2 page;
	/* Producer: synced length. Reader: observed length, not durability. */
	uint64 synced_bytes;
	uint8 image_digest[CLUSTER_RECOVERY_V2_DIGEST_BYTES];
} ClusterRecoveryBootFileV2;

/* F's original file producer. No WAL at BOOT; no registry/ROOT publication.
 * False/ERROR preserves original FD, partial files and original responsibility.
 * Outputs are installed only after complete sync and independent readback. */
extern bool BootStrapCLOGManagedV2(const ClusterRecoveryFileIdentityV2 *,
								   FullTransactionId actual_initial_next,
								   ClusterRecoveryBootFileV2 *out);
extern bool cluster_undo_bootstrap_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
									  ClusterRecoveryBootFileV2 *out);
/* Original empty-FD producer; custody remains with the creator. */
extern bool cluster_clog_bootstrap_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
									  FullTransactionId actual_initial_next,
									  ClusterRecoveryBootFileV2 *out);
/* Read-only whole-file observations for the original creator's final recheck.
 * Expected identity is independent; neither function opens/closes the FD. */
extern bool cluster_clog_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
										   ClusterRecoveryBootFileV2 *out);
extern bool cluster_undo_bootstrap_read_v2(int fd, const ClusterRecoveryFileIdentityV2 *,
										   ClusterRecoveryBootFileV2 *out);

static inline bool
cluster_recovery_boot_file_matches_v2(const ClusterRecoveryFileIdentityV2 *expected,
									  const ClusterRecoveryBootFileV2 *result)
{
	uint8 encoded[128], any = 0;
	if (!expected || !result || result->synced_bytes == 0 || result->page.logical_page != 0
		|| !cluster_recovery_file_identity_encode_v2(expected, encoded, sizeof(encoded))
		|| !cluster_recovery_file_identity_matches_v2(encoded, sizeof(encoded), &result->identity))
		return false;
	/* A CLOG BOOT page has the actual original epoch0/XID3 birth. The
	 * unused TT40 seed has no transaction birth/generation/LSN to certify. */
	if (expected->kind == CLUSTER_RECOVERY_FILE_CLOG_V2) {
		if (result->synced_bytes
				!= CLUSTER_RECOVERY_CLOG_SEGMENT_HEADER_V2_BYTES
					   + CLUSTER_RECOVERY_CLOG_PAGE_BYTES_V2
			|| result->page.page_generation != FirstNormalTransactionId
			|| !result->page.birth_observed
			|| !FullTransactionIdEquals(result->page.birth_full_xid, FirstNormalFullTransactionId))
			return false;
	} else if (expected->kind == CLUSTER_RECOVERY_FILE_UNDO_V2) {
		if (result->page.page_generation != 0 || result->page.birth_observed
			|| U64FromFullTransactionId(result->page.birth_full_xid) != 0
			|| result->page.page_lsn != InvalidXLogRecPtr || result->page.metadata_crc != 0)
			return false;
	} else
		return false;
	for (unsigned i = 0; i < sizeof(result->image_digest); ++i)
		any |= result->image_digest[i];
	return any != 0;
}
#endif
