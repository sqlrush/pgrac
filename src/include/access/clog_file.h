/* Versioned CLOG file framing. Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CLOG_FILE_H
#define CLUSTER_CLOG_FILE_H

#include "cluster/cluster_recovery_file_identity.h"

#define CLUSTER_CLOG_FILE_HEADER_BYTES 8192
#define CLUSTER_CLOG_PAGE_HEADER_BYTES 64
#define CLUSTER_CLOG_PAGE_BYTES 8192
#define CLUSTER_CLOG_FRAME_BYTES 8256
#define CLUSTER_CLOG_PAGES_PER_FILE 32
#define CLUSTER_CLOG_FILE_REFERENCE_BYTES 128
#define CLUSTER_TT_FILE_REFERENCE_OFFSET 3072

/* The original control owner's complete identity, encoded field by field.
 * This framing layer does not confer custody,
 * checkpoint coverage, transaction outcome, or permission to write. */
typedef struct ClusterClogFileBindingV2
{
	ClusterRecoveryFileIdentityV2 identity;
	uint32 segment;
} ClusterClogFileBindingV2;

typedef struct ClusterClogPageIdentityV2
{
	/* page_lsn is the page's source-local watermark, not complete checkpoint
	 * coverage. CRC fields are computed on encode and returned on decode. */
	ClusterRecoveryPageIdentityV2 identity;
	/* The full-image WAL LSN is separate from the covered watermark. */
	XLogRecPtr image_lsn;
} ClusterClogPageIdentityV2;

/* All lengths are exact; failure leaves every output byte unchanged.
 * Identity fields are supplied by the original control/file owner, never
 * reconstructed from a raw xid, current epoch, or the file being read. */
extern bool cluster_clog_file_offset_v2(uint32 local_page, uint64 *offset);
extern bool cluster_clog_file_header_encode_v2(const ClusterClogFileBindingV2 *binding,
											  void *out, Size length);
extern bool cluster_clog_file_header_matches_v2(const void *bytes, Size length,
											   const ClusterClogFileBindingV2 *expected);
extern bool cluster_clog_file_page_encode_v2(const ClusterClogFileBindingV2 *binding,
											const ClusterClogPageIdentityV2 *page,
											const void *payload, Size payload_length,
											void *out, Size length);
extern bool cluster_clog_file_page_decode_v2(const ClusterClogFileBindingV2 *binding,
											uint64 expected_page, const void *bytes, Size length,
											ClusterClogPageIdentityV2 *page_out, void *payload_out,
											Size payload_length);

/* Private complete TT40 images only. The caller owns the original block0
 * writer context and must reseal/publish the whole page with that owner.
 * Existing references are immutable: an exact retry is allowed; replacing
 * a different reference or interpreting TT32 is refused. */
extern bool cluster_tt_file_reference_prepare_v2(void *private_block0, Size length,
												const ClusterRecoveryFileIdentityV2 *identity);
extern bool cluster_tt_file_reference_matches_v2(const void *block0, Size length,
												const ClusterRecoveryFileIdentityV2 *identity);

StaticAssertDecl(CLUSTER_CLOG_FILE_HEADER_BYTES +
	CLUSTER_CLOG_PAGES_PER_FILE * CLUSTER_CLOG_FRAME_BYTES == 272384,
	"CLOG framed segment size must remain exact");
StaticAssertDecl(CLUSTER_TT_FILE_REFERENCE_OFFSET +
	CLUSTER_CLOG_FILE_REFERENCE_BYTES == 3200, "TT reference must end at 3200");
StaticAssertDecl(CLUSTER_RECOVERY_V2_UUID_BYTES == 16,
	"file reference encoding requires complete 16-byte UUIDs");

#endif
