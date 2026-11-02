/* Borrowed-FD CLOG observations. Author: SqlRush <sqlrush@gmail.com> */
#ifndef CLUSTER_CLOG_STORAGE_H
#define CLUSTER_CLOG_STORAGE_H

#include "access/clog_file.h"

/* The original owner supplies the expected registration and holds the FD,
 * page I/O serialization and file lifetime throughout this read. It must
 * recheck its control/source custody before publishing an observation.
 * This helper never opens, closes, creates, repairs, writes or syncs a file.
 * Missing/legacy/partial target frames or mismatched input are refused, even
 * during recovery. Other frames may be extending under their own I/O locks.
 * Success returns bytes and page identity, not checkpoint or replay authority.
 * Failure preserves both outputs. Input and output ranges must not overlap.
 */
extern bool cluster_clog_storage_read_page_v2(int fd,
									 const ClusterClogFileBindingV2 *expected,
									 uint64 logical_page,
									 ClusterClogPageIdentityV2 *page_out,
									 void *payload_out, Size payload_length);

/* Readonly whole-header/FD/target-slot probe for an existing selected segment.
 * Partial or nonzero targets refuse. Success supplies neither page birth nor
 * authority; the original owner must exclude concurrent target I/O. */
extern bool cluster_clog_storage_page_unwritten_v2(int fd,
									 const ClusterClogFileBindingV2 *expected,
									 uint64 logical_page);

/* Complete byte observation for the original checkpoint/file owner. Extent
 * slots include zero sparse frames; only materialized_pages identifies born
 * pages. LSN maxima belong solely to this file's original source, and are not
 * a source interval, checkpoint coverage or a transaction bound. */
typedef struct ClusterClogFileSyncV2
{
	ClusterClogFileBindingV2 binding;
	uint64 synced_bytes;
	uint32 extent_pages;
	uint32 materialized_pages;
	XLogRecPtr max_page_lsn;
	XLogRecPtr max_image_lsn;
	uint8 image_digest[CLUSTER_RECOVERY_V2_DIGEST_BYTES];
} ClusterClogFileSyncV2;

/* Borrow both original FDs and independently selected expected identity.
 * The caller continuously serializes ALL I/O/extension to this file, retains
 * its original table/file/source/cut custody, and binds directory_fd to the
 * selected namespace. After return it must recheck that same custody before
 * publishing a receipt. This helper does not establish any of those rights.
 *
 * Validate/hash the entire complete framed extent, fsync file then directory,
 * and read/validate/hash every byte again. Both observations and held inode/
 * extent/header must agree. Fully zero sparse slots supply no page birth;
 * partial/unknown/corrupt frames, an empty segment, disabled fsync and changes
 * refuse. No open/close/write/repair/WAL/truncate or coverage publication.
 * Refusal/ERROR retains outputs, both FDs and all original owner obligations.
 * Input/output ranges must be distinct. */
extern bool cluster_clog_storage_sync_file_v2(int fd, int directory_fd,
									const ClusterClogFileBindingV2 *expected,
									ClusterClogFileSyncV2 *out);

/* Write a local frame. The original owner
 * holds file lifetime and page I/O serialization throughout, and supplies one
 * private control-lock snapshot of page identity, bytes and group-LSN maximum
 * (page_lsn). It rechecks original control/source custody before publication.
 *
 * Every call logs a complete frame through native WAL, flushes its actual end,
 * then writes, syncs and reads back the complete frame. This conservatively
 * protects every overwrite without a slot-based or cached redo-point shortcut.
 * WAL contains the captured frame; replay must stamp the actual record end as
 * image_lsn, as this writer does after insertion. Existing valid frames require
 * the same observed birth and captured image_lsn, with no source-watermark
 * regression. A fresh original allocator birth with captured image_lsn=0 may
 * fill a wholly absent or complete zero sparse slot in an existing, fully
 * validated segment. Empty bytes do not supply birth or control authority.
 * Before WAL and again after flush the header/FD/entire target slot must agree;
 * partial/nonzero/corrupt targets are never repaired or replaced as new pages.
 * Other pages may extend under their own I/O serialization. Header creation,
 * foreign/recovery write, fsync-disabled or bootstrap phony WAL is refused here.
 * An authenticated creation child uses its original batch's complete matching
 * file identity for the local WAL origin, retaining its disabled node identity.
 *
 * The borrowed FD, inputs, preowned checkpoint-delay flag and last_end output
 * are retained on failure. Partial writes/material are never rolled back or
 * repaired; the original owner retains its dirty/error/recovery responsibility.
 * Native WAL errors propagate after restoring only our checkpoint-delay flag.
 * This is not file registration, custody, checkpoint or replay authority.
 */
extern bool cluster_clog_storage_write_page_v2(int fd,
									 const ClusterClogFileBindingV2 *expected,
									 const ClusterClogPageIdentityV2 *page,
									 const void *payload, Size payload_length,
									 XLogRecPtr *last_end);

/* Initialize an empty, exclusively owned local segment FD with its header and
 * first materialized page. The original file owner has already reserved the
 * exact name/registration and excludes every other writer for this call. It
 * supplies the same private control-lock snapshot as write_page, with no prior
 * image_lsn. The first materialized page need not be local page zero; gaps are
 * not valid pages and readers must continue to refuse them.
 *
 * Native header and complete-page WAL records are flushed before either file
 * write. Success requires fsync and complete readback, and returns the page
 * record's actual end. Existing/partial/legacy files are never overwritten or
 * repaired. Failures preserve last_end, inputs, borrowed FD and any partial
 * material; WAL errors restore only our checkpoint-delay flag and propagate.
 *
 * This does not open/publish a name, sync its directory, register a file, or
 * provide bootstrap/replay authority. The original owner retains namespace
 * durability and custody. Recovery/bootstrap/fsync-disabled writes are refused.
 */
extern bool cluster_clog_storage_initialize_v2(int fd,
									 const ClusterClogFileBindingV2 *expected,
									 const ClusterClogPageIdentityV2 *page,
									 const void *payload, Size payload_length,
									 XLogRecPtr *last_end);

/* Replay a selected native segment-header (0x40) or full-page (0x50) record
 * through an original Startup recovery owner's borrowed read/write FD. The
 * owner continuously holds exact control registration, legal retained source/
 * cut, file lifetime, page I/O serialization and old-writer exclusion. Neither
 * a matching record nor InRecovery supplies those rights. The owner must know
 * independently that this is its registered managed file, including when its
 * header is torn. No open/create, directory sync, WAL insertion, truncate or
 * ordinary recovery authority is added here.
 *
 * The actual record end is required. Complete existing valid disk effects are
 * preserved. Optional current_frame is the original owner's complete private
 * same-birth replay state, including all effects not yet on disk; it must cover
 * every valid disk effect. NULL/0 asserts no extra unflushed replay state, not
 * permission to discard valid disk terminal bits. Conflicts refuse before
 * mutation. A damaged header needs its own selected segment record first.
 *
 * Success follows whole-header/frame write, fsync and complete same-FD
 * readback, then stores the consumed record end (not checkpoint coverage).
 * Refusal/ERROR retains inputs/output, borrowed FD and partial material; the
 * original recovery owner retains retry/error responsibility. All native,
 * SIDE/cold and lifecycle callers must be composed before format activation. */
extern bool cluster_clog_storage_replay_v2(int fd,
									const ClusterClogFileBindingV2 *expected,
									uint8 info, const void *record, Size record_length,
									XLogRecPtr record_end, const void *current_frame,
									Size current_length, XLogRecPtr *applied_end);

#endif
