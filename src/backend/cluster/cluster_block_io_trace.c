/*-------------------------------------------------------------------------
 * cluster_block_io_trace.c -- opt-in physical IO/source receipts.
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#include "postgres.h"
#include <fcntl.h>
#include <unistd.h>
#include "cluster/cluster_block_io_trace.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_pcm_own.h"
#include "cluster/cluster_pcm_lock.h"
#include "cluster/cluster_startup_phase.h"
#include "cluster/cluster_gcs.h"
#include "cluster/cluster_grd.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_membership.h"
#include "cluster/cluster_qvotec.h"
#include "miscadmin.h"
#include "utils/timestamp.h"

#ifdef USE_PGRAC_CLUSTER
/* Conditions are observations at this cut, not a new service admission.
 * result records the original branch; the claim identity has a locked native
 * lifecycle. A missing snapshot stays unknown, never an invented blocker. */
void
cluster_block_io_trace_master(const ClusterPcmReadGrantKey *key, const char *event, int result)
{
	ClusterPcmReadGrant blocker = {0};
	bool known, pending = false, serving, member, join_active, rebuilt, recovering;
	int route;
	int saved_errno = errno;
	if (!cluster_trace_block_io || key == NULL)
		return;
	known = cluster_pcm_lock_read_grant_blocker_snapshot(key->tag, key->requester_node, &blocker);
	serving = cluster_authority_readiness_managed()
		? cluster_serving_ready_check(&pending, NULL) : cluster_qvotec_in_quorum();
	member = cluster_membership_is_member(cluster_node_id);
	join_active = cluster_grd_join_remaster_active_for_shard(key->tag);
	rebuilt = cluster_grd_block_view_rebuilt(key->tag);
	recovering = cluster_gcs_block_phase_for_tag(key->tag) == GCS_BLOCK_RECOVERING;
	route = cluster_gcs_lookup_master(key->tag);
	write_stderr("BLOCK_IO event=FIRST_FILL_MASTER action=%s time=" INT64_FORMAT
		" node=%d pid=%d tag=%u/%u/%u/%d/%u request=" UINT64_FORMAT
		" epoch=" UINT64_FORMAT " requester=%d backend=%d decision=%d"
		" blocker_known=%d blocker_request=" UINT64_FORMAT " blocker_epoch=" UINT64_FORMAT
		" blocker_node=%d blocker_backend=%d blocker_binding=" UINT64_FORMAT
		" blocker_revoking=%d blocker_ack_consumed=%d conditions_sampled=1"
		" serving=%d serving_pending=%d member=%d join_active=%d rebuilt=%d recovering=%d route=%d\n",
		event, GetCurrentTimestamp(), cluster_node_id, MyProcPid, key->tag.spcOid, key->tag.dbOid,
		key->tag.relNumber, key->tag.forkNum, key->tag.blockNum, key->request_id, key->epoch,
		key->requester_node, key->backend_id, result, known, blocker.key.request_id,
		blocker.key.epoch, known ? blocker.key.requester_node : -1,
		known ? blocker.key.backend_id : -1, blocker.binding_generation,
		blocker.revoking, blocker.ack_consumed, serving, pending, member, join_active,
		rebuilt, recovering, route);
	errno = saved_errno;
}

void
cluster_block_io_trace_receipt(const ClusterPcmReadSource *source, const char *event,
	int backend, int buffer, int owner_pid, uint64 incarnation, unsigned count)
{
	int saved_errno = errno;
	if (cluster_trace_block_io && source != NULL)
		write_stderr("BLOCK_IO event=QUIET_RECEIPT action=%s time=" INT64_FORMAT
			" node=%d pid=%d backend=%d owner_pid=%d incarnation=" UINT64_FORMAT
			" tag=%u/%u/%u/%d/%u request=" UINT64_FORMAT " epoch=" UINT64_FORMAT
			" master=%d buffer=%d generation=" UINT64_FORMAT " token=" UINT64_FORMAT
			" backend_receipts=%u\n", event, GetCurrentTimestamp(), cluster_node_id,
			MyProcPid, backend, owner_pid, incarnation, source->tag.spcOid, source->tag.dbOid,
			source->tag.relNumber, source->tag.forkNum, source->tag.blockNum,
			source->request_id, source->request_epoch, source->master_node, buffer,
			source->base_generation, source->reservation_token, count);
	errno = saved_errno;
}

void
cluster_block_io_trace_pending_s(const ClusterPcmReadGrantKey *key, const char *event,
	int phase, int holder, uint64 binding, int result)
{
	int saved_errno = errno;
	if (cluster_trace_block_io && key != NULL)
		write_stderr("BLOCK_IO event=PENDING_S action=%s time=" INT64_FORMAT
			" node=%d pid=%d tag=%u/%u/%u/%d/%u request=" UINT64_FORMAT
			" epoch=" UINT64_FORMAT " requester=%d backend=%d phase=%d holder=%d"
			" binding=" UINT64_FORMAT " result=%d\n", event, GetCurrentTimestamp(),
			cluster_node_id, MyProcPid, key->tag.spcOid, key->tag.dbOid, key->tag.relNumber,
			key->tag.forkNum, key->tag.blockNum, key->request_id, key->epoch,
			key->requester_node, key->backend_id, phase, holder, binding, result);
	errno = saved_errno;
}

void
cluster_block_io_trace_source(const ClusterPcmReadSource *source, const char *phase)
{
	int saved_errno = errno;
	if (cluster_trace_block_io && source != NULL)
		write_stderr("BLOCK_IO event=READ_SOURCE phase=%s time=" INT64_FORMAT
					 " node=%d pid=%d tag=%u/%u/%u/%d/%u source=%d master=%d"
					 " generation=" UINT64_FORMAT " token=" UINT64_FORMAT " request=" UINT64_FORMAT
					 " epoch=" UINT64_FORMAT " watermark=" UINT64_FORMAT " durable=%d quiet=%d\n",
					 phase, GetCurrentTimestamp(), cluster_node_id, MyProcPid, source->tag.spcOid,
					 source->tag.dbOid, source->tag.relNumber, source->tag.forkNum,
					 source->tag.blockNum, source->kind, source->master_node,
					 source->base_generation, source->reservation_token, source->request_id,
					 source->request_epoch, source->watermark, source->durable, source->quiescent);
	errno = saved_errno;
}

void
cluster_block_io_trace_write(BufferDesc *buf, const char *bytes)
{
	BufferTag tag;
	uint32 state;
	uint64 generation, token;
	uint8 pcm, type;
	int saved_errno = errno;
	if (!cluster_trace_block_io)
		return;
	state = LockBufHdr(buf);
	tag = buf->tag;
	generation = cluster_pcm_own_gen_get(buf->buf_id);
	token = cluster_pcm_own_reservation_token_get(buf->buf_id);
	pcm = buf->pcm_state;
	type = buf->buffer_type;
	UnlockBufHdr(buf, state);
	write_stderr("BLOCK_IO event=WRITE_COMPLETE time=" INT64_FORMAT
				 " node=%d pid=%d tag=%u/%u/%u/%d/%u buffer=%d state=%u pcm=%u type=%u"
				 " generation=" UINT64_FORMAT " token=" UINT64_FORMAT " version=" UINT64_FORMAT
				 " lsn=" UINT64_FORMAT " checksum=%u durable=0\n",
				 GetCurrentTimestamp(), cluster_node_id, MyProcPid, tag.spcOid, tag.dbOid,
				 tag.relNumber, tag.forkNum, tag.blockNum, buf->buf_id, state, pcm, type,
				 generation, token, ((PageHeader)bytes)->pd_block_scn,
				 (uint64)PageGetLSN((Page)bytes), ((PageHeader)bytes)->pd_checksum);
	errno = saved_errno;
}

void
cluster_block_io_trace_pi_retired(BufferTag tag, int buffer_id, uint64 generation, uint64 token,
								  uint64 version)
{
	int saved_errno = errno;
	if (cluster_trace_block_io)
		write_stderr("BLOCK_IO event=PI_RETIRED time=" INT64_FORMAT " node=%d pid=%d"
					 " tag=%u/%u/%u/%d/%u buffer=%d generation=" UINT64_FORMAT
					 " token=" UINT64_FORMAT " version=" UINT64_FORMAT "\n",
					 GetCurrentTimestamp(), cluster_node_id, MyProcPid, tag.spcOid, tag.dbOid,
					 tag.relNumber, tag.forkNum, tag.blockNum, buffer_id, generation, token,
					 version);
	errno = saved_errno;
}

void
cluster_block_io_trace_note(BufferTag tag, const char *event, uint64 version, uint64 sequence,
							uint32 holders)
{
	int saved_errno = errno;
	if (cluster_trace_block_io)
		write_stderr(
			"BLOCK_IO event=%s time=" INT64_FORMAT " node=%d pid=%d"
			" tag=%u/%u/%u/%d/%u version=" UINT64_FORMAT " sequence=" UINT64_FORMAT " holders=%u\n",
			event, GetCurrentTimestamp(), cluster_node_id, MyProcPid, tag.spcOid, tag.dbOid,
			tag.relNumber, tag.forkNum, tag.blockNum, version, sequence, holders);
	errno = saved_errno;
}

/* Best effort only: a failed diagnostic must preserve the original failure.
 * Exclusive create never overwrites a previous capture, including PID reuse. */
void
cluster_block_io_trace_failed(BufferTag tag, const char *bytes, const char *source, uint32 expected,
							  uint32 calculated)
{
	static uint64 capture_sequence;
	char path[MAXPGPATH];
	int fd, length, saved_errno = errno;
	size_t written = 0;
	TimestampTz now;
	int capture_errno = 0;

	if (!cluster_trace_block_io || bytes == NULL || DataDir == NULL)
		return;
	now = GetCurrentTimestamp();
	length = snprintf(path, sizeof(path),
					  "%s/pg_stat_tmp/pcm-failed-%d-" INT64_FORMAT "-" UINT64_FORMAT ".bin",
					  DataDir, MyProcPid, now, ++capture_sequence);
	fd = length > 0 && length < sizeof(path)
			 ? open(path, O_WRONLY | O_CREAT | O_EXCL | PG_BINARY, S_IRUSR | S_IWUSR)
			 : -1;
	if (fd < 0)
		capture_errno = length >= sizeof(path) ? ENAMETOOLONG : errno;
	else {
		while (written < BLCKSZ) {
			ssize_t n = write(fd, bytes + written, BLCKSZ - written);
			if (n < 0 && errno == EINTR)
				continue;
			if (n <= 0) {
				capture_errno = n < 0 ? errno : EIO;
				break;
			}
			written += n;
		}
		if (close(fd) != 0 && capture_errno == 0)
			capture_errno = errno;
	}
	write_stderr("BLOCK_IO event=VERIFY_FAILED time=" INT64_FORMAT " node=%d pid=%d"
				 " tag=%u/%u/%u/%d/%u source=%s expected=%u calculated=%u capture=%s"
				 " bytes=%zu capture_errno=%d\n",
				 now, cluster_node_id, MyProcPid, tag.spcOid, tag.dbOid, tag.relNumber, tag.forkNum,
				 tag.blockNum, source, expected, calculated,
				 length < sizeof(path) ? path : "PATH_TOO_LONG", written, capture_errno);
	errno = saved_errno;
}
#endif
