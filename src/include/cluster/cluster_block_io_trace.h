/* Block IO diagnostics; observations never grant buffer authority. */
#ifndef CLUSTER_BLOCK_IO_TRACE_H
#define CLUSTER_BLOCK_IO_TRACE_H
#include "cluster/cluster_pcm_read_source.h"
extern void cluster_block_io_trace_write(BufferDesc *buf, const char *bytes);
extern void cluster_block_io_trace_source(const ClusterPcmReadSource *source, const char *phase);
extern void cluster_block_io_trace_pending_s(const ClusterPcmReadGrantKey *key,
	const char *event, int phase, int holder, uint64 binding, int result);
extern void cluster_block_io_trace_pi_retired(BufferTag tag, int buffer_id, uint64 generation,
											  uint64 token, uint64 version);
extern void cluster_block_io_trace_note(BufferTag tag, const char *event, uint64 version,
										uint64 sequence, uint32 holders);
extern void cluster_block_io_trace_failed(BufferTag tag, const char *bytes, const char *source,
										  uint32 expected, uint32 calculated);
#endif
