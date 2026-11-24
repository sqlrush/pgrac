/*-------------------------------------------------------------------------
 *
 * test_pgrac_verdict_bench.c
 *	  Micro-benchmark fixture for the origin-side own-xid verdict resolver.
 *
 *	  Times the real product entry points in a live backend:
 *	    - the complete durable TT header scan
 *	      (cluster_tt_slot_durable_resolve_by_xid), and
 *	    - the shared verdict core behind the LMS serve and the self leg
 *	      (cluster_lms_undo_verdict_fill_page).
 *	  It also provisions undo segment files through the product's own
 *	  creation code and can drop their header pages from the OS page cache
 *	  to compare warm against cold reads.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/test/modules/test_pgrac_verdict_bench/test_pgrac_verdict_bench.c
 *
 * NOTES
 *	  This is a pgrac-original file (no derivation from PostgreSQL).
 *
 *	  Opt-in measurement module.  It is not listed in src/test/modules and
 *	  is never built or loaded by "make check".  Every function is
 *	  superuser-only.  No product code path is modified: the timed calls
 *	  are the exported product functions themselves, and the reported
 *	  pread/open/close counts are the product's existing smgr counters.
 *
 *	  The "clog_leg" mode is NOT a product function.  It repeats, with the
 *	  same exported primitives, the fenced literal CLOG read the verdict
 *	  core performs, so that leg can be timed without the TT scan.  It does
 *	  not reproduce the core's ordering.  Treat it as a decomposition aid
 *	  only.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <unistd.h>
#ifdef __APPLE__
#include <dlfcn.h>
#include <libproc.h>
#endif

#include "access/clog.h"
#include "access/transam.h"
#include "catalog/pg_type.h"
#include "fmgr.h"
#include "lib/stringinfo.h"
#include "miscadmin.h"
#include "portability/instr_time.h"
#include "storage/fd.h"
#include "storage/lwlock.h"
#include "utils/array.h"
#include "utils/builtins.h"

#ifdef USE_PGRAC_CLUSTER
#include "cluster/cluster_cr.h"
#include "cluster/cluster_cr_server.h"
#include "cluster/cluster_gcs_block.h"
#include "cluster/cluster_guc.h"
#include "cluster/cluster_tt_durable.h"
#include "cluster/cluster_undo_record_api.h"
#include "cluster/cluster_undo_segment_init.h"
#include "cluster/cluster_undo_smgr.h"
#include "cluster/cluster_xid_stripe.h"
#include "cluster/storage/cluster_undo_alloc.h"
#endif

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(test_pgrac_vb_provision);
PG_FUNCTION_INFO_V1(test_pgrac_vb_segments);
PG_FUNCTION_INFO_V1(test_pgrac_vb_evict);
PG_FUNCTION_INFO_V1(test_pgrac_vb_run);

/* Upper bound on one run's sample array (8 bytes per iteration). */
#define VB_MAX_ITERATIONS 5000000

/* Width of the optional interposer's snapshot; must match vb_interpose.c. */
#define VB_INTERPOSE_SLOTS 16

/* Result codes are small enums; this covers every mode's domain. */
#define VB_RESULT_CODES 8

typedef enum VbMode {
	VB_MODE_SCAN = 0,	   /* complete durable TT header scan only */
	VB_MODE_SCAN_LOCK_CONTROL, /* scan under a known shared LWLock, not a latency sample */
	VB_MODE_LEAF_ORDINARY, /* verdict core, ordinary single-xid consumer */
	VB_MODE_LEAF_LEGACY,   /* verdict core, authoritative legacy consumer */
	VB_MODE_CLOG_LEG	   /* decomposition aid; see file header */
} VbMode;

/* Process totals sampled around one run. */
typedef struct VbCounters {
	uint64 smgr_pread;
	uint64 smgr_open;
	uint64 smgr_close;
	uint64 by_xid_scan;
	uint64 disk_bytes_read; /* physical; UINT64_MAX when unavailable */
	long ru_inblock;
	long ru_majflt;
	long ru_nvcsw;
	long ru_nivcsw;
	bool interposed;	  /* optional libc-level counters present */
	uint64 interpose[VB_INTERPOSE_SLOTS]; /* see vb_interpose.c for the slots */
} VbCounters;

/*
 * vb_check_caller -- restrict the fixture to superusers on a cluster node.
 * Inputs: current role and GUC face.  Returns: void.
 * Side effects: ERROR on denial or on a non-cluster build/instance.
 */
static void
vb_check_caller(void)
{
	if (!superuser())
		ereport(ERROR, (errcode(ERRCODE_INSUFFICIENT_PRIVILEGE),
						errmsg("verdict benchmark requires superuser"),
						errhint("This function is for measurement fixtures only.")));
#ifdef USE_PGRAC_CLUSTER
	if (!cluster_enabled || cluster_node_id < 0)
		ereport(ERROR, (errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						errmsg("verdict benchmark requires cluster.enabled and a node id")));
#else
	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("verdict benchmark requires a cluster build")));
#endif
}

#ifdef USE_PGRAC_CLUSTER

/*
 * vb_own_segment_range -- this node's undo segment id range.
 * Inputs: none.  Outputs: owner instance, first id.  Returns: void.
 */
static void
vb_own_segment_range(uint8 *owner, uint32 *first)
{
	*owner = (uint8)(cluster_node_id + 1);
	*first = (uint32)cluster_node_id * CLUSTER_UNDO_SEGS_PER_INSTANCE + 1;
}

/*
 * vb_sample_counters -- copy the product counters and process I/O totals.
 * Inputs: none.  Outputs: *out.  Returns: void.  Side effects: none.
 */
static void
vb_sample_counters(VbCounters *out)
{
	struct rusage usage;

	memset(out, 0, sizeof(*out));
	out->smgr_pread = cluster_undo_smgr_pread_count();
	out->smgr_open = cluster_undo_smgr_open_count();
	out->smgr_close = cluster_undo_smgr_close_count();
	out->by_xid_scan = cluster_tt_durable_by_xid_scan_count();
	out->disk_bytes_read = UINT64_MAX;
	if (getrusage(RUSAGE_SELF, &usage) == 0) {
		out->ru_inblock = usage.ru_inblock;
		out->ru_majflt = usage.ru_majflt;
		out->ru_nvcsw = usage.ru_nvcsw;
		out->ru_nivcsw = usage.ru_nivcsw;
	}
#ifdef __APPLE__
	{
		struct rusage_info_v2 info;
		void (*snapshot)(uint64 *);

		if (proc_pid_rusage(getpid(), RUSAGE_INFO_V2, (rusage_info_t *)&info) == 0)
			out->disk_bytes_read = info.ri_diskio_bytesread;
		/* Present only when the optional interposer was preloaded. */
		snapshot = (void (*)(uint64 *))dlsym(RTLD_DEFAULT, "vb_interpose_snapshot");
		if (snapshot != NULL) {
			snapshot(out->interpose);
			out->interposed = true;
		}
	}
#endif
#ifdef __linux__
	{
		FILE *io = AllocateFile("/proc/self/io", "r");

		if (io != NULL) {
			char line[128];

			while (fgets(line, sizeof(line), io) != NULL) {
				unsigned long long value;

				if (sscanf(line, "read_bytes: %llu", &value) == 1) {
					out->disk_bytes_read = (uint64)value;
					break;
				}
			}
			FreeFile(io);
		}
	}
#endif
}

/*
 * vb_mapping_resident -- is any page of an untouched file mapping cached?
 * Inputs: mapping, its length, the VM page size.  Returns: bool.
 */
static bool
vb_mapping_resident(void *mapped, size_t map_bytes, size_t page_size)
{
	unsigned char residency[8];
	size_t pages = (map_bytes + page_size - 1) / page_size;
	size_t i;

	memset(residency, 0, sizeof(residency));
	if (pages > lengthof(residency) || mincore(mapped, map_bytes, (void *)residency) != 0)
		return true; /* unknown counts as still cached */
	for (i = 0; i < pages; i++)
		if ((residency[i] & 1) != 0)
			return true;
	return false;
}

static int
vb_compare_uint64(const void *a, const void *b)
{
	uint64 x = *(const uint64 *)a;
	uint64 y = *(const uint64 *)b;

	return x < y ? -1 : (x > y ? 1 : 0);
}

/* Nearest-rank percentile over an ascending sample array. */
static uint64
vb_percentile(const uint64 *sorted, int count, double fraction)
{
	int rank = (int)(fraction * (double)count + 0.999999);

	if (rank < 1)
		rank = 1;
	if (rank > count)
		rank = count;
	return sorted[rank - 1];
}

static VbMode
vb_parse_mode(const char *mode)
{
	if (strcmp(mode, "scan") == 0)
		return VB_MODE_SCAN;
	if (strcmp(mode, "scan_lock_control") == 0)
		return VB_MODE_SCAN_LOCK_CONTROL;
	if (strcmp(mode, "leaf_ordinary") == 0)
		return VB_MODE_LEAF_ORDINARY;
	if (strcmp(mode, "leaf_legacy") == 0)
		return VB_MODE_LEAF_LEGACY;
	if (strcmp(mode, "clog_leg") == 0)
		return VB_MODE_CLOG_LEG;
	ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
					errmsg("unknown verdict benchmark mode \"%s\"", mode),
					errhint("Use scan, scan_lock_control, leaf_ordinary, leaf_legacy or clog_leg.")));
	return VB_MODE_SCAN; /* keep compiler quiet */
}

/*
 * vb_one_call -- execute one timed unit and return its small result code.
 *
 *	scan            ClusterTTDurableResolve
 *	leaf_*          ClusterGcsUndoVerdictKind, 0 when the core refused
 *	clog_leg        raw CLOG status + 1, 0 when not sampled
 */
static int
vb_one_call(VbMode mode, TransactionId xid)
{
	switch (mode) {
	case VB_MODE_SCAN_LOCK_CONTROL: {
		int result;

		/* Positive control for the interposer, outside all reported hot timings. */
		LWLockAcquire(XactTruncationLock, LW_SHARED);
		PG_TRY();
		{
			result = vb_one_call(VB_MODE_SCAN, xid);
		}
		PG_CATCH();
		{
			/* ERROR resets interrupt holdoff; use the original bulk cleanup. */
			HOLD_INTERRUPTS();
			LWLockReleaseAll();
			RESUME_INTERRUPTS();
			PG_RE_THROW();
		}
		PG_END_TRY();
		LWLockRelease(XactTruncationLock);
		return result;
	}
	case VB_MODE_SCAN: {
		SCN commit_scn = InvalidScn;
		uint16 segment = 0;
		uint16 slot = 0;
		uint16 wrap = 0;

		return (int)cluster_tt_slot_durable_resolve_by_xid(xid, CLUSTER_TT_WRAP_ANY, &commit_scn,
														   &segment, &slot, &wrap);
	}
	case VB_MODE_LEAF_ORDINARY:
	case VB_MODE_LEAF_LEGACY: {
		ClusterGcsUndoVerdictPage page;

		/* The callee's contract: the caller zeroes the page first. */
		memset(&page, 0, sizeof(page));
		if (!cluster_lms_undo_verdict_fill_page(xid, mode == VB_MODE_LEAF_LEGACY,
												mode == VB_MODE_LEAF_ORDINARY, &page))
			return 0;
		return (int)page.verdict;
	}
	case VB_MODE_CLOG_LEG: {
		int result = 0;

		cluster_cr_native_prehistory_reader_lock();
		if (!cluster_cr_native_prehistory_disabled() && cluster_xid_is_mine(xid)) {
			LWLockAcquire(XactTruncationLock, LW_SHARED);
			if (!TransactionIdPrecedes(xid, ShmemVariableCache->oldestClogXid)) {
				XLogRecPtr lsn = InvalidXLogRecPtr;

				result = (int)TransactionIdGetStatus(xid, &lsn) + 1;
			}
			LWLockRelease(XactTruncationLock);
		}
		cluster_cr_native_prehistory_reader_unlock();
		return result;
	}
	}
	return 0;
}

#endif /* USE_PGRAC_CLUSTER */

/*
 * test_pgrac_vb_segments -- count this node's existing undo segment files.
 * Inputs: none.  Returns: int4.  Side effects: 256 existence probes.
 */
Datum
test_pgrac_vb_segments(PG_FUNCTION_ARGS)
{
	int32 present = 0;

	vb_check_caller();
#ifdef USE_PGRAC_CLUSTER
	{
		uint8 owner;
		uint32 first;
		uint32 i;

		vb_own_segment_range(&owner, &first);
		for (i = 0; i < CLUSTER_UNDO_SEGS_PER_INSTANCE; i++)
			if (cluster_undo_segment_file_exists(owner, first + i))
				present++;
	}
#endif
	PG_RETURN_INT32(present);
}

/*
 * test_pgrac_vb_provision -- grow this node's undo pool to nsegments files.
 *
 *	method "allocate": the allocator's own first publication
 *	(cluster_undo_segment_allocate), exactly what autoextend calls.
 *	method "publish": the same file publication code underneath it
 *	(product header template + temp create + sealed no-replace publish),
 *	for instances where the allocator's authority admission is unavailable.
 *	Either way the bytes on disk are written by product code.
 *
 * Inputs: target pool size, method.  Returns: JSON text.
 * Side effects: creates segment files; "allocate" also writes WAL.
 */
Datum
test_pgrac_vb_provision(PG_FUNCTION_ARGS)
{
	int32 target = PG_GETARG_INT32(0);
	char *method = text_to_cstring(PG_GETARG_TEXT_PP(1));
	int32 existing = 0;
	int32 created = 0;

	vb_check_caller();
#ifdef USE_PGRAC_CLUSTER
	{
		bool use_allocator = strcmp(method, "allocate") == 0;
		uint8 owner;
		uint32 first;
		int32 i;

		if (!use_allocator && strcmp(method, "publish") != 0)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("unknown provision method \"%s\"", method),
							errhint("Use allocate or publish.")));
		if (target < 1 || target > (int32)CLUSTER_UNDO_SEGS_PER_INSTANCE)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("segment count %d out of range [1, %u]", target,
								   CLUSTER_UNDO_SEGS_PER_INSTANCE)));

		vb_own_segment_range(&owner, &first);
		for (i = 0; i < target; i++) {
			uint32 segment_id = first + (uint32)i;

			if (cluster_undo_segment_file_exists(owner, segment_id)) {
				existing++;
				continue;
			}
			if (use_allocator)
				cluster_undo_segment_allocate(segment_id, owner);
			else {
				ClusterUndoPathIntent intent = cluster_undo_intent_for_owner(owner);
				ClusterUndoSmgrPublishResult published;
				PGAlignedBlock page;
				char temp_path[MAXPGPATH];

				cluster_undo_segment_make_header_bytes(segment_id, owner, page.data);
				if (!cluster_undo_smgr_provision_temp_create(intent, segment_id, owner, temp_path))
					ereport(ERROR,
							(errcode_for_file_access(),
							 errmsg("could not create undo segment %u staging file: %m",
									segment_id)));
				published = cluster_undo_smgr_provision_temp_publish(intent, segment_id, owner,
																	 temp_path, page.data);
				if (published != CLUSTER_UNDO_SMGR_PUBLISH_PUBLISHED
					&& published != CLUSTER_UNDO_SMGR_PUBLISH_EXISTS)
					ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
									errmsg("could not publish undo segment %u: result %d",
										   segment_id, (int)published)));
			}
			if (!cluster_undo_segment_file_exists(owner, segment_id))
				ereport(ERROR, (errcode(ERRCODE_IO_ERROR),
								errmsg("undo segment %u missing after provisioning", segment_id)));
			created++;
			CHECK_FOR_INTERRUPTS();
		}
	}
#endif
	PG_RETURN_TEXT_P(
		cstring_to_text(psprintf("{\"target\":%d,\"existing\":%d,\"created\":%d,\"method\":\"%s\"}",
								 target, existing, created, method)));
}

/*
 * test_pgrac_vb_evict -- drop each own segment header from the OS page cache.
 *
 *	Maps the first VM page of every existing own segment, asks the kernel to
 *	invalidate its cached copy, and reports residency before and after so the
 *	caller can tell whether the eviction took.  File metadata stays cached.
 *
 * Inputs: none.  Returns: JSON text.  Side effects: page cache only.
 */
Datum
test_pgrac_vb_evict(PG_FUNCTION_ARGS)
{
	int32 files = 0;
	int32 resident_before = 0;
	int32 resident_after = 0;
	int32 failed = 0;

	vb_check_caller();
#ifdef USE_PGRAC_CLUSTER
	{
		long page_size = sysconf(_SC_PAGESIZE);
		size_t map_bytes = (size_t)Max(page_size, (long)BLCKSZ);
		uint8 owner;
		uint32 first;
		uint32 i;

		vb_own_segment_range(&owner, &first);
		for (i = 0; i < CLUSTER_UNDO_SEGS_PER_INSTANCE; i++) {
			char path[MAXPGPATH];
			void *mapped;
			int fd;

			if (!cluster_undo_segment_file_exists(owner, first + i)
				|| cluster_undo_path_resolve(cluster_undo_intent_for_owner(owner), owner, first + i,
											 path, sizeof(path))
					   != 0)
				continue;
			fd = BasicOpenFile(path, O_RDONLY | PG_BINARY);
			if (fd < 0) {
				failed++;
				continue;
			}
			files++;
			mapped = mmap(NULL, map_bytes, PROT_READ, MAP_SHARED, fd, 0);
			if (mapped == MAP_FAILED) {
				failed++;
				close(fd);
				continue;
			}
			if (vb_mapping_resident(mapped, map_bytes, (size_t)page_size))
				resident_before++;
			if (msync(mapped, map_bytes, MS_INVALIDATE) != 0)
				failed++;
#ifdef __linux__
			/* Linux keeps clean pages across MS_INVALIDATE; ask for the drop. */
			if (posix_fadvise(fd, 0, (off_t)map_bytes, POSIX_FADV_DONTNEED) != 0)
				failed++;
#endif
			if (vb_mapping_resident(mapped, map_bytes, (size_t)page_size))
				resident_after++;
			munmap(mapped, map_bytes);
			close(fd);
		}
	}
#endif
	PG_RETURN_TEXT_P(cstring_to_text(
		psprintf("{\"files\":%d,\"resident_before\":%d,\"resident_after\":%d,\"failed\":%d}", files,
				 resident_before, resident_after, failed)));
}

/*
 * test_pgrac_vb_run -- time `iterations` serial calls of one mode.
 *
 *	xids are visited round-robin.  With latch_stripe the backend-local xid
 *	stripe runtime is latched for the duration of the run so that xids in
 *	this node's congruence class derive as own, as they do on a striped
 *	cluster; it is unlatched before returning and refused when striping is
 *	genuinely active.  Nothing shared is modified.
 *
 * Inputs: mode, xid array, iteration count, latch flag.
 * Returns: JSON text with nanosecond latency statistics, result-code
 *	histogram and counter deltas.
 * Side effects: read-only product calls; a process-local latch (see above).
 */
Datum
test_pgrac_vb_run(PG_FUNCTION_ARGS)
{
	char *mode_name = text_to_cstring(PG_GETARG_TEXT_PP(0));
	ArrayType *xid_array = PG_GETARG_ARRAYTYPE_P(1);
	int32 iterations = PG_GETARG_INT32(2);
	bool latch_stripe = PG_GETARG_BOOL(3);
	StringInfoData out;

	vb_check_caller();
	initStringInfo(&out);
#ifdef USE_PGRAC_CLUSTER
	{
		VbMode mode = vb_parse_mode(mode_name);
		Datum *xid_datums;
		bool *xid_nulls;
		int xid_count;
		uint64 *samples;
		uint64 histogram[VB_RESULT_CODES];
		uint64 total_ns = 0;
		VbCounters before;
		VbCounters after;
		volatile bool latched = false;
		int i;

		if (iterations < 1 || iterations > VB_MAX_ITERATIONS)
			ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
							errmsg("iteration count %d out of range [1, %d]", iterations,
								   VB_MAX_ITERATIONS)));
		if (ARR_ELEMTYPE(xid_array) != XIDOID)
			ereport(ERROR, (errcode(ERRCODE_DATATYPE_MISMATCH), errmsg("xid array expected")));
		deconstruct_array(xid_array, XIDOID, sizeof(TransactionId), true, TYPALIGN_INT, &xid_datums,
						  &xid_nulls, &xid_count);
		if (xid_count < 1)
			ereport(ERROR,
					(errcode(ERRCODE_INVALID_PARAMETER_VALUE), errmsg("at least one xid required")));
		for (i = 0; i < xid_count; i++)
			if (xid_nulls[i] || !TransactionIdIsNormal(DatumGetTransactionId(xid_datums[i])))
				ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
								errmsg("xid array element %d is not a normal xid", i + 1)));

		samples = (uint64 *)palloc(sizeof(uint64) * (Size)iterations);
		memset(histogram, 0, sizeof(histogram));

		if (latch_stripe) {
			if (cluster_xid_origin_slot(DatumGetTransactionId(xid_datums[0])) >= 0)
				ereport(ERROR,
						(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
						 errmsg("xid stripe runtime is already active"),
						 errhint("Run without the latch on a striped cluster.")));
			cluster_xid_stripe_latch_runtime(
				true, cluster_node_id,
				FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId));
			latched = true;
		}

		PG_TRY();
		{
			vb_sample_counters(&before);
			for (i = 0; i < iterations; i++) {
				TransactionId xid = DatumGetTransactionId(xid_datums[i % xid_count]);
				instr_time started;
				instr_time finished;
				int code;

				INSTR_TIME_SET_CURRENT(started);
				code = vb_one_call(mode, xid);
				INSTR_TIME_SET_CURRENT(finished);
				INSTR_TIME_SUBTRACT(finished, started);
				samples[i] = (uint64)INSTR_TIME_GET_NANOSEC(finished);
				total_ns += samples[i];
				histogram[code >= 0 && code < VB_RESULT_CODES ? code : VB_RESULT_CODES - 1]++;
				CHECK_FOR_INTERRUPTS();
			}
			vb_sample_counters(&after);
		}
		PG_FINALLY();
		{
			if (latched)
				cluster_xid_stripe_latch_runtime(false, -1, InvalidFullTransactionId);
		}
		PG_END_TRY();

		qsort(samples, (size_t)iterations, sizeof(uint64), vb_compare_uint64);
		appendStringInfo(&out,
						 "{\"mode\":\"%s\",\"iterations\":%d,\"xids\":%d,\"latched\":%s,"
						 "\"pid\":%d,\"total_ns\":" UINT64_FORMAT ",\"mean_ns\":" UINT64_FORMAT
						 ",\"min_ns\":" UINT64_FORMAT ",\"p50_ns\":" UINT64_FORMAT
						 ",\"p90_ns\":" UINT64_FORMAT ",\"p99_ns\":" UINT64_FORMAT
						 ",\"p999_ns\":" UINT64_FORMAT ",\"max_ns\":" UINT64_FORMAT,
						 mode_name, iterations, xid_count, latch_stripe ? "true" : "false", MyProcPid,
						 total_ns, total_ns / (uint64)iterations, samples[0],
						 vb_percentile(samples, iterations, 0.50),
						 vb_percentile(samples, iterations, 0.90),
						 vb_percentile(samples, iterations, 0.99),
						 vb_percentile(samples, iterations, 0.999), samples[iterations - 1]);
		appendStringInfoString(&out, ",\"results\":[");
		for (i = 0; i < VB_RESULT_CODES; i++)
			appendStringInfo(&out, "%s" UINT64_FORMAT, i == 0 ? "" : ",", histogram[i]);
		appendStringInfo(&out,
						 "],\"smgr_pread\":" UINT64_FORMAT ",\"smgr_open\":" UINT64_FORMAT
						 ",\"smgr_close\":" UINT64_FORMAT ",\"by_xid_scan\":" UINT64_FORMAT
						 ",\"ru_inblock\":%ld,\"ru_majflt\":%ld,\"ru_nvcsw\":%ld,\"ru_nivcsw\":%ld",
						 after.smgr_pread - before.smgr_pread, after.smgr_open - before.smgr_open,
						 after.smgr_close - before.smgr_close, after.by_xid_scan - before.by_xid_scan,
						 after.ru_inblock - before.ru_inblock, after.ru_majflt - before.ru_majflt,
						 after.ru_nvcsw - before.ru_nvcsw, after.ru_nivcsw - before.ru_nivcsw);
		if (before.disk_bytes_read != UINT64_MAX && after.disk_bytes_read != UINT64_MAX)
			appendStringInfo(&out, ",\"disk_bytes_read\":" UINT64_FORMAT,
							 after.disk_bytes_read - before.disk_bytes_read);
		else
			appendStringInfoString(&out, ",\"disk_bytes_read\":null");
		if (before.interposed && after.interposed) {
			appendStringInfoString(&out, ",\"libc\":[");
			for (i = 0; i < VB_INTERPOSE_SLOTS; i++)
				appendStringInfo(&out, "%s" UINT64_FORMAT, i == 0 ? "" : ",",
								 after.interpose[i] - before.interpose[i]);
			appendStringInfoChar(&out, ']');
		} else
			appendStringInfoString(&out, ",\"libc\":null");
		appendStringInfoChar(&out, '}');
		pfree(samples);
	}
#endif
	PG_RETURN_TEXT_P(cstring_to_text(out.data));
}
