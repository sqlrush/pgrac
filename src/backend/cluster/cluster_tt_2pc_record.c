/*-------------------------------------------------------------------------
 *
 * cluster_tt_2pc_record.c
 *	  pgrac 2PC record pure serialize/parse layer (spec-3.15 D1).
 *
 *	  Split from cluster_tt_2pc.c so the record schema has exactly one
 *	  implementation (L212/L216) with zero backend dependencies beyond
 *	  CRC32C (libpgport) -- cluster_unit links this TU alone and
 *	  enumerates the corruption matrix.
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_tt_2pc_record.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.
 *	  Spec: spec-3.15-2pc-prepared-visibility.md (FROZEN v0.2) §2.1.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#ifdef USE_PGRAC_CLUSTER

#include "port/pg_crc32c.h"

#include "cluster/cluster_tt_2pc.h"


/*
 * cluster_tt_2pc_record_size -- exact serialized length.
 */
uint32
cluster_tt_2pc_record_size(uint16 nbindings, uint32 nsublinks)
{
	return (uint32)sizeof(ClusterTT2PCRecord)
		   + (uint32)nbindings * (uint32)sizeof(ClusterTT2PCBinding)
		   + nsublinks * (uint32)sizeof(ClusterTT2PCSubLink);
}


/*
 * CRC coverage: everything after the crc field (i.e. the two count
 * fields live BEFORE crc in the struct, so cover from the byte after
 * crc through the payload, plus the header fields before crc).  To keep
 * it simple and order-independent: compute over the whole buffer with
 * the crc field itself zeroed.
 */
static uint32
record_crc(const char *buf, uint32 len)
{
	ClusterTT2PCRecord hdr;
	pg_crc32c crc;

	Assert(len >= sizeof(ClusterTT2PCRecord));
	memcpy(&hdr, buf, sizeof(hdr));
	hdr.crc = 0;

	INIT_CRC32C(crc);
	COMP_CRC32C(crc, (const char *)&hdr, sizeof(hdr));
	COMP_CRC32C(crc, buf + sizeof(hdr), len - (uint32)sizeof(hdr));
	FIN_CRC32C(crc);
	return (uint32)crc;
}


/*
 * cluster_tt_2pc_serialize -- pure serializer.
 *
 *	Returns bytes written, or 0 when counts exceed the caps or dstcap is
 *	too small (callers treat 0 as a hard error; AtPrepare raises the
 *	§1.4-4 capacity ereport).
 */
uint32
cluster_tt_2pc_serialize(const ClusterTT2PCBinding *bindings, uint16 nbindings,
						 const ClusterTT2PCSubLink *sublinks, uint32 nsublinks, char *dst,
						 uint32 dstcap)
{
	ClusterTT2PCRecord hdr;
	uint32 need = cluster_tt_2pc_record_size(nbindings, nsublinks);
	char *p;

	if (nbindings > CLUSTER_TT_2PC_MAX_BINDINGS)
		return 0;
	if (nsublinks > CLUSTER_TT_2PC_MAX_SUBLINKS)
		return 0;
	if (dst == NULL || dstcap < need)
		return 0;

	memset(&hdr, 0, sizeof(hdr));
	hdr.magic = CLUSTER_TT_2PC_MAGIC;
	hdr.version = CLUSTER_TT_2PC_VERSION;
	hdr.nbindings = nbindings;
	hdr.nsublinks = nsublinks;
	hdr.crc = 0;

	p = dst;
	memcpy(p, &hdr, sizeof(hdr));
	p += sizeof(hdr);
	if (nbindings > 0) {
		memcpy(p, bindings, (size_t)nbindings * sizeof(ClusterTT2PCBinding));
		p += (size_t)nbindings * sizeof(ClusterTT2PCBinding);
	}
	if (nsublinks > 0) {
		memcpy(p, sublinks, (size_t)nsublinks * sizeof(ClusterTT2PCSubLink));
		p += (size_t)nsublinks * sizeof(ClusterTT2PCSubLink);
	}
	Assert((uint32)(p - dst) == need);

	/* Stamp crc last (computed with the field zeroed). */
	((ClusterTT2PCRecord *)dst)->crc = record_crc(dst, need);
	return need;
}


/*
 * cluster_tt_2pc_parse_record -- validate + expose pointers.
 *
 *	Pure: returns false on any structural mismatch (magic / version /
 *	length arithmetic / caps / crc); the caller fail-closes with
 *	DATA_CORRUPTED.  No partial output on failure.
 */
bool
cluster_tt_2pc_parse_record(const void *recdata, uint32 len, ClusterTT2PCParsed *out)
{
	const char *buf = (const char *)recdata;
	ClusterTT2PCRecord hdr;
	uint32 need;

	if (out == NULL || buf == NULL)
		return false;
	if (len < sizeof(ClusterTT2PCRecord))
		return false;

	memcpy(&hdr, buf, sizeof(hdr));
	if (hdr.magic != CLUSTER_TT_2PC_MAGIC)
		return false;
	if (hdr.version != CLUSTER_TT_2PC_VERSION)
		return false;
	if (hdr.nbindings > CLUSTER_TT_2PC_MAX_BINDINGS)
		return false;
	if (hdr.nsublinks > CLUSTER_TT_2PC_MAX_SUBLINKS)
		return false;

	need = cluster_tt_2pc_record_size(hdr.nbindings, hdr.nsublinks);
	if (len != need)
		return false;
	if (record_crc(buf, len) != hdr.crc)
		return false;

	out->nbindings = hdr.nbindings;
	out->nsublinks = hdr.nsublinks;
	out->bindings = (hdr.nbindings > 0)
						? (const ClusterTT2PCBinding *)(buf + sizeof(ClusterTT2PCRecord))
						: NULL;
	out->sublinks
		= (hdr.nsublinks > 0)
			  ? (const ClusterTT2PCSubLink *)(buf + sizeof(ClusterTT2PCRecord)
											  + (size_t)hdr.nbindings * sizeof(ClusterTT2PCBinding))
			  : NULL;
	return true;
}

#endif /* USE_PGRAC_CLUSTER */
