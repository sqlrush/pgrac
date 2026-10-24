/* Original PREPARE producer framing; resource bodies stay opaque obligations.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RECOVERY_PREPARE_FIXTURE_H
#define TEST_CLUSTER_RECOVERY_PREPARE_FIXTURE_H
#include "access/twophase_rmgr.h"

#define TEST_PREPARE_MAX 2048
static size_t
test_prepare_append(char *bytes, size_t at, const void *item, size_t length)
{
	Assert(at + MAXALIGN(length) <= TEST_PREPARE_MAX);
	if (length != 0)
		memcpy(bytes + at, item, length);
	return at + MAXALIGN(length);
}

static uint32
test_prepare_payload(char *bytes, bool full, uint16 gidlen)
{
	xl_xact_prepare header = { 0 };
	TransactionId children[] = { 4195137, 4195138 };
	RelFileLocator commitrels[] = { { 1663, 5, 16386 }, { 1663, 5, 16387 } };
	RelFileLocator abortrels[] = { { 1664, 0, 16388 }, { 1663, 5, 16389 } };
	xl_xact_stats_item commitstats[2], abortstats[2];
	SharedInvalidationMessage messages[2];
	TwoPhaseRecordOnDisk end = { 0 };
	char gid[GIDSIZE];
	size_t at = MAXALIGN(sizeof(header));
	memset(bytes, 0xa5, TEST_PREPARE_MAX);
	memset(gid, 'g', sizeof(gid));
	Assert(gidlen > 0 && gidlen <= GIDSIZE);
	gid[gidlen - 1] = '\0';
	header.magic = TWOPHASE_MAGIC;
	header.xid = 4195136;
	header.database = 5;
	header.owner = 10;
	header.prepared_at = 8765;
	header.origin_lsn = 0xabc;
	header.origin_timestamp = 7654;
	header.gidlen = gidlen;
	header.initfileinval = full;
	header.nsubxacts = header.ncommitrels = header.nabortrels = full ? 2 : 0;
	header.ncommitstats = header.nabortstats = header.ninvalmsgs = full ? 2 : 0;
	at = test_prepare_append(bytes, at, gid, gidlen);
	if (full) {
		memset(commitstats, 0x31, sizeof(commitstats));
		memset(abortstats, 0x42, sizeof(abortstats));
		memset(messages, 0x53, sizeof(messages));
		at = test_prepare_append(bytes, at, children, sizeof(children));
		at = test_prepare_append(bytes, at, commitrels, sizeof(commitrels));
		at = test_prepare_append(bytes, at, abortrels, sizeof(abortrels));
		at = test_prepare_append(bytes, at, commitstats, sizeof(commitstats));
		at = test_prepare_append(bytes, at, abortstats, sizeof(abortstats));
		at = test_prepare_append(bytes, at, messages, sizeof(messages));
		for (unsigned id = 1; id <= TWOPHASE_RM_MAX_ID; ++id) {
			TwoPhaseRecordOnDisk resource = { .len = id + 3, .rmid = id, .info = 0xc000 + id };
			char data[16];
			memset(data, 10 + id, sizeof(data));
			at = test_prepare_append(bytes, at, &resource, sizeof(resource));
			at = test_prepare_append(bytes, at, data, resource.len);
		}
	}
	at = test_prepare_append(bytes, at, &end, sizeof(end));
	header.total_len = at + sizeof(pg_crc32c);
	memcpy(bytes, &header, sizeof(header));
	return at;
}
#endif
