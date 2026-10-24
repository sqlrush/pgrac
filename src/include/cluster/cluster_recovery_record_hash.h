/*-------------------------------------------------------------------------
 * cluster_recovery_record_hash.h
 *    Canonical complete decoded-record observations for cold recovery.
 * Copyright (c) 2026, pgrac contributors
 * Author: SqlRush <sqlrush@gmail.com>
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_RECOVERY_RECORD_HASH_H
#define CLUSTER_RECOVERY_RECORD_HASH_H
#ifdef USE_PGRAC_CLUSTER
#include "access/xlogreader.h"
#include "common/cryptohash.h"

/* Append one original decoded record to an initialized SHA-256 context.
 * Domain-separated v1 encoding: every scalar is eight little-endian bytes;
 * counted payload/image/data and 16-byte incarnations are copied in full.
 * All persistent decoded header/block/edge fields are included, never
 * addresses, allocation sizes, compiler padding or prefetch state.
 * This is an observation fingerprint, not authority/shape/apply validation.
 * Original source/cut/ROOT/physical-reader checks remain mandatory. On
 * failure the partially updated context must be discarded. No allocation. */
extern bool cluster_recovery_record_hash_update(pg_cryptohash_ctx *hash,
												const XLogReaderState *reader);
#endif
#endif
