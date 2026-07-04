/*-------------------------------------------------------------------------
 *
 * cluster_reverse_key.h
 *	  pgrac spec-6.12f -- reverse-key btree index encoding.
 *
 *	  A reverse-key index byte-reverses its leading fixed-width integer
 *	  key so a monotonically-increasing sequence (bigserial PK, the
 *	  canonical Oracle reverse-key case) lands on scattered leaf pages
 *	  instead of always the rightmost one -- dispersing the cross-node
 *	  rightmost-leaf ping that spec-5.59 measured (~241us/xfer, ~10.8e4
 *	  pings/8s).  Byte-reversal is a bijection, so equality is preserved
 *	  (reverse(a)==reverse(b) iff a==b); order is NOT (reverse is not
 *	  monotone), which is why the planner treats the index as
 *	  equality-only (Q18-A: no range / ORDER BY / index-only scan).
 *
 *	  The transform is applied at exactly two AM choke points -- insert
 *	  values (btinsert) and equality scan keys (btrescan) -- so the stored
 *	  keys and the search key are reversed consistently and the btree
 *	  comparator (unchanged) compares reversed integers.
 *
 *	  Supported leading key types: int2 / int4 / int8 (pass-by-value,
 *	  typlen in {2,4,8}); any other shape is rejected at CREATE INDEX
 *	  (rule 8 explicit rejection, never a silent wrong-result path).
 *
 * Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * Spec: spec-6.12-crossnode-cache-fusion-perf-optimization.md
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_reverse_key.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_REVERSE_KEY_H
#define CLUSTER_REVERSE_KEY_H

#include "postgres.h"
#include "access/skey.h"
#include "utils/rel.h"

/*
 * True if `typlen` is a supported reverse-key width (2/4/8, pass-by-value
 * integer).  The caller has already established the type is by-value.
 */
extern bool cluster_reverse_key_typlen_supported(int16 typlen);

/*
 * Byte-reverse a fixed-width pass-by-value integer datum over `typlen`
 * bytes.  Involutive: encoding twice yields the original.  Undefined for
 * unsupported typlen (callers gate on cluster_reverse_key_typlen_supported
 * / the CREATE-INDEX validation).
 */
extern Datum cluster_reverse_key_encode(Datum value, int16 typlen);

/*
 * Rewrite the scan keys of a reverse-key index scan so equality probes
 * search for the stored (byte-reversed) encoding.  Handles cross-type
 * probes (e.g. an int4 literal against an int8 key via integer_ops
 * int84eq) by normalizing the probe datum to the index key type first --
 * byte-reversal is a bijection only at equal width.  Must be called
 * exactly once per fresh scankey copy (btrescan); errors out on any
 * non-equality key (planner contract violation, Q18-A).
 */
extern void cluster_reverse_key_transform_scankeys(Relation indexRel, ScanKey keys, int nkeys);

/*
 * Validate a reverse-key index at build time.  Rejects (FEATURE_NOT_
 * SUPPORTED) any index whose leading key is not a single supported
 * integer column.  No-op when the index is not a reverse-key index.
 */
extern void cluster_reverse_key_validate_index(Relation index);

#endif /* CLUSTER_REVERSE_KEY_H */
