/*-------------------------------------------------------------------------
 *
 * test_cluster_undo_segment_half_page_oversized.c
 *	  Negative compile fixture for Spec 8.4A U9.
 *
 * This translation unit must not compile: 125 durable 32-byte TT slots at
 * the frozen byte-112 offset cross the first-half-page boundary.
 *
 *-------------------------------------------------------------------------
 */
#define USE_PGRAC_CLUSTER 1

#include "postgres.h"

#include <stddef.h>

#include "cluster/cluster_tt_slot.h"

typedef struct ClusterUndoOversizedHalfPageFixture {
	/* Layout-only prefix is required by the negative offsetof assertion. */
	/* cppcheck-suppress unusedStructMember */
	char prefix[112];
	TTSlot tt_slots[125];
} ClusterUndoOversizedHalfPageFixture;

StaticAssertDecl(offsetof(ClusterUndoOversizedHalfPageFixture, tt_slots)
						 + lengthof(((ClusterUndoOversizedHalfPageFixture *)0)->tt_slots)
							   * sizeof(TTSlot)
					 <= 4096,
				 "spec-8.4A U9 oversized TT fixture must fail");
