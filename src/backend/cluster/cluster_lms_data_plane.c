/*-------------------------------------------------------------------------
 *
 * cluster_lms_data_plane.c
 *	  pgrac DATA-plane (LMS<->LMS) interconnect loop — spec-7.2 D2.
 *
 *	  The LMS aux process owns the DATA plane: its own tier1 listener
 *	  (bound to the node's declared data_addr), its own active-role
 *	  connects, its own HELLO state machines, and — after the spec-7.2
 *	  D3/D4 plane flip — the GCS block family dispatch and replies.
 *	  This mirrors the LMON tier1 mesh loop (cluster_lmon.c) with the
 *	  control-plane duties removed:
 *
 *	    - NO heartbeat send and NO heartbeat-liveness scan: the CONTROL
 *	      plane (CSSD/LMON) is the single liveness authority (spec-7.2
 *	      Q-D2-1).  A dead peer is discovered via CONTROL membership
 *	      events (spec-7.2 D5 wires the forced reset), via TCP errors
 *	      on use, or via requester timeouts (fail-closed, 53R90 family).
 *	    - Connect-establishment timeouts still apply (a peer that never
 *	      completes HELLO must not pin a pending slot forever).
 *	    - Reconnect backoff mirrors the LMON cadence (one heartbeat
 *	      interval between attempts).
 *
 *	  Process-plane discipline: LmsMain aims this process's tier1 state
 *	  at the DATA plane (cluster_ic_tier1_set_my_plane) before any tier1
 *	  use, so every tier1 helper called below lands on the DATA-plane
 *	  shmem instance and this process's private fd/buffer statics.  The
 *	  CONTROL plane in LMON is untouched.
 *
 *	  A node without a declared data_addr runs no DATA plane: startup
 *	  returns false and LmsMain keeps its historic latch-only loop
 *	  (park-serve construction keeps working; the plane flip in D3/D4
 *	  is what makes a missing DATA plane a hard error for block ships).
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/backend/cluster/cluster_lms_data_plane.c
 *
 * NOTES
 *	  This is a pgrac-original file.  Compiled only in --enable-cluster
 *	  builds.  Spec: spec-7.2-ic-data-plane-decoupling.md (D2c).
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include <unistd.h>

#include "cluster/cluster_conf.h"
#include "cluster/cluster_elog.h"
#include "cluster/cluster_epoch.h" /* PGRAC: spec-7.2 D5 epoch watch */
#include "cluster/cluster_guc.h"
#include "cluster/cluster_ic.h"
#include "cluster/cluster_ic_tier1.h"
#include "cluster/cluster_lms.h"
#include "miscadmin.h"
#include "storage/latch.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#ifdef USE_PGRAC_CLUSTER

/* Per-peer DATA-plane connection substate (mirrors the LMON tracker). */
typedef enum LmsDpSubstate {
	LMS_DP_DOWN = 0,
	LMS_DP_CONNECT_PEND,
	LMS_DP_HELLO_SENDING,
	LMS_DP_HELLO_WAIT,
	LMS_DP_CONNECTED
} LmsDpSubstate;

typedef struct LmsDpPeerTrack {
	int fd;
	LmsDpSubstate substate;
	bool is_active; /* mesh role: we dial (true) or they dial us */
	TimestampTz next_attempt_at;
	TimestampTz connect_started_at;
} LmsDpPeerTrack;

static LmsDpPeerTrack dp_track[CLUSTER_MAX_NODES];
static int dp_pending_fds[CLUSTER_MAX_NODES];
static int dp_listener_fd = -1;
static WaitEventSet *dp_wes = NULL;
static bool dp_wes_dirty = true;
static bool dp_enabled = false;

/*
 * cluster_lms_data_plane_startup — bind the DATA listener + init the mesh.
 *
 *	Returns true when the DATA plane is live (listener bound);  false =
 *	plane off for this node (no data_addr declared, cluster disabled, or
 *	a stub/mock interconnect tier).  Callable once from LmsMain before
 *	the READY transition.
 */
bool
cluster_lms_data_plane_startup(void)
{
	int32 self_id = cluster_node_id;
	int32 pi;

	Assert(MyBackendType == B_LMS);

	if (!cluster_enabled)
		return false;
	if (cluster_interconnect_tier != CLUSTER_IC_TIER_1
		&& cluster_interconnect_tier != CLUSTER_IC_TIER_2
		&& cluster_interconnect_tier != CLUSTER_IC_TIER_3)
		return false;

	/* Aim this process's tier1 state at the DATA plane BEFORE any use. */
	cluster_ic_tier1_set_my_plane(CLUSTER_IC_PLANE_DATA);

	dp_listener_fd = cluster_ic_tier1_listener_bind();
	if (dp_listener_fd < 0)
		return false; /* no data_addr — plane off (LOG emitted inside) */

	for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
		dp_track[pi].fd = -1;
		dp_track[pi].substate = LMS_DP_DOWN;
		dp_track[pi].is_active = false;
		dp_track[pi].next_attempt_at = 0;
		dp_track[pi].connect_started_at = 0;
		dp_pending_fds[pi] = -1;

		if (pi == self_id)
			continue;
		if (cluster_conf_lookup_node(pi) == NULL)
			continue;
		dp_track[pi].is_active
			= (cluster_ic_mesh_role_for_pair(self_id, pi) == CLUSTER_IC_MESH_ACTIVE);
	}

	dp_wes_dirty = true;
	dp_enabled = true;
	ereport(LOG, (errmsg("cluster_lms: DATA-plane listener bound (node %d)", self_id)));
	return true;
}

bool
cluster_lms_data_plane_enabled(void)
{
	return dp_enabled;
}

/* Rebuild the WaitEventSet when the fd set changed. */
static void
dp_rebuild_wes(void)
{
	int32 pi;

	if (dp_wes != NULL) {
		FreeWaitEventSet(dp_wes);
		dp_wes = NULL;
	}

	dp_wes = CreateWaitEventSet(CurrentMemoryContext, 3 + 2 * CLUSTER_MAX_NODES);
	AddWaitEventToSet(dp_wes, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
	AddWaitEventToSet(dp_wes, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
	if (dp_listener_fd >= 0)
		AddWaitEventToSet(dp_wes, WL_SOCKET_READABLE, dp_listener_fd, NULL, (void *)(intptr_t)-1);

	for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
		int events;

		if (dp_track[pi].fd < 0)
			continue;

		switch (dp_track[pi].substate) {
		case LMS_DP_CONNECT_PEND:
		case LMS_DP_HELLO_SENDING:
			events = WL_SOCKET_WRITEABLE;
			break;
		case LMS_DP_HELLO_WAIT:
			events = WL_SOCKET_READABLE;
			break;
		case LMS_DP_CONNECTED:
			events = WL_SOCKET_READABLE;
			if (cluster_ic_tier1_pending_outbound(pi))
				events |= WL_SOCKET_WRITEABLE;
			break;
		default:
			continue;
		}

		AddWaitEventToSet(dp_wes, events, dp_track[pi].fd, NULL, (void *)(intptr_t)pi);
	}

	for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
		if (dp_pending_fds[pi] < 0)
			continue;
		AddWaitEventToSet(dp_wes, WL_SOCKET_READABLE, dp_pending_fds[pi], NULL,
						  (void *)(intptr_t)(CLUSTER_MAX_NODES + pi));
	}

	dp_wes_dirty = false;
}

/*
 * cluster_lms_data_plane_tick — one management + wait + dispatch round.
 *
 *	Called from the LmsMain loop in place of the historic plain
 *	WaitLatch:  the wait now also wakes on DATA sockets.  MyLatch is
 *	reset here, so the caller must run its latch-driven drains (CR
 *	park-serve etc.) BEFORE calling in, mirroring the LMON loop shape.
 */
void
cluster_lms_data_plane_tick(long timeout_ms)
{
	WaitEvent ev[2 * CLUSTER_MAX_NODES + 3];
	TimestampTz now;
	int n_events;
	int32 pi;
	int i;

	Assert(dp_enabled);

	now = GetCurrentTimestamp();

	/*
	 * PGRAC: spec-7.2 D5 (INV-7.2-CONN-EPOCH ③) — proactive connection
	 * reset on an epoch bump:  every DATA connection is bound to the
	 * epoch it was established under, so a bump force-closes the mesh
	 * and the reconnect below re-HELLOs at the current epoch.  The
	 * sender gate in tier1_send_bytes is the structural backstop for
	 * the window between the bump and this tick.
	 */
	{
		static uint64 dp_last_epoch = 0;
		static bool dp_epoch_seen = false;
		uint64 cur_epoch = cluster_epoch_get_current();

		if (!dp_epoch_seen) {
			dp_last_epoch = cur_epoch;
			dp_epoch_seen = true;
		} else if (cur_epoch != dp_last_epoch) {
			for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
				if (dp_track[pi].fd >= 0) {
					cluster_ic_tier1_close_peer(pi, "data-plane epoch bump reset");
					dp_track[pi].fd = -1;
					dp_track[pi].substate = LMS_DP_DOWN;
					dp_track[pi].connect_started_at = 0;
					dp_track[pi].next_attempt_at = 0; /* reconnect immediately */
					dp_wes_dirty = true;
				}
			}
			dp_last_epoch = cur_epoch;
		}
	}

	/* Active-role reconnect for DOWN peers whose backoff elapsed. */
	for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
		int new_fd = -1;

		if (pi == cluster_node_id || !dp_track[pi].is_active)
			continue;
		if (dp_track[pi].substate != LMS_DP_DOWN)
			continue;
		if (dp_track[pi].next_attempt_at > now)
			continue;

		dp_track[pi].next_attempt_at
			= now + (int64)cluster_interconnect_heartbeat_interval_ms * INT64CONST(1000);

		/* A peer without data_addr is unreachable on this plane —
		 * connect_one fails fast via the NULL peer_addr;  skip quietly
		 * (the conf may declare it later; SIGHUP reload re-checks). */
		if (cluster_ic_tier1_connect_one(pi, &new_fd) && new_fd >= 0) {
			dp_track[pi].fd = new_fd;
			dp_track[pi].substate = LMS_DP_CONNECT_PEND;
			dp_track[pi].connect_started_at = now;
			dp_wes_dirty = true;
		}
	}

	/* Connect-establishment timeout scan (no liveness scan on DATA:
	 * CONTROL is the liveness authority, Q-D2-1). */
	{
		int64 connect_to_us = (int64)cluster_interconnect_connect_timeout_ms * INT64CONST(1000);

		for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
			if (dp_track[pi].fd < 0)
				continue;
			if ((dp_track[pi].substate == LMS_DP_CONNECT_PEND
				 || dp_track[pi].substate == LMS_DP_HELLO_SENDING
				 || dp_track[pi].substate == LMS_DP_HELLO_WAIT)
				&& dp_track[pi].connect_started_at > 0
				&& now > dp_track[pi].connect_started_at + connect_to_us) {
				cluster_ic_tier1_close_peer(pi, "data-plane connect timeout");
				dp_track[pi].fd = -1;
				dp_track[pi].substate = LMS_DP_DOWN;
				dp_track[pi].connect_started_at = 0;
				dp_wes_dirty = true;
			}
		}
	}

	if (dp_wes_dirty)
		dp_rebuild_wes();

	if (timeout_ms < 0)
		timeout_ms = 0;

	n_events = WaitEventSetWait(dp_wes, timeout_ms, ev, lengthof(ev), WAIT_EVENT_PG_SLEEP);

	for (i = 0; i < n_events; i++) {
		intptr_t tag = (intptr_t)ev[i].user_data;

		if (ev[i].events & WL_LATCH_SET) {
			ResetLatch(MyLatch);
			continue;
		}

		if (tag == -1) {
			/* Listener: drain all pending accepts into anon slots. */
			for (;;) {
				int new_fd = -1;
				int32 dummy_peer_id = -1;
				int slot;

				if (!cluster_ic_tier1_accept_one(&new_fd, &dummy_peer_id))
					break;
				if (new_fd < 0)
					break;

				for (slot = 0; slot < CLUSTER_MAX_NODES; slot++)
					if (dp_pending_fds[slot] < 0)
						break;
				if (slot >= CLUSTER_MAX_NODES) {
					(void)close(new_fd);
					continue;
				}
				dp_pending_fds[slot] = new_fd;
				dp_wes_dirty = true;
			}
		} else if (tag >= 0 && tag < CLUSTER_MAX_NODES) {
			int32 peer = (int32)tag;
			int peer_fd = dp_track[peer].fd;

			if (peer_fd < 0)
				continue;

			if (dp_track[peer].substate == LMS_DP_CONNECT_PEND
				&& (ev[i].events & WL_SOCKET_WRITEABLE)) {
				if (cluster_ic_tier1_finish_connect(peer, peer_fd)) {
					if (cluster_ic_tier1_hello_send_remaining(peer) == 0)
						dp_track[peer].substate = LMS_DP_CONNECTED;
					else
						dp_track[peer].substate = LMS_DP_HELLO_SENDING;
					dp_wes_dirty = true;
				} else {
					dp_track[peer].fd = -1;
					dp_track[peer].substate = LMS_DP_DOWN;
					dp_wes_dirty = true;
				}
			} else if (dp_track[peer].substate == LMS_DP_HELLO_SENDING
					   && (ev[i].events & WL_SOCKET_WRITEABLE)) {
				if (cluster_ic_tier1_continue_hello_send(peer, peer_fd)) {
					if (cluster_ic_tier1_hello_send_remaining(peer) == 0) {
						dp_track[peer].substate = LMS_DP_CONNECTED;
						dp_wes_dirty = true;
					}
				} else {
					dp_track[peer].fd = -1;
					dp_track[peer].substate = LMS_DP_DOWN;
					dp_wes_dirty = true;
				}
			} else if (dp_track[peer].substate == LMS_DP_CONNECTED
					   && (ev[i].events & WL_SOCKET_READABLE)) {
				/* Generic envelope pump: recv + verify + dispatch.  No
				 * DATA msg_type is registered before the D3/D4 flip, so
				 * pre-flip traffic is limited to HELLO/errors;  post-
				 * flip this is the block-family dispatch entry. */
				if (!cluster_ic_tier1_recv_heartbeat_drain(peer, peer_fd)) {
					cluster_ic_tier1_close_peer(peer, "data-plane recv failed");
					dp_track[peer].fd = -1;
					dp_track[peer].substate = LMS_DP_DOWN;
					dp_wes_dirty = true;
				}
			}
		} else if (tag >= CLUSTER_MAX_NODES && tag < 2 * CLUSTER_MAX_NODES) {
			/* Anonymous accepted fd: accumulate + verify HELLO. */
			int slot = (int)(tag - CLUSTER_MAX_NODES);
			int pend_fd = dp_pending_fds[slot];
			int32 learned = -1;

			if (pend_fd < 0)
				continue;

			if (cluster_ic_tier1_continue_hello_recv(slot, pend_fd, &learned)) {
				if (learned >= 0) {
					/* HELLO complete: bind to the learned peer. */
					if (dp_track[learned].fd >= 0 && dp_track[learned].fd != pend_fd)
						cluster_ic_tier1_close_peer(learned, "data-plane duplicate connection");
					dp_track[learned].fd = pend_fd;
					dp_track[learned].substate = LMS_DP_CONNECTED;
					dp_pending_fds[slot] = -1;
					cluster_ic_tier1_anon_hello_reset(slot);
					dp_wes_dirty = true;
				}
				/* else: still accumulating; stay registered READABLE */
			} else {
				(void)close(pend_fd);
				dp_pending_fds[slot] = -1;
				cluster_ic_tier1_anon_hello_reset(slot);
				dp_wes_dirty = true;
			}
		}
	}
}

/* Close everything we own (LmsMain teardown path). */
void
cluster_lms_data_plane_shutdown(void)
{
	int32 pi;

	if (!dp_enabled)
		return;

	for (pi = 0; pi < CLUSTER_MAX_NODES; pi++) {
		if (dp_track[pi].fd >= 0) {
			cluster_ic_tier1_close_peer(pi, "lms shutdown");
			dp_track[pi].fd = -1;
			dp_track[pi].substate = LMS_DP_DOWN;
		}
		if (dp_pending_fds[pi] >= 0) {
			(void)close(dp_pending_fds[pi]);
			dp_pending_fds[pi] = -1;
		}
	}
	if (dp_wes != NULL) {
		FreeWaitEventSet(dp_wes);
		dp_wes = NULL;
	}
	dp_enabled = false;
}

#endif /* USE_PGRAC_CLUSTER */
