/*-------------------------------------------------------------------------
 *
 * cluster_sinval_bcast.h
 *	  pgrac cluster SI Broadcaster aux process — spec-2.38 D4.
 *
 *	  Aux process spawned by postmaster Phase 4 (after IC + LMON);
 *	  main loop drains ClusterSinvalOutbound + ClusterSinvalInbound and
 *	  performs fail-safe SIResetAll() on inbound overflow.
 *
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 * Portions Copyright (c) 2026, pgrac contributors
 *
 * Author: SqlRush <sqlrush@gmail.com>
 *
 * IDENTIFICATION
 *	  src/include/cluster/cluster_sinval_bcast.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef CLUSTER_SINVAL_BCAST_H
#define CLUSTER_SINVAL_BCAST_H

#ifdef USE_PGRAC_CLUSTER

/*
 * SinvalBcastMain — aux process main entry point dispatched by
 * AuxiliaryProcessMain.  pg_attribute_noreturn() because the function
 * loops until SIGTERM and then calls proc_exit(0).
 */
extern void SinvalBcastMain(void) pg_attribute_noreturn();

#endif /* USE_PGRAC_CLUSTER */
#endif /* CLUSTER_SINVAL_BCAST_H */
