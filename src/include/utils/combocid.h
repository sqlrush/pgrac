/*-------------------------------------------------------------------------
 *
 * combocid.h
 *	  Combo command ID support routines
 *
 *
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * src/include/utils/combocid.h
 *
 *-------------------------------------------------------------------------
 */
#ifndef COMBOCID_H
#define COMBOCID_H

/*
 * HeapTupleHeaderGetCmin and HeapTupleHeaderGetCmax function prototypes
 * are in access/htup.h, because that's where the macro definitions that
 * those functions replaced used to be.
 */

extern void AtEOXact_ComboCid(void);
extern void RestoreComboCIDState(char *comboCIDstate);
extern void SerializeComboCIDState(Size maxsize, char *start_address);
extern Size EstimateComboCIDStateSpace(void);

#ifdef USE_PGRAC_CLUSTER
struct HeapTupleHeaderData;
/* Local decoding only, no ownership authority. Caller must first prove the
 * exact current owner from the complete typed set. Never resolves raw MX. */
extern bool cluster_combo_cmax_local(struct HeapTupleHeaderData *tuple, CommandId *cmax);
#endif

#endif							/* COMBOCID_H */
