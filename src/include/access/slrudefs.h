/* Shared SLRU file geometry for native writers and frontend readers.
 * Author: SqlRush <sqlrush@gmail.com>
 * Portions Copyright (c) 1996-2023, PostgreSQL Global Development Group
 */
#ifndef SLRUDEFS_H
#define SLRUDEFS_H

/*
 * Define SLRU segment size.  A page is the same BLCKSZ as is used everywhere
 * else in Postgres.  The segment size can be chosen somewhat arbitrarily;
 * we make it 32 pages by default, or 256Kb, i.e. 1M transactions for CLOG
 * or 64K transactions for SUBTRANS.
 *
 * Note: because TransactionIds are 32 bits and wrap around at 0xFFFFFFFF,
 * page numbering also wraps around at 0xFFFFFFFF/xxxx_XACTS_PER_PAGE (where
 * xxxx is CLOG or SUBTRANS, respectively), and segment numbering at
 * 0xFFFFFFFF/xxxx_XACTS_PER_PAGE/SLRU_PAGES_PER_SEGMENT.  We need
 * take no explicit notice of that fact in slru.c, except when comparing
 * segment and page numbers in SimpleLruTruncate (see PagePrecedes()).
 */
#define SLRU_PAGES_PER_SEGMENT	32

#endif /* SLRUDEFS_H */
