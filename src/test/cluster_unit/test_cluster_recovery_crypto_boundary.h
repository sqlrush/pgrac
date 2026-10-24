/* Standalone recovery crypto allocation boundary.
 * Author: SqlRush <sqlrush@gmail.com>
 * Copyright (c) 2026, pgrac contributors. */
#ifndef TEST_CLUSTER_RECOVERY_CRYPTO_BOUNDARY_H
#define TEST_CLUSTER_RECOVERY_CRYPTO_BOUNDARY_H
#include "utils/resowner_private.h"

/* The standalone recovery fixtures links the production OpenSSL cryptohash
 * object, which normally receives these ResourceOwner/MemoryContext hooks
 * from the backend.  Keep this fixture self-contained so their physical-root
 * assertions execute instead of stopping at link time. */
ResourceOwner CurrentResourceOwner = NULL;
MemoryContext TopMemoryContext = NULL;

void *
MemoryContextAlloc(MemoryContext context pg_attribute_unused(), Size size)
{
	return malloc(size);
}

void
pfree(void *pointer)
{
	free(pointer);
}

void
ResourceOwnerEnlargeCryptoHash(ResourceOwner owner pg_attribute_unused())
{}

void
ResourceOwnerRememberCryptoHash(ResourceOwner owner pg_attribute_unused(),
								Datum handle pg_attribute_unused())
{}

void
ResourceOwnerForgetCryptoHash(ResourceOwner owner pg_attribute_unused(),
							  Datum handle pg_attribute_unused())
{}

#endif
