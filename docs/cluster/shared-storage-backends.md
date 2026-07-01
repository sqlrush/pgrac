# Shared-Storage Backends

## spec-6.0a Implementation Notes

spec-6.0a lands the `block_device` production shared-storage backend on top of the `ClusterSharedFsOps` provider framework. The CI-portable path uses a regular-file raw image with `cluster.block_device_use_odirect=off`; production deployments should use a persistent block-device path with direct I/O enabled.

The implementation intentionally records these frozen-spec deltas:

- The raw backend opens the device with `BasicOpenFile(..., PG_O_DIRECT)` instead of adding a PostgreSQL `fd.c` VFD substrate. This keeps the PG buffered file path untouched and matches the voting-disk raw-fd precedent. The direct-I/O contract remains fail-closed at backend startup: unsupported `PG_O_DIRECT` or incompatible `BLCKSZ`/`PG_IO_ALIGN_SIZE` raises `cluster_storage_io_alignment`.
- `cluster.block_device_path` accepts either a block device or a regular-file raw image. Regular files are accepted for CI and development conformance tests only and emit a startup warning.
- The frozen spec reserved SQLSTATEs `58R02` and `58R03`, but current main already uses them. This implementation uses `58R14` for `cluster_storage_io_alignment` and `58R15` for `cluster_storage_fence_unavailable`.
- SCSI-3 PR coverage in CI is limited to fail-closed forced-driver behavior on a non-PR raw image and unit coverage for node-key derivation. Hardware PR probe/register legs require a real SG_IO-capable device and remain external/manual release evidence.
- The raw layout implementation currently lives in `cluster_shared_fs_block_device.c`. A future cleanup should split the on-device layout/allocator/cache code into raw-layout-specific files without changing the storage contract.
