/* Isolate the existing native CLOG fixture's defined public symbols.
 * Generated once from its parent object; no build-time generator needed.
 * Author: SqlRush <sqlrush@gmail.com> */
#ifndef TEST_CLUSTER_RELMAP_NATIVE_CLOG_NAMESPACE_H
#define TEST_CLUSTER_RELMAP_NATIVE_CLOG_NAMESPACE_H
#define AbsorbSyncRequests d3_clog_AbsorbSyncRequests
#define AllocateDir d3_clog_AllocateDir
#define BootStrapCLOG d3_clog_BootStrapCLOG
#define CLOGShmemBuffers d3_clog_CLOGShmemBuffers
#define CLOGShmemInit d3_clog_CLOGShmemInit
#define CLOGShmemSize d3_clog_CLOGShmemSize
#define CheckPointCLOG d3_clog_CheckPointCLOG
#define CheckpointStats d3_clog_CheckpointStats
#define CloseTransientFile d3_clog_CloseTransientFile
#define ClusterClogAdoptNativeStatus d3_clog_ClusterClogAdoptNativeStatus
#define CritSectionCount d3_clog_CritSectionCount
#define ExceptionalCondition d3_clog_ExceptionalCondition
#define ExtendCLOG d3_clog_ExtendCLOG
#define FreeDir d3_clog_FreeDir
#define InRecovery d3_clog_InRecovery
#define InterruptHoldoffCount d3_clog_InterruptHoldoffCount
#define InterruptPending d3_clog_InterruptPending
#define IsUnderPostmaster d3_clog_IsUnderPostmaster
#define LWLockAcquire d3_clog_LWLockAcquire
#define LWLockConditionalAcquire d3_clog_LWLockConditionalAcquire
#define LWLockHeldByMe d3_clog_LWLockHeldByMe
#define LWLockInitialize d3_clog_LWLockInitialize
#define LWLockRelease d3_clog_LWLockRelease
#define MainLWLockArray d3_clog_MainLWLockArray
#define MyAuxProcType d3_clog_MyAuxProcType
#define MyBackendType d3_clog_MyBackendType
#define OpenTransientFile d3_clog_OpenTransientFile
#define PG_exception_stack d3_clog_PG_exception_stack
#define ProcessInterrupts d3_clog_ProcessInterrupts
#define ReadDir d3_clog_ReadDir
#define RegisterSyncRequest d3_clog_RegisterSyncRequest
#define ShmemInitStruct d3_clog_ShmemInitStruct
#define SimpleLruDoesPhysicalPageExist d3_clog_SimpleLruDoesPhysicalPageExist
#define SimpleLruInit d3_clog_SimpleLruInit
#define SimpleLruReadPage d3_clog_SimpleLruReadPage
#define SimpleLruReadPage_ReadOnly d3_clog_SimpleLruReadPage_ReadOnly
#define SimpleLruShmemSize d3_clog_SimpleLruShmemSize
#define SimpleLruTruncate d3_clog_SimpleLruTruncate
#define SimpleLruWriteAll d3_clog_SimpleLruWriteAll
#define SimpleLruWritePage d3_clog_SimpleLruWritePage
#define SimpleLruZeroPage d3_clog_SimpleLruZeroPage
#define SlruDeleteSegment d3_clog_SlruDeleteSegment
#define SlruScanDirCbDeleteAll d3_clog_SlruScanDirCbDeleteAll
#define SlruScanDirCbReportPresence d3_clog_SlruScanDirCbReportPresence
#define SlruScanDirectory d3_clog_SlruScanDirectory
#define SlruSyncFileTag d3_clog_SlruSyncFileTag
#define StartupCLOG d3_clog_StartupCLOG
#define TransactionIdFollowsOrEquals d3_clog_TransactionIdFollowsOrEquals
#define TransactionIdGetStatus d3_clog_TransactionIdGetStatus
#define TransactionIdPrecedes d3_clog_TransactionIdPrecedes
#define TransactionIdSetTreeStatus d3_clog_TransactionIdSetTreeStatus
#define TrimCLOG d3_clog_TrimCLOG
#define TruncateCLOG d3_clog_TruncateCLOG
#define XLogFlush d3_clog_XLogFlush
#define clog_redo d3_clog_clog_redo
#define clogsyncfiletag d3_clog_clogsyncfiletag
#define cluster_clog_native_recovery_apply_v1 d3_clog_cluster_clog_native_recovery_apply_v1
#define cluster_clog_native_recovery_base_read_v1 d3_clog_cluster_clog_native_recovery_base_read_v1
#define cluster_clog_native_recovery_base_read_held_v1                                             \
	d3_clog_cluster_clog_native_recovery_base_read_held_v1
#define cluster_clog_native_recovery_base_release_v1 d3_clog_cluster_clog_native_recovery_base_release_v1
#define cluster_clog_native_recovery_record_apply_v1                                               \
	d3_clog_cluster_clog_native_recovery_record_apply_v1
#define cluster_clog_native_recovery_record_verify_v1                                              \
	d3_clog_cluster_clog_native_recovery_record_verify_v1
#define cluster_clog_native_recovery_verify_v1 d3_clog_cluster_clog_native_recovery_verify_v1
#define cluster_clog_native_terminal_sync_v1 d3_clog_cluster_clog_native_terminal_sync_v1
#define cluster_node_id d3_clog_cluster_node_id
#define cluster_shared_config d3_clog_cluster_shared_config
#define cluster_shared_data_dir d3_clog_cluster_shared_data_dir
#define cluster_undo_recovery_abort_clog_authorized_v1                                             \
	d3_clog_cluster_undo_recovery_abort_clog_authorized_v1
#define cluster_undo_recovery_cold_origin_authorized_v1                                            \
	d3_clog_cluster_undo_recovery_cold_origin_authorized_v1
#define data_sync_elevel d3_clog_data_sync_elevel
#define enableFsync d3_clog_enableFsync
#define errcode d3_clog_errcode
#define errcode_for_file_access d3_clog_errcode_for_file_access
#define errdetail d3_clog_errdetail
#define errfinish d3_clog_errfinish
#define errmsg d3_clog_errmsg
#define errmsg_internal d3_clog_errmsg_internal
#define error_context_stack d3_clog_error_context_stack
#define errstart d3_clog_errstart
#define errstart_cold d3_clog_errstart_cold
#define fsync_fname d3_clog_fsync_fname
#define main d3_clog_main
#define my_wait_event_info d3_clog_my_wait_event_info
#define pg_file_create_mode d3_clog_pg_file_create_mode
#define pg_fsync d3_clog_pg_fsync
#define pg_re_throw d3_clog_pg_re_throw
#define pgstat_count_slru_flush d3_clog_pgstat_count_slru_flush
#define pgstat_count_slru_page_hit d3_clog_pgstat_count_slru_page_hit
#define pgstat_count_slru_page_read d3_clog_pgstat_count_slru_page_read
#define pgstat_count_slru_page_written d3_clog_pgstat_count_slru_page_written
#define pgstat_count_slru_page_zeroed d3_clog_pgstat_count_slru_page_zeroed
#define pgstat_count_slru_truncate d3_clog_pgstat_count_slru_truncate
#define pgstat_get_slru_index d3_clog_pgstat_get_slru_index
#define ut_current_failed d3_clog_ut_current_failed
#define ut_failed_count d3_clog_ut_failed_count
#define ut_test_count d3_clog_ut_test_count
#endif
