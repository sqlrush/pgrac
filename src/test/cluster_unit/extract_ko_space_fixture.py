#!/usr/bin/env python3
"""Keep KO peer dispatch connected to the real SPACE readers and S identity gate.
Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import sys
from extract_vacuum_mx_fixture import function
root = pathlib.Path(sys.argv[1])
space = (root / "cluster/cluster_space_storage.c").read_text()
for name, result in [("space_namespace", "static bool"),
                     ("space_read_identity_buffer", "static Buffer"),
                     ("space_read_locked_identity", "static bool"),
                     ("space_read_identity", "static bool"),
                     ("cluster_space_relation_read_identity", "bool"),
                     ("cluster_space_relation_read_maintenance_identity", "bool")]:
    print(function(space, name, result))
pcm = function((root / "cluster/cluster_pcm_lock.c").read_text(),
               "cluster_pcm_lock_acquire_buffer", "bool")
start = pcm.index("\tif (MyBackendId == InvalidBackendId)")
end = pcm.index("\n\ttag = buf->tag;", start)
print("static void\nfixture_pcm_backend_gate(void)\n{")
print(pcm[start:end])
print("}")
