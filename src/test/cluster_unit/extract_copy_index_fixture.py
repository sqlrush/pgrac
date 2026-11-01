#!/usr/bin/env python3
"""Extract the original COPY flush and complete heap multi-insert consumer.

Copyright (c) 2026, pgrac contributors
Author: SqlRush <sqlrush@gmail.com>
"""
import pathlib
import re
import sys

from extract_vacuum_mx_fixture import function


if __name__ == "__main__":
    source = pathlib.Path(sys.argv[1]).read_text()
    if sys.argv[2] == "--copy":
        print(re.search(r"^#define MAX_BUFFERED_TUPLES[^\n]*", source, re.M)[0])
        for name in ("CopyMultiInsertBuffer", "CopyMultiInsertInfo"):
            print(re.search(r"^typedef struct " + name + r"\n\{.*?^\} "
                            + name + ";", source, re.M | re.S)[0])
        print(function(source, "CopyMultiInsertBufferFlush", "static inline void"))
    elif sys.argv[2] == "--heap":
        print(function(source, "cluster_heap_multi_insert_route",
                       "static ClusterHeapMultiInsertRoute"))
        print(function(source, "heap_multi_insert", "void"))
    else:
        raise ValueError("expected --copy or --heap")
