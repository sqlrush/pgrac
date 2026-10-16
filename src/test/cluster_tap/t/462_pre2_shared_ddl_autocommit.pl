#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC AM05: shared DROP/TRUNCATE are top-level-only operations.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use SharedDDLAutocommit;
run_shared_ddl_autocommit_case();
