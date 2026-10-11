#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: shared catalog writes must reject subtransactions before receipts.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use SubtransactionGuard;
run_catalog_guard_case();
