#!/usr/bin/env perl
# Copyright (c) 2026, pgrac contributors
# Author: SqlRush <sqlrush@gmail.com>
# PGRAC: shared DML rejects subtransactions before preparing receipts.
use strict;
use warnings;
use FindBin;
use lib "$FindBin::RealBin/../../perl", "$FindBin::RealBin/../pre2";
use SubtransactionGuard;
run_guard_case(0);
