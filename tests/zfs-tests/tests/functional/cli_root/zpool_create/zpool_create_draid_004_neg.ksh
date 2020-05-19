#!/bin/ksh -p
#
# CDDL HEADER START
#
# The contents of this file are subject to the terms of the
# Common Development and Distribution License (the "License").
# You may not use this file except in compliance with the License.
#
# You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
# or http://www.opensolaris.org/os/licensing.
# See the License for the specific language governing permissions
# and limitations under the License.
#
# When distributing Covered Code, include this CDDL HEADER in each
# file and include the License file at usr/src/OPENSOLARIS.LICENSE.
# If applicable, add the following below this CDDL HEADER, with the
# fields enclosed by brackets "[]" replaced with your own identifying
# information: Portions Copyright [yyyy] [name of copyright owner]
#
# CDDL HEADER END
#

#
# Copyright (c) 2020 Lawrence Livermore National Security, LLC.

. $STF_SUITE/include/libtest.shlib

#
# DESCRIPTION:
# Verify that when specifying a number of groups and the number of data
# vdevs in those groups only valid geometries are accepted.  The following
# expression must be satisfied:
#
#    (children - spares) % ((parity + data) * groups) == 0
#
# STRATEGY:
# 1) Test a set of valid layouts which must succeed.
# 2) Test a set of invalid layouts which must fail.
#
# Note all of the configurations tested below happen to use 32 drive.
# There is no such constraint with dRAID, it's solely done as a small
# convenience when writing these tests.
#

verify_runnable "global"

function cleanup
{
	poolexists $TESTPOOL && destroy_pool $TESTPOOL

	rm -f $all_vdevs
	rmdir $TESTDIR
}

log_assert "'zpool create <pool> draid:#g:#d:#s <vdevs>'"

log_onexit cleanup

all_vdevs=$(echo $TESTDIR/file.{01..32})

mkdir $TESTDIR
log_must truncate -s $MINVDEVSIZE $all_vdevs

# Valid: (32 - 2) % ((1 + 4) * 6) = 0
log_must zpool create $TESTPOOL draid1:4d:6g:2s $all_vdevs
poolexists $TESTPOOL && destroy_pool $TESTPOOL

# Valid: (32 - 2) % ((2 + 8) * 3) = 0
log_must zpool create $TESTPOOL draid2:8d:3g:2s $all_vdevs
poolexists $TESTPOOL && destroy_pool $TESTPOOL

# Valid: (32 - 4) % ((3 + 11) * 2) = 0
log_must zpool create $TESTPOOL draid3:11d:2g:4s $all_vdevs
poolexists $TESTPOOL && destroy_pool $TESTPOOL

# Valid: (32 - 2) % ((1 + 2) * 10) = 0
log_must zpool create $TESTPOOL draid:2d:10g:2s $all_vdevs
poolexists $TESTPOOL && destroy_pool $TESTPOOL

# Invalid: (32 - 1) % ((1 + 6) * 4) != 0
log_mustnot zpool create $TESTPOOL draid1:6d:4g:1s $all_vdevs

# Invalid: (32 - 2) % ((2 + 2) * 8) != 0
log_mustnot zpool create $TESTPOOL draid2:2d:8g:2s $all_vdevs

# Invalid: (32 - 1) % ((3 + 7) * 3) != 0
log_mustnot zpool create $TESTPOOL draid3:7d:3g:1s $all_vdevs

log_pass "'zpool create <pool> draid:#g:#d:#s <vdevs>'"
