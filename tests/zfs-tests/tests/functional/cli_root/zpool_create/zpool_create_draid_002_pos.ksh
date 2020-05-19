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
# Create dRAID pool using the maximum number of vdevs (255).  Then verify
# that creating a pool with 256 fails as expected.
#
# STRATEGY:
# 1) Create one more than the maximum number of allowed dRAID vdevs (256).
# 2) Create a pool using the maximum allowed (255).
# 3) Create a pool using more than the maximum allowed (256).
#
# Note: The pool creation is limited to a single interation to speed up
# the test.  We only care that it can succeed.
#

verify_runnable "global"

function cleanup
{
	poolexists $TESTPOOL && destroy_pool $TESTPOOL

	rm -f $all_vdevs
	rmdir $TESTDIR
}

log_assert "'zpool create <pool> draid <255|256 vdevs>'"

log_onexit cleanup

all_vdevs=$(echo $TESTDIR/file.{01..256})

mkdir $TESTDIR
log_must truncate -s $MINVDEVSIZE $all_vdevs

# Maximum dRAID vdev count (255).
log_must zpool create $TESTPOOL draid:1i $(echo $TESTDIR/file.{01..255})
log_must poolexists $TESTPOOL
destroy_pool $TESTPOOL

# Exceeds maximum dRAID vdev count (256).
log_mustnot zpool create $TESTPOOL draid:1i $(echo $TESTDIR/file.{01..256})

log_pass "'zpool create <pool> draid <255|256 vdevs>'"
