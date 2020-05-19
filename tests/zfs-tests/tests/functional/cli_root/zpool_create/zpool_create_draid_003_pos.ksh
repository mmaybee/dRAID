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
# Verify a specific number of dRAID groups and hot spares may be
# configured at pool creation time.
#
# STRATEGY:
# 1) Test valid group/spare combinations given the number of children.
# 2) Test invalid group/spare combinations outside the allow limits.
#

verify_runnable "global"

function cleanup
{
	poolexists $TESTPOOL && destroy_pool $TESTPOOL

	rm -f $all_vdevs
	rmdir $TESTDIR
}

log_assert "'zpool create <pool> draid:#g:#s <vdevs>'"

log_onexit cleanup

all_vdevs=$(echo $TESTDIR/file.{01..33})
draid_vdevs=$(echo $TESTDIR/file.{01..32})

mkdir $TESTDIR
log_must truncate -s $MINVDEVSIZE $all_vdevs

# Request from 1-10 groups. (32 wide to 3 wide stripes).
for g in {1..10}; do
	# Request from 1-3 hot spares.
	for s in {1..3}; do
		log_must zpool create $TESTPOOL draid:${g}g:${s}s $draid_vdevs
		log_must poolexists $TESTPOOL
		destroy_pool $TESTPOOL
	done
done

# Exceeds maximum pairty (3).
log_mustnot zpool create $TESTPOOL draid4 $draid_vdevs

# Exceeds maximum group size (32).
log_mustnot zpool create $TESTPOOL draid2:1g $all_vdevs

# Below minimum group size (parity + 1).
log_mustnot zpool create $TESTPOOL draid2:11g $draid_vdevs

# Spares may not exceed (children - (groups * (parity + data))).
log_must zpool create $TESTPOOL draid2:2g:25s $draid_vdevs
log_must poolexists $TESTPOOL
destroy_pool $TESTPOOL
log_mustnot zpool create $TESTPOOL draid2:26s $draid_vdevs

# At least one group or spare must be requested.
log_mustnot zpool create $TESTPOOL draid2:0g $draid_vdevs
log_mustnot zpool create $TESTPOOL draid2:0s $draid_vdevs
log_mustnot zpool create $TESTPOOL draid2:0s:0g $draid_vdevs

log_pass "'zpool create <pool> draid:#g:#s <vdevs>'"
