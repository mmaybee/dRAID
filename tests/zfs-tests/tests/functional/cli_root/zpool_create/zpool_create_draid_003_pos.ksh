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
# Verify allowed striped widths (data+parity) and hot spares may be
# configured at pool creation time.
#
# STRATEGY:
# 1) Test valid stripe/spare combinations given the number of children.
# 2) Test invalid stripe/spare combinations outside the allow limits.
#

verify_runnable "global"

function cleanup
{
	poolexists $TESTPOOL && destroy_pool $TESTPOOL

	rm -f $draid_vdevs
	rmdir $TESTDIR
}

log_assert "'zpool create <pool> draid:#d:#s <vdevs>'"

log_onexit cleanup

parity=1
children=32
draid_vdevs=$(echo $TESTDIR/file.{01..$children})

mkdir $TESTDIR
log_must truncate -s $MINVDEVSIZE $draid_vdevs

# Request from 1-3 hot spares.
for s in {1..3}; do
	max_data=$((children - s - parity))

	# Request all valid stripe widths up to the maximum.
	# Iterations are set to 1 solely to speed up the test.
	for d in {1..$max_data}; do
		log_must zpool create $TESTPOOL \
		    draid:${d}d:${s}s:1i $draid_vdevs
		log_must poolexists $TESTPOOL
		destroy_pool $TESTPOOL
	done
done

# Exceeds maximum pairty (3).
log_mustnot zpool create $TESTPOOL draid4 $draid_vdevs

# Exceeds maximum data disks (limited by total children)
log_mustnot zpool create $TESTPOOL draid2:30d $draid_vdevs

# Spares may not exceed (children - (parity + data))).
log_must zpool create $TESTPOOL draid2:20d:10s $draid_vdevs
log_must poolexists $TESTPOOL
destroy_pool $TESTPOOL
log_mustnot zpool create $TESTPOOL draid2:20d:11s $draid_vdevs

# At least one data or spare disk must be requested.
log_mustnot zpool create $TESTPOOL draid2:0d $draid_vdevs
log_mustnot zpool create $TESTPOOL draid2:0s $draid_vdevs
log_mustnot zpool create $TESTPOOL draid2:0s:0d $draid_vdevs

log_pass "'zpool create <pool> draid:#d:#s <vdevs>'"
