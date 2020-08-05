#!/bin/ksh -p

#
# CDDL HEADER START
#
# This file and its contents are supplied under the terms of the
# Common Development and Distribution License ("CDDL"), version 1.0.
# You may only use this file in accordance with the terms of version
# 1.0 of the CDDL.
#
# A full copy of the text of the CDDL should have accompanied this
# source.  A copy of the CDDL is also available via the Internet at
# http://www.illumos.org/license/CDDL.
#
# CDDL HEADER END
#

#
# Copyright (c) 2019, Datto Inc. All rights reserved.
# Copyright (c) 2020 by Lawrence Livermore National Security, LLC.
#

. $STF_SUITE/include/libtest.shlib
. $STF_SUITE/tests/functional/redundancy/redundancy.kshlib

#
# DESCRIPTION:
# Verify resilver to dRAID distributed spares.
#
# STRATEGY:
# 1. For resilvers:
#    a. Create a semi-random dRAID pool configuration which can:
#       - sustain N failures (1-3), and
#       - has N distributed spares to replace all faulted vdevs
#    b. Fill the pool with data
#    c. Systematically fault a vdev, then replace it with a spare
#    d. Scrub the pool to verify no data was lost
#    e. Verify the contents of files in the pool
#

log_assert "Verify resilver to dRAID distributed spares"

log_onexit cleanup

for replace_mode in "healing" "sequential"; do

	if [[ "$replace_mode" = "sequential" ]]; then
		flags="-s"
	else
		flags=""
	fi

	#
	# XXX - Checksum errors may still occur when rebuilding to the
	# second (or later) distibuted spare.  When only a single distributed
	# spare is available checksum errors are never observed.
	#
	parity=$(random_int_between 1 3)
	spares=$(random_int_between $parity 3)
	groups=$(random_int_between 1 3)
	draid="draid${parity}:${groups}g:${spares}s"

	(( min_children = (groups * (parity + 1)) + spares ))

	typeset -i cnt=$(random_int_between $min_children 20)

	setup_test_env $TESTPOOL $draid $cnt

	i=0
	while [[ $i -lt $spares ]]; do
		fault_vdev="$BASEDIR/vdev$i"
		spare_vdev="s${i}-${draid}-0"
		log_must zpool offline -f $TESTPOOL $fault_vdev
		log_must zpool replace -w $flags $TESTPOOL \
		    $fault_vdev $spare_vdev
		log_must zpool detach $TESTPOOL $fault_vdev
		is_pool_scrubbing $TESTPOOL && wait_scrubbed $TESTPOOL

		(( i += 1 ))
	done

	log_must is_data_valid $TESTPOOL

	verify_pool $TESTPOOL
	log_must check_pool_status $TESTPOOL "scan" "repaired 0B"
	log_must check_pool_status $TESTPOOL "scan" "with 0 errors"

	cleanup
done

log_pass "Verify resilver to dRAID distributed spares"
