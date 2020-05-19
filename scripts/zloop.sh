#!/usr/bin/env bash

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
# Copyright (c) 2015 by Delphix. All rights reserved.
# Copyright (C) 2016 Lawrence Livermore National Security, LLC.
# Copyright (c) 2017, Intel Corporation.
#

# Turns on generation of draid groups of different sizes if non-zero
degenerate=1

BASE_DIR=$(dirname "$0")
SCRIPT_COMMON=common.sh
if [ -f "${BASE_DIR}/${SCRIPT_COMMON}" ]; then
	. "${BASE_DIR}/${SCRIPT_COMMON}"
else
	echo "Missing helper script ${SCRIPT_COMMON}" && exit 1
fi

# shellcheck disable=SC2034
PROG=zloop.sh
GDB=${GDB:-gdb}

DEFAULTWORKDIR=/var/tmp
DEFAULTCOREDIR=/var/tmp/zloop

function usage
{
	echo -e "\n$0 [-t <timeout>] [ -s <vdev size> ] [-c <dump directory>]" \
	    "[ -- [extra ztest parameters]]\n" \
	    "\n" \
	    "  This script runs ztest repeatedly with randomized arguments.\n" \
	    "  If a crash is encountered, the ztest logs, any associated\n" \
	    "  vdev files, and core file (if one exists) are moved to the\n" \
	    "  output directory ($DEFAULTCOREDIR by default). Any options\n" \
	    "  after the -- end-of-options marker will be passed to ztest.\n" \
	    "\n" \
	    "  Options:\n" \
	    "    -t  Total time to loop for, in seconds. If not provided,\n" \
	    "        zloop runs forever.\n" \
	    "    -s  Size of vdev devices.\n" \
	    "    -f  Specify working directory for ztest vdev files.\n" \
	    "    -c  Specify a core dump directory to use.\n" \
	    "    -m  Max number of core dumps to allow before exiting.\n" \
	    "    -l  Create 'ztest.core.N' symlink to core directory.\n" \
	    "    -h  Print this help message.\n" \
	    "" >&2
}

function or_die
{
	# shellcheck disable=SC2068
	$@
	# shellcheck disable=SC2181
	if [[ $? -ne 0 ]]; then
		# shellcheck disable=SC2145
		echo "Command failed: $@"
		exit 1
	fi
}

case $(uname) in
FreeBSD)
	coreglob="z*.core"
	;;
Linux)
	# core file helpers
	origcorepattern="$(cat /proc/sys/kernel/core_pattern)"
	coreglob="$(grep -E -o '^([^|%[:space:]]*)' /proc/sys/kernel/core_pattern)*"

	if [[ $coreglob = "*" ]]; then
		echo "Setting core file pattern..."
		echo "core" > /proc/sys/kernel/core_pattern
		coreglob="$(grep -E -o '^([^|%[:space:]]*)' \
		    /proc/sys/kernel/core_pattern)*"
	fi
	;;
*)
	exit 1
	;;
esac

function core_file
{
	# shellcheck disable=SC2012 disable=2086
        printf "%s" "$(ls -tr1 $coreglob 2> /dev/null | head -1)"
}

function core_prog
{
	prog=$ZTEST
	core_id=$($GDB --batch -c "$1" | grep "Core was generated by" | \
	    tr  \' ' ')
	# shellcheck disable=SC2076
	if [[ "$core_id" =~ "zdb "  ]]; then
		prog=$ZDB
	fi
	printf "%s" "$prog"
}

function store_core
{
	core="$(core_file)"
	if [[ $ztrc -ne 0 ]] || [[ -f "$core" ]]; then
		df -h "$workdir" >>ztest.out
		coreid=$(date "+zloop-%y%m%d-%H%M%S")
		foundcrashes=$((foundcrashes + 1))

		# zdb debugging
		zdbcmd="$ZDB -U "$workdir/zpool.cache" -dddMmDDG ztest"
		zdbdebug=$($zdbcmd 2>&1)
		echo -e "$zdbcmd\n" >>ztest.zdb
		echo "$zdbdebug" >>ztest.zdb

		dest=$coredir/$coreid
		or_die mkdir -p "$dest"
		or_die mkdir -p "$dest/vdev"

		if [[ $symlink -ne 0 ]]; then
			or_die ln -sf "$dest" ztest.core.$foundcrashes
		fi

		echo "*** ztest crash found - moving logs to $dest"

		or_die mv ztest.history "$dest/"
		or_die mv ztest.zdb "$dest/"
		or_die mv ztest.out "$dest/"
		or_die mv "$workdir/ztest*" "$dest/vdev/"

		if [[ -e "$workdir/zpool.cache" ]]; then
			or_die mv "$workdir/zpool.cache" "$dest/vdev/"
		fi

		# check for core
		if [[ -f "$core" ]]; then
			coreprog=$(core_prog "$core")
			coredebug=$($GDB --batch --quiet \
			    -ex "set print thread-events off" \
			    -ex "printf \"*\n* Backtrace \n*\n\"" \
			    -ex "bt" \
			    -ex "printf \"*\n* Libraries \n*\n\"" \
			    -ex "info sharedlib" \
			    -ex "printf \"*\n* Threads (full) \n*\n\"" \
			    -ex "info threads" \
			    -ex "printf \"*\n* Backtraces \n*\n\"" \
			    -ex "thread apply all bt" \
			    -ex "printf \"*\n* Backtraces (full) \n*\n\"" \
			    -ex "thread apply all bt full" \
			    -ex "quit" "$coreprog" "$core" 2>&1 | \
			    grep -v "New LWP")

			# Dump core + logs to stored directory
			echo "$coredebug" >>"$dest/ztest.gdb"
			or_die mv "$core" "$dest/"

			# Record info in cores logfile
			echo "*** core @ $coredir/$coreid/$core:" | \
			    tee -a ztest.cores
		fi

		if [[ $coremax -gt 0 ]] &&
		   [[ $foundcrashes -ge $coremax ]]; then
			echo "exiting... max $coremax allowed cores"
			exit 1
		else
			echo "continuing..."
		fi
	fi
}

# parse arguments
# expected format: zloop [-t timeout] [-c coredir] [-- extra ztest args]
coredir=$DEFAULTCOREDIR
basedir=$DEFAULTWORKDIR
rundir="zloop-run"
timeout=0
size="512m"
coremax=0
symlink=0
while getopts ":ht:m:s:c:f:l" opt; do
	case $opt in
		t ) [[ $OPTARG -gt 0 ]] && timeout=$OPTARG ;;
		m ) [[ $OPTARG -gt 0 ]] && coremax=$OPTARG ;;
		s ) [[ $OPTARG ]] && size=$OPTARG ;;
		c ) [[ $OPTARG ]] && coredir=$OPTARG ;;
		f ) [[ $OPTARG ]] && basedir=$(readlink -f "$OPTARG") ;;
		l ) symlink=1 ;;
		h ) usage
		    exit 2
		    ;;
		* ) echo "Invalid argument: -$OPTARG";
		    usage
		    exit 1
	esac
done
# pass remaining arguments on to ztest
shift $((OPTIND - 1))

# enable core dumps
ulimit -c unlimited
export ASAN_OPTIONS=abort_on_error=1:disable_coredump=0

if [[ -f "$(core_file)" ]]; then
	echo -n "There's a core dump here you might want to look at first... "
	core_file
	echo
	exit 1
fi

if [[ ! -d $coredir ]]; then
	echo "core dump directory ($coredir) does not exist, creating it."
	or_die mkdir -p "$coredir"
fi

if [[ ! -w $coredir ]]; then
	echo "core dump directory ($coredir) is not writable."
	exit 1
fi

or_die rm -f ztest.history
or_die rm -f ztest.zdb
or_die rm -f ztest.cores

ztrc=0		# ztest return value
foundcrashes=0	# number of crashes found so far
starttime=$(date +%s)
curtime=$starttime

# if no timeout was specified, loop forever.
while [[ $timeout -eq 0 ]] || [[ $curtime -le $((starttime + timeout)) ]]; do
	zopt="-L -VVVVV"

	# start each run with an empty directory
	workdir="$basedir/$rundir"
	or_die rm -rf "$workdir"
	or_die mkdir "$workdir"

	# switch between three types of configs
	# 25% basic, 25% raidz mix, and 50% draid mix
	choice=$((RANDOM % 4))

	# ashift range 9 - 15
	align=$(((RANDOM % 2) * 3 + 9))

	if [[ $choice -eq 0 ]]; then
		# basic mirror only
		mirrors=2
		raidz=0
		parity=1
		vdevs=2
		zopt="$zopt -K raidz"
	elif [[ $choice -eq 1 ]]; then
		# fully randomized mirror/raidz (sans dRAID)
		mirrors=$(((RANDOM % 3) * 1))
		parity=$(((RANDOM % 3) + 1))
		raidz=$((((RANDOM % 9) + parity + 1) * (RANDOM % 2)))
		vdevs=$(((RANDOM % 3) + 3))
		zopt="$zopt -K raidz"
	else
		# mix of draid fixed (one per parity) and fully random
		mirrors=0
		raidz=1
		align=12
		case $((RANDOM % 4)) in
		0 )	# draid1: 3 x (4 + 1) + 1 = 16 drives
			parity=1
			draid_data=4
			draid_groups=3
			draid_spares=1
			vdevs=2
			class="special=off"
			size=320m
			;;
		1 )	# draid2: 3 x (5 + 2) + 2 = 23 drives
			parity=2
			draid_data=5
			draid_groups=3
			draid_spares=2
			vdevs=0
			class="special=on"
			size=240m
			;;
		2 )	# draid3: 4 x (6 + 3) + 3 = 39 drives
			parity=3
			draid_data=6
			draid_groups=4
			draid_spares=3
			vdevs=0
			class="special=on"
			size=160m
			;;
		3 )	# dRAID with varying choices
			# parity: 1 --> 3
			# data: 3 --> 6
			# groups: 2 --> 6
			# spares: 0 --> parity + 1
			# yields max drives = 6 x (6 + 3) + 4 = 58 drives
			parity=$(((RANDOM % 3) + 1))
			draid_data=$(((RANDOM % 4) + 3 + parity))
			draid_groups=$(((RANDOM % 5) + 2))
			draid_spares=$(((RANDOM % 2) + parity))
			vdevs=$((RANDOM % 3))
			class="special=random"
			gbavail=$(df -B 1G "$workdir" | awk 'NR==2 {print $4}')
			# check if we have at least 60G availible
			if [[ $gbavail -gt 60 ]]; then
				# use size range 128MB - 1GB
				megabytes=$((((RANDOM % 8) + 1) * 128))
				size="${megabytes}m"
			else
				echo "limited storage on '$workdir'" >>ztest.out
				size=128m
			fi
		esac

		if [[ $degenerate -gt 0 ]]; then
			draid_extra=$((RANDOM % draid_data))
		else
			draid_extra=0
		fi
		draid_vdevs=$(((draid_data + parity) * draid_groups +
		    draid_spares + draid_extra))
		zopt="$zopt -K draid"
		zopt="$zopt -D $draid_vdevs"
		zopt="$zopt -G $draid_groups"
		zopt="$zopt -S $draid_spares"
		zopt="$zopt -C $class"
	fi

	# run from 30 to 120 seconds
	runtime=$(((RANDOM % 90) + 30))
	passtime=$((RANDOM % (runtime / 3 + 1) + 10))

	zopt="$zopt -m $mirrors"
	zopt="$zopt -r $raidz"
	zopt="$zopt -R $parity"
	zopt="$zopt -v $vdevs"
	zopt="$zopt -a $align"
	zopt="$zopt -T $runtime"
	zopt="$zopt -P $passtime"
	zopt="$zopt -s $size"
	zopt="$zopt -f $workdir"

	# shellcheck disable=SC2124
	cmd="$ZTEST $zopt $@"
	desc="$(date '+%m/%d %T') $cmd"
	echo "$desc" | tee -a ztest.history
	echo "$desc" >>ztest.out
	$cmd >>ztest.out 2>&1
	ztrc=$?
	grep -E '===|WARNING' ztest.out >>ztest.history

	store_core

	curtime=$(date +%s)
done

echo "zloop finished, $foundcrashes crashes found"

# restore core pattern.
case $(uname) in
Linux)
	echo "$origcorepattern" > /proc/sys/kernel/core_pattern
	;;
*)
	;;
esac

uptime >>ztest.out

if [[ $foundcrashes -gt 0 ]]; then
	exit 1
fi
