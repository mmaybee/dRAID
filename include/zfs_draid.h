/*
 * CDDL HEADER START
 *
 * The contents of this file are subject to the terms of the
 * Common Development and Distribution License (the "License").
 * You may not use this file except in compliance with the License.
 *
 * You can obtain a copy of the license at usr/src/OPENSOLARIS.LICENSE
 * or http://www.opensolaris.org/os/licensing.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * When distributing Covered Code, include this CDDL HEADER in each
 * file and include the License file at usr/src/OPENSOLARIS.LICENSE.
 * If applicable, add the following below this CDDL HEADER, with the
 * fields enclosed by brackets "[]" replaced with your own identifying
 * information: Portions Copyright [yyyy] [name of copyright owner]
 *
 * CDDL HEADER END
 */
/*
 * Copyright (c) 2018, Intel Corporation.
 * Copyright (c) 2020 by Lawrence Livermore National Security, LLC.
 */

#ifndef _ZFS_DRAID_H
#define	_ZFS_DRAID_H

#include <sys/types.h>
#include <sys/nvpair.h>
#include <sys/vdev.h>

#ifdef  __cplusplus
extern "C" {
#endif

/*
 * The maximum number of children per dRAID is limited by the on-disk
 * format to a uint8_t to minimize the space required to store the
 * permutation groups.
 */
#define	VDEV_DRAID_MAX_CHILDREN		UINT8_MAX
#define	VDEV_DRAID_MAX_PARITY		3
#define	VDEV_DRAID_MAX_ROWS		32768

/*
 * These values apply reasonable limits to a dRAID configuration.
 */
#define	VDEV_DRAID_MAX_GROUPSIZE	128
#define	VDEV_DRAID_MAX_GROUPS		VDEV_DRAID_MAX_CHILDREN
#define	VDEV_DRAID_MAX_SPARES		100

/*
 * When a specific number of data disks is not provided limit a redundancy
 * group to 8 data disks.  This value was selected to provide a reasonable
 * tradeoff between capacity and performance.
 */
#define	VDEV_DRAID_TGT_DATA		8

/*
 * The default number of passes when developing a new dRAID configuration.
 * Larger values will result is more evenly distributed maps but will
 * increase the time required to create a new pool.  A minimum of 3
 * iterations are done to prevent accidentally generating a particularly
 * unbalanced configuration.
 */
#define	DRAIDCFG_DEFAULT_PASSES		3

/*
 * Strategy for evaluating the quality of a permutation group mapping
 * when generating random permutations.  DRAIDCFG_EVAL_WORST is the
 * default strategy and should normally be used.  Additional, strategies
 * are provided which may be useful for custom configurations.
 *
 * DRAIDCFG_EVAL_WORST - Avoid worst possible case drive failure scenarios,
 * this strategy is insensitive to any given failure.
 *
 * DRAIDCFG_EVAL_MEAN - Average over all possible drive failures, this
 * strategy is more sensitive to all possible failures (unused).
 *
 * DRAIDCFG_EVAL_RMS - Root mean square over all possible drive
 * failures penalizes higher imbalance more (unused).
 */
typedef enum {
	DRAIDCFG_EVAL_UNOPTIMIZED = 0,
	DRAIDCFG_EVAL_WORST,
	DRAIDCFG_EVAL_MEAN,
	DRAIDCFG_EVAL_RMS,
} draidcfg_eval_t;

#define	DRAIDCFG_DEFAULT_EVAL		DRAIDCFG_EVAL_WORST

/*
 * When evaluating the permutation group mapping the number of faulted
 * devices which are rebuilt to the distributed spares to consider.
 * Only single and double fault evaluations are currently supported.
 */
#define	DRAIDCFG_DEFAULT_FAULTS		1

/* Child vdev dRAID slice size */
#define	DRAID_ROWSHIFT			SPA_MAXBLOCKSHIFT
#define	DRAID_ROWSIZE			(1ULL << DRAID_ROWSHIFT)

/*
 * dRAID configuration.
 */
typedef struct vdev_draid_config {
	/*
	 * Values read from the ZPOOL_CONFIG_DRAIDCFG nvlist.
	 */
	uint64_t vdc_guid;		/* unique identifier */
	uint64_t vdc_seed;		/* seed which generated permutations */
	uint64_t vdc_scores[3];		/* permutation scores worst/mean/rms */
	uint64_t vdc_ndata;		/* # of data devices in group */
	uint64_t vdc_nparity;		/* # of parity devices in group */
	uint64_t vdc_nspares;		/* # of distributed spares */
	uint64_t vdc_children;		/* # of children */
	uint64_t vdc_ngroups;		/* # groups per slice */
	uint64_t vdc_nperms;		/* # of rows in permutations */
	uint64_t *vdc_perms;		/* permutation array */

	/*
	 * Immutable derived constants.
	 */
	uint64_t vdc_groupwidth;	/* = data + parity */
	uint64_t vdc_ndisks;		/* = children - spares */
	uint64_t vdc_groupsz;		/* = groupwidth * DRAID_ROWSIZE */
	uint64_t vdc_devslicesz;	/* = (groupsz * groups) / ndisks */
} vdev_draid_config_t;


/*
 * Errors which may be returned when validating a dRAID configuration.
 *
 * - MISSING indicates the key/value pair does not exist,
 * - INVALID indicates the value falls outside the allow range,
 * - MISMATCH indicates the value is in some way inconsistent with other
 *   configuration values, or if provided the top-level dRAID vdev.
 */
typedef enum {
	DRAIDCFG_OK = 0,		/* valid configuration */
	DRAIDCFG_ERR_CHILDREN_MISSING,	/* children key/value is missing */
	DRAIDCFG_ERR_CHILDREN_INVALID,	/* children value is invalid */
	DRAIDCFG_ERR_CHILDREN_MISMATCH,	/* children value is inconsistent */
	DRAIDCFG_ERR_PARITY_MISSING,	/* parity key/value is missing */
	DRAIDCFG_ERR_PARITY_INVALID,	/* parity value is invalid */
	DRAIDCFG_ERR_PARITY_MISMATCH,	/* parity value is inconsistent */
	DRAIDCFG_ERR_GROUPS_MISSING,	/* groups key/value is missing */
	DRAIDCFG_ERR_GROUPS_INVALID,	/* groups value is invalid */
	DRAIDCFG_ERR_GROUPS_MISMATCH,	/* groups value is inconsistent */
	DRAIDCFG_ERR_SPARES_MISSING,	/* spares key/value is missing */
	DRAIDCFG_ERR_SPARES_INVALID,	/* spares value is invalid */
	DRAIDCFG_ERR_SPARES_MISMATCH,	/* spares value is inconsistent */
	DRAIDCFG_ERR_DATA_MISSING,	/* data key/value is missing */
	DRAIDCFG_ERR_DATA_INVALID,	/* data value is invalid */
	DRAIDCFG_ERR_DATA_MISMATCH,	/* data value is inconsistent */
	DRAIDCFG_ERR_NPERMS_MISSING,	/* base key/value is missing */
	DRAIDCFG_ERR_NPERMS_INVALID,	/* base value is invalid */
	DRAIDCFG_ERR_PERMS_MISSING,	/* perm key/value is missing */
	DRAIDCFG_ERR_PERMS_INVALID,	/* perm value is invalid */
	DRAIDCFG_ERR_PERMS_MISMATCH,	/* perm value is inconsistent */
	DRAIDCFG_ERR_PERMS_DUPLICATE,	/* perm value is a duplicate */
	DRAIDCFG_ERR_LAYOUT,		/* groups entirely fill slice */
	DRAIDCFG_ERR_INTERNAL,		/* internal error */
} draidcfg_err_t;

/*
 * Required in user and kernel space to validate a dRAID configuration.
 */
extern draidcfg_err_t vdev_draid_config_validate(nvlist_t *, uint64_t,
    uint64_t, uint64_t, uint64_t);
extern char *vdev_draid_name(char *, int, uint64_t, uint64_t, uint64_t,
    uint64_t);
extern char *vdev_draid_name_nv(char *, int, nvlist_t *);
extern char *vdev_draid_spare_name(char *, int, uint64_t, uint64_t, uint64_t);
extern int vdev_draid_spare_values(const char *, uint64_t *, uint64_t *,
    uint64_t *);
extern boolean_t vdev_draid_is_spare(const char *);

/*
 * Required by the zpool and ztest commands to create pool which uses dRAID.
 */
#ifndef _KERNEL

/*
 * Path to known dRAID configurations provided either by the upstream
 * OpenZFS project or a vendor.
 */
#define	DRAIDCFG_DRYRUN_DIR	"/var/tmp"
#define	DRAIDCFG_DEFAULT_DIR	"/etc/zfs/draid.d"
#define	DRAIDCFG_DEFAULT_FILE	"defaults"

extern draidcfg_err_t vdev_draid_config_generate(uint64_t, uint64_t, uint64_t,
    uint64_t, int, draidcfg_eval_t, int, nvlist_t **);
extern void vdev_draid_config_free(nvlist_t *);
extern int vdev_draid_config_read_file(const char *, char *, nvlist_t **);
extern int vdev_draid_config_write_file(const char *, char *, nvlist_t *,
    boolean_t);
extern void vdev_draid_config_print_error(draidcfg_err_t);
extern void vdev_draid_config_print(nvlist_t *);
#endif

#ifdef  __cplusplus
}
#endif

#endif /* _ZFS_DRAID_H */
