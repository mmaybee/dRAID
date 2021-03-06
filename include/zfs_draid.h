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
#define	VDEV_DRAID_MAX_GROUPSIZE	32
#define	VDEV_DRAID_MAX_GROUPS		128
#define	VDEV_DRAID_MAX_SPARES		100

/*
 * When a specific number of groups was not provided create enough
 * groups such that each group contains no more than this many devices.
 */
#define	VDEV_DRAID_TGT_GROUPSIZE	12

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

typedef struct vdev_draid_config {
	uint64_t vdc_guid;
	uint64_t vdc_groups;
	uint64_t vdc_parity;
	uint64_t vdc_spares;
	uint64_t vdc_children;
	uint64_t vdc_bases;
	uint64_t *vdc_data;
	uint64_t *vdc_base_perms;
	long int vdc_seed;
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
	DRAIDCFG_ERR_BASE_MISSING,	/* base key/value is missing */
	DRAIDCFG_ERR_BASE_INVALID,	/* base value is invalid */
	DRAIDCFG_ERR_PERM_MISSING,	/* perm key/value is missing */
	DRAIDCFG_ERR_PERM_INVALID,	/* perm value is invalid */
	DRAIDCFG_ERR_PERM_MISMATCH,	/* perm value is inconsistent */
	DRAIDCFG_ERR_PERM_DUPLICATE,	/* perm value is a duplicate */
	DRAIDCFG_ERR_LAYOUT,		/* layout (n - s) != (d + p) */
	DRAIDCFG_ERR_INTERNAL,		/* internal error */
} draidcfg_err_t;

/*
 * Required in user and kernel space to validate a dRAID configuration.
 */
extern draidcfg_err_t vdev_draid_config_validate(nvlist_t *, uint64_t,
    uint64_t, uint64_t, uint64_t);
extern char *vdev_draid_name(char *, int, uint64_t, uint64_t, uint64_t,
    uint64_t);
extern char *vdev_draid_spare_name(char *, int, uint64_t, uint64_t, uint64_t,
    uint64_t, uint64_t);
extern int vdev_draid_spare_values(const char *, uint64_t *, uint64_t *,
    uint64_t *, uint64_t *, uint64_t *);
extern boolean_t vdev_draid_is_spare(const char *);

/*
 * Required by the zpool and ztest commands to create pool which uses dRAID.
 */
#ifndef _KERNEL

/*
 * Path to known dRAID configurations provided either by the upstream
 * OpenZFS project or a vendor.
 */
#define	DRAIDCFG_DEFAULT_DIR	"/etc/zfs/draid.d"
#define	DRAIDCFG_DEFAULT_FILE	"defaults"

extern draidcfg_err_t vdev_draid_config_generate(uint64_t, uint64_t, uint64_t,
    uint64_t, uint64_t, int, draidcfg_eval_t, int, nvlist_t **);
extern void vdev_draid_config_free(nvlist_t *);
extern int vdev_draid_config_read_file(const char *, char *, nvlist_t **);
extern int vdev_draid_config_write_file(const char *, char *, nvlist_t *);
extern void vdev_draid_config_print_error(draidcfg_err_t);
extern void vdev_draid_config_print(nvlist_t *);
#endif

#ifdef  __cplusplus
}
#endif

#endif /* _ZFS_DRAID_H */
