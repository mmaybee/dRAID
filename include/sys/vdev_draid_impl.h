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
 * Copyright (c) 2016, Intel Corporation.
 * Copyright (c) 2020 by Lawrence Livermore National Security, LLC.
 */

#ifndef _VDEV_DRAID_IMPL_H
#define	_VDEV_DRAID_IMPL_H

#include <sys/types.h>
#include <sys/abd.h>
#include <sys/nvpair.h>
#include <sys/zio.h>
#include <sys/vdev_impl.h>
#include <sys/vdev_raidz_impl.h>
#include <zfs_draid.h>

#ifdef  __cplusplus
extern "C" {
#endif

extern boolean_t vdev_draid_group_degraded(vdev_t *, uint64_t, uint64_t,
    uint64_t);
extern uint64_t vdev_draid_check_block(vdev_t *, uint64_t, uint64_t *);
extern uint64_t vdev_draid_get_astart(vdev_t *, uint64_t);
extern uint64_t vdev_draid_offset_to_group(vdev_t *, uint64_t);
extern uint64_t vdev_draid_group_to_offset(vdev_t *, uint64_t);
extern boolean_t vdev_draid_readable(vdev_t *, uint64_t);
extern boolean_t vdev_draid_is_dead(vdev_t *, uint64_t);
extern boolean_t vdev_draid_missing(vdev_t *, uint64_t, uint64_t, uint64_t);
extern uint64_t vdev_draid_asize_to_psize(vdev_t *, uint64_t, uint64_t);
extern uint64_t vdev_draid_max_rebuildable_asize(vdev_t *, uint64_t);
extern void vdev_draid_map_include_skip_sectors(zio_t *);
extern nvlist_t *vdev_draid_read_config_spare(vdev_t *);

extern vdev_t *vdev_draid_spare_get_parent(vdev_t *);
extern boolean_t vdev_draid_spare_is_active(vdev_t *);

#ifdef  __cplusplus
}
#endif

#endif /* _VDEV_DRAID_IMPL_H */
