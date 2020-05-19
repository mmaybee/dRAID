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
 * Copyright (c) 2018 Intel Corporation.
 * Copyright (c) 2020 by Lawrence Livermore National Security, LLC.
 */

#include <sys/zfs_context.h>
#include <sys/spa.h>
#include <sys/spa_impl.h>
#include <sys/vdev_impl.h>
#include <sys/vdev_draid_impl.h>
#include <sys/vdev_raidz.h>
#include <sys/vdev_rebuild.h>
#include <sys/abd.h>
#include <sys/zio.h>
#include <sys/nvpair.h>
#include <sys/zio_checksum.h>
#include <sys/fs/zfs.h>
#include <sys/fm/fs/zfs.h>

uint64_t vdev_draid_asize_by_type(const vdev_t *, uint64_t, uint64_t);

static void
vdev_draid_debug_map(raidz_map_t *rm)
{
	for (int c = 0; rm != NULL && c < rm->rm_scols; c++) {
		char t = 'D';
		raidz_col_t *rc = &rm->rm_col[c];
		vdev_t *cvd = rm->rm_vdev->vdev_child[rc->rc_devidx];

		if (c >= rm->rm_cols) {
			t = 'S';
		} else if (c < rm->rm_firstdatacol) {
			switch (c) {
			case 0:
				t = 'P';
				break;
			case 1:
				t = 'Q';
				break;
			case 2:
				t = 'R';
				break;
			default:
				ASSERT0(c);
			}
		}

		zfs_dbgmsg("%c: dev %llu (%s) off %llu, sz %llu, "
		    "err %d, skipped %d, tried %d", t, rc->rc_devidx,
		    cvd->vdev_path != NULL ? cvd->vdev_path : "NA",
		    rc->rc_offset, rc->rc_size,
		    rc->rc_error, rc->rc_skipped, rc->rc_tried);
	}
}

void
vdev_draid_debug_zio(zio_t *zio)
{
	if (zfs_flags & ZFS_DEBUG_DRAID) {
		zfs_dbgmsg("dRAID zio: off %llu sz %llu data %px",
		    zio->io_offset, zio->io_size, zio->io_abd);
		vdev_draid_debug_map(zio->io_vsd);
	}
}

/* A child vdev is divided into slices */
static unsigned int slice_shift = 0;
#define	DRAID_SLICESHIFT (SPA_MAXBLOCKSHIFT + slice_shift)
/* 2 ** slice_shift * SPA_MAXBLOCKSIZE */
#define	DRAID_SLICESIZE  (1ULL << DRAID_SLICESHIFT)
#define	DRAID_SLICEMASK  (DRAID_SLICESIZE - 1)

static void
vdev_draid_get_permutation(uint64_t *p, uint64_t nr,
    const vdev_draid_config_t *vdc)
{
	uint64_t ncols = vdc->vdc_children;
	uint64_t off = nr % (vdc->vdc_bases * ncols);
	uint64_t base = off / ncols;
	uint64_t dev = off % ncols;

	for (uint64_t i = 0; i < ncols; i++) {
		const uint64_t *base_perm = vdc->vdc_base_perms +
		    (base * ncols);

		p[i] = (base_perm[i] + dev) % ncols;
	}
}

noinline static raidz_map_t *
vdev_draid_map_alloc(zio_t *zio, uint64_t **array)
{
	vdev_t *vd = zio->io_vd;
	const vdev_draid_config_t *vdc = vd->vdev_tsd;
	const uint64_t unit_shift = vd->vdev_top->vdev_ashift;
	const uint64_t ngroups = vdc->vdc_groups;
	const uint64_t nparity = vdc->vdc_parity;
	const uint64_t nspare = vdc->vdc_spares;
	const uint64_t ncols = vdc->vdc_children;
	/* The starting dRAID (parent) vdev sector of the block. */
	const uint64_t b = zio->io_offset >> unit_shift;
	/* The zio's size in units of the vdev's minimum sector size. */
	const uint64_t psize = zio->io_size >> unit_shift;
	const uint64_t slice = DRAID_SLICESIZE >> unit_shift;
	uint64_t o, q, r, c, bc, acols, asize, tot, ndata = 0;
	uint64_t perm, group, offset, groupsz = 0, groupstart, abd_off;
	raidz_map_t *rm;
	uint64_t *permutation;

	ASSERT0(P2PHASE(DRAID_SLICESIZE, 1ULL << unit_shift));

	/* HH: may not actually need the nspare columns for normal IO */
	permutation = kmem_alloc(sizeof (permutation[0]) * ncols, KM_SLEEP);

	perm = b / ((ncols - nspare) * slice);
	offset = b % ((ncols - nspare) * slice);
	/* Figure out in which group the IO will fall */
	for (group = 0, groupstart = 0; group < ngroups; group++) {
		ndata = vdc->vdc_data[group];
		groupsz = ndata + nparity;
		uint64_t span = groupsz * slice;
		if (offset < span)
			break;
		offset -= span;
		groupstart += groupsz;
	}
	ASSERT3U(group, <, ngroups);
	ASSERT3U(group, ==,
	    vdev_draid_offset_to_group(vd, zio->io_offset) % ngroups);

	/* offset is now a sector offset within this group chunk */
	ASSERT0(offset % groupsz);
	ASSERT3U(groupstart + groupsz, <=, ncols - nspare);

	/* The starting byte offset on each child vdev. */
	o = (perm * slice + offset / groupsz) << unit_shift;

	/*
	 * "Quotient": The number of data sectors for this stripe on all but
	 * the "big column" child vdevs that also contain "remainder" data.
	 */
	q = psize / ndata;

	/*
	 * "Remainder": The number of partial stripe data sectors in this I/O.
	 * This will add a sector to some, but not all, child vdevs.
	 */
	r = psize - q * ndata;

	/* The number of "big columns" - those which contain remainder data. */
	bc = (r == 0 ? 0 : r + nparity);
	ASSERT3U(bc, <, groupsz);

	/*
	 * The total number of data and parity sectors associated with
	 * this I/O.
	 */
	tot = psize + (nparity * (q + (r == 0 ? 0 : 1)));

	/* acols: The columns that will be accessed. */
	if (q == 0) {
		/* Our I/O request doesn't span all child vdevs. */
		acols = bc;
	} else {
		acols = groupsz;
	}

	rm = kmem_alloc(offsetof(raidz_map_t, rm_col[groupsz]), KM_SLEEP);
	rm->rm_cols = acols;
	rm->rm_scols = groupsz;
	rm->rm_bigcols = bc;
	rm->rm_skipstart = bc;
	rm->rm_missingdata = 0;
	rm->rm_missingparity = 0;
	rm->rm_firstdatacol = nparity;
	rm->rm_abd_copy = NULL;
	rm->rm_reports = 0;
	rm->rm_freed = 0;
	rm->rm_ecksuminjected = 0;
	rm->rm_vdev = vd;

	vdev_draid_get_permutation(permutation, perm, vdc);

	for (c = 0, asize = 0; c < groupsz; c++) {
		uint64_t i = groupstart + c;

		ASSERT3U(i, <, ncols - nspare);

		rm->rm_col[c].rc_devidx = permutation[i];
		rm->rm_col[c].rc_offset = o;
		rm->rm_col[c].rc_abd = NULL;
		rm->rm_col[c].rc_gdata = NULL;
		rm->rm_col[c].rc_error = 0;
		rm->rm_col[c].rc_tried = 0;
		rm->rm_col[c].rc_skipped = 0;

		if (c >= acols)
			rm->rm_col[c].rc_size = 0;
		else if (c < bc)
			rm->rm_col[c].rc_size = (q + 1) << unit_shift;
		else
			rm->rm_col[c].rc_size = q << unit_shift;

		asize += rm->rm_col[c].rc_size;
	}

	ASSERT3U(asize, ==, tot << unit_shift);
	rm->rm_asize = roundup(asize, groupsz << unit_shift);
	rm->rm_nskip = roundup(tot, groupsz) - tot;
	ASSERT3U(rm->rm_asize - asize, ==, rm->rm_nskip << unit_shift);
	ASSERT3U(rm->rm_nskip, <, ndata);

	/* allocate a buffer if we have a rounding gap */
	if (rm->rm_nskip == 0 ||
	    (zio->io_flags & (ZIO_FLAG_SCRUB | ZIO_FLAG_RESILVER)) == 0)
		rm->rm_abd_skip = NULL;
	else
		rm->rm_abd_skip =
		    abd_alloc_linear(rm->rm_nskip << unit_shift, B_TRUE);

	/* allocate buffers for parity */
	for (c = 0; c < rm->rm_firstdatacol; c++)
		rm->rm_col[c].rc_abd =
		    abd_alloc_linear(rm->rm_col[c].rc_size, B_TRUE);

	/* create buffers for the data chunks */
	abd_off = 0;
	ASSERT3U(c, <, acols);
	for (; c < acols; c++) {
		rm->rm_col[c].rc_abd = abd_get_offset_size(zio->io_abd,
		    abd_off, rm->rm_col[c].rc_size);
		abd_off += rm->rm_col[c].rc_size;
	}

	if (array == NULL)
		kmem_free(permutation, sizeof (permutation[0]) * ncols);
	else
		*array = permutation; /* caller will free */

	rm->rm_ops = vdev_raidz_math_get_ops();
	zio->io_vsd = rm;
	zio->io_vsd_ops = &vdev_raidz_vsd_ops;
	return (rm);
}

static inline uint64_t
vdev_draid_permutation_asize(vdev_draid_config_t *vdc)
{
	return ((vdc->vdc_children - vdc->vdc_spares) << DRAID_SLICESHIFT);
}

uint64_t
vdev_draid_offset_to_group(const vdev_t *vd, uint64_t offset)
{
	uint64_t perm, perm_off, group;
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t nparity = vd->vdev_nparity;
	uint64_t asize = vdev_draid_permutation_asize(vdc);

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	perm = offset / asize;
	perm_off = offset % asize;

	for (group = 0; group < vdc->vdc_groups; group++) {
		uint64_t ndata = vdc->vdc_data[group];
		uint64_t group_size = (ndata + nparity) << DRAID_SLICESHIFT;
		if (perm_off < group_size)
			break;
		perm_off -= group_size;
	}
	ASSERT3U(group, <, vdc->vdc_groups);

	return (perm * vdc->vdc_groups + group);
}

uint64_t
vdev_draid_group_to_offset(const vdev_t *vd, uint64_t group)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	uint64_t nparity = vd->vdev_nparity;
	uint64_t offset = vdev_draid_permutation_asize(vdc) *
	    (group / vdc->vdc_groups);

	for (uint64_t i = 0; i < group % vdc->vdc_groups; i++) {
		uint64_t ndata = vdc->vdc_data[i];
		offset += (ndata + nparity) << DRAID_SLICESHIFT;
	}

	return (offset);
}

/*
 * Given a offset into a draid, compute a properly aligned offset.
 * This involves finding the appropriate permutation chunk and then
 * computing which group in that chunk the offset falls. Once this
 * is calculated, the offset must be aligned to a slice boundry within
 * the raid group.
 */
uint64_t
vdev_draid_get_astart(const vdev_t *vd, const uint64_t start)
{
	uint64_t astart, perm_off, groupsz, group_chunk;
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	perm_off = start % vdev_draid_permutation_asize(vdc);
	groupsz = 0;
	group_chunk = 0;

	for (int i = 0; i < vdc->vdc_groups; i++) {
		groupsz = vdc->vdc_data[i] + vd->vdev_nparity;
		group_chunk = groupsz << DRAID_SLICESHIFT;
		if (perm_off < group_chunk)
			break;
		perm_off -= group_chunk;
	}

	ASSERT3U(perm_off, <, group_chunk);
	ASSERT3U(groupsz, <=, vdc->vdc_children - vdc->vdc_spares);

	astart = start - perm_off;
	astart += roundup(perm_off, groupsz << vd->vdev_ashift);
	ASSERT3U(astart, >=, start);

	return (astart);
}

uint64_t
vdev_draid_check_block(const vdev_t *vd, uint64_t start, uint64_t *sizep)
{
	uint64_t group = vdev_draid_offset_to_group(vd, start);
	uint64_t asize = vdev_draid_asize_by_type(vd, start, *sizep);
	uint64_t end = start + asize - 1;

	/*
	 * An allocation may not span metaslabs.
	 */
	if (start >> vd->vdev_ms_shift != end >> vd->vdev_ms_shift)
		return (-1ULL);

	/*
	 * A block is good if it:
	 * - does not cross group boundary, AND
	 * - does not use a remainder group
	 */
	if (group == vdev_draid_offset_to_group(vd, end)) {
		ASSERT3U(start, ==, vdev_draid_get_astart(vd, start));
		*sizep = asize;
		return (start);
	}

	/* jump to the next group */
	group++;
	start = vdev_draid_group_to_offset(vd, group);
	asize = vdev_draid_asize_by_type(vd, start, *sizep);
	end = start + asize - 1;

	ASSERT3U(group, ==, vdev_draid_offset_to_group(vd, end));

	*sizep = asize;

	return (start);
}

static vdev_t *vdev_dspare_get_child(vdev_t *vd, uint64_t offset);

/*
 * dRAID spare does not fit into the DTL model. While it has child vdevs,
 * there is no redundancy among them, and the effective child vdev is
 * determined by offset. Moreover, DTLs of a child vdev before the spare
 * becomes active are invalid, because the spare blocks were not in use yet.
 *
 * Here we are essentially doing a vdev_dtl_reassess() on the fly, by replacing
 * a dRAID spare with the child vdev under the offset. Note that it is a
 * recursive process because the child vdev can be another dRAID spare, and so
 * on.
 */
boolean_t
vdev_draid_missing(vdev_t *vd, uint64_t offset, uint64_t txg, uint64_t size)
{
	int c;

	if (vdev_dtl_contains(vd, DTL_MISSING, txg, size))
		return (B_TRUE);

	if (vd->vdev_ops == &vdev_draid_spare_ops)
		vd = vdev_dspare_get_child(vd, offset);

	if (vd->vdev_ops != &vdev_spare_ops)
		return (vdev_dtl_contains(vd, DTL_MISSING, txg, size));

	if (vdev_dtl_contains(vd, DTL_MISSING, txg, size))
		return (B_TRUE);

	for (c = 0; c < vd->vdev_children; c++) {
		vdev_t *cvd = vd->vdev_child[c];

		if (!vdev_readable(cvd))
			continue;

		if (!vdev_draid_missing(cvd, offset, txg, size))
			return (B_FALSE);
	}

	return (B_TRUE);
}

boolean_t
vdev_draid_readable(vdev_t *vd, uint64_t offset)
{
	int c;

	if (vd->vdev_ops == &vdev_draid_spare_ops)
		vd = vdev_dspare_get_child(vd, offset);

	if (vd->vdev_ops != &vdev_spare_ops)
		return (vdev_readable(vd));

	for (c = 0; c < vd->vdev_children; c++)
		if (vdev_draid_readable(vd->vdev_child[c], offset))
			return (B_TRUE);

	return (B_FALSE);
}

boolean_t
vdev_draid_is_dead(vdev_t *vd, uint64_t offset)
{
	int c;

	if (vd->vdev_ops == &vdev_draid_spare_ops)
		vd = vdev_dspare_get_child(vd, offset);

	if (vd->vdev_ops != &vdev_spare_ops)
		return (vdev_is_dead(vd));

	for (c = 0; c < vd->vdev_children; c++)
		if (!vdev_draid_is_dead(vd->vdev_child[c], offset))
			return (B_FALSE);

	return (B_TRUE);
}

static boolean_t
vdev_draid_guid_exists(vdev_t *vd, uint64_t guid, uint64_t offset)
{
	int c;

	if (vd->vdev_ops == &vdev_draid_spare_ops)
		vd = vdev_dspare_get_child(vd, offset);

	if (vd->vdev_guid == guid)
		return (B_TRUE);

	if (vd->vdev_ops->vdev_op_leaf)
		return (B_FALSE);

	for (c = 0; c < vd->vdev_children; c++)
		if (vdev_draid_guid_exists(vd->vdev_child[c], guid, offset))
			return (B_TRUE);

	return (B_FALSE);
}

static boolean_t
vdev_draid_vd_degraded(vdev_t *vd, const vdev_t *fault_vdev, uint64_t offset)
{
	/* Resilver */
	if (fault_vdev == NULL)
		return (!vdev_dtl_empty(vd, DTL_PARTIAL));

	/* Rebuild */
	ASSERT(fault_vdev->vdev_ops->vdev_op_leaf);
	ASSERT(fault_vdev->vdev_ops != &vdev_draid_spare_ops);

	return (vdev_draid_guid_exists(vd, fault_vdev->vdev_guid, offset));
}

boolean_t
vdev_draid_group_degraded(vdev_t *vd, vdev_t *fault_vdev, uint64_t offset,
    uint64_t size)
{
	uint64_t ashift = vd->vdev_top->vdev_ashift;
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	boolean_t degraded = B_FALSE;
	zio_t *zio;
	int dummy_data;
	uint64_t *perm;
	char buf[128];

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	zio = kmem_alloc(sizeof (*zio), KM_SLEEP);
	zio->io_vd = vd;
	zio->io_offset = offset;
	zio->io_size = MAX(SPA_MINBLOCKSIZE, 1ULL << ashift);
	zio->io_abd = abd_get_from_buf(&dummy_data, zio->io_size);

	buf[0] = '\0';
	raidz_map_t *rm = vdev_draid_map_alloc(zio, &perm);

	uint64_t group __maybe_unused = vdev_draid_offset_to_group(vd, offset);
	ASSERT3U(group, ==, vdev_draid_offset_to_group(vd, offset + size - 1));
	ASSERT3U(rm->rm_scols, ==,
	    vdc->vdc_parity + vdc->vdc_data[group % vdc->vdc_groups]);

	for (int c = 0; c < rm->rm_scols; c++) {
		raidz_col_t *rc = &rm->rm_col[c];
		vdev_t *cvd = vd->vdev_child[rc->rc_devidx];
		char *status = "";

		if (vdev_draid_vd_degraded(cvd, fault_vdev, rc->rc_offset)) {
			degraded = B_TRUE;
			status = "*";
		}
		if (zfs_flags & ZFS_DEBUG_DRAID) {
			snprintf(buf + strlen(buf),
			    sizeof (buf) - strlen(buf), "%llu%s ",
			    (u_longlong_t)cvd->vdev_id, status);
		}
	}

	if (zfs_flags & ZFS_DEBUG_DRAID) {
		snprintf(buf + strlen(buf), sizeof (buf) - strlen(buf),
		    "spares: ");
		for (int c = 0; c < vdc->vdc_spares; c++)
			snprintf(buf + strlen(buf),
			    sizeof (buf) - strlen(buf), "%llu",
			    (u_longlong_t)perm[vdc->vdc_children - 1 - c]);

		zfs_dbgmsg("%s dRAID, fault_guid=%llu, at %lluK of %lluK: %s",
		    degraded ? "Degraded" : "Healthy",
		    fault_vdev ? (u_longlong_t)fault_vdev->vdev_guid : 0,
		    offset >> 10, size >> 10, buf);
	}

	kmem_free(perm, sizeof (perm[0]) * vdc->vdc_children);

	(*zio->io_vsd_ops->vsd_free)(zio);

	abd_put(zio->io_abd);
	kmem_free(zio, sizeof (*zio));

	return (degraded);
}

static uint64_t *
vdev_draid_create_base_perms(const uint8_t *perms,
    const vdev_draid_config_t *vdc)
{
	uint64_t children = vdc->vdc_children, *base_perms;
	size_t sz = sizeof (uint64_t) * vdc->vdc_bases * children;

	base_perms = kmem_alloc(sz, KM_SLEEP);
	for (int i = 0; i < vdc->vdc_bases; i++)
		for (int j = 0; j < children; j++)
			base_perms[i * children + j] = perms[i * children + j];

	return (base_perms);
}

static vdev_draid_config_t *
vdev_draid_config_create(vdev_t *vd)
{
	uint_t c;
	uint8_t *perms = NULL;
	uint8_t *data = NULL;
	uint64_t *ndata = NULL;
	nvlist_t *nvl = vd->vdev_cfg;
	ASSERT(nvl != NULL);

	if (vdev_draid_config_validate(nvl, 0, vd->vdev_nparity, 0,
	    vd->vdev_children) != DRAIDCFG_OK) {
		return (NULL);
	}

	vdev_draid_config_t *vdc = kmem_alloc(sizeof (*vdc), KM_SLEEP);
	vdc->vdc_children = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_CHILDREN);
	vdc->vdc_groups = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_GROUPS);
	vdc->vdc_parity = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_PARITY);
	vdc->vdc_spares = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_SPARES);
	vdc->vdc_bases = fnvlist_lookup_uint64(nvl, ZPOOL_CONFIG_DRAIDCFG_BASE);

	VERIFY0(nvlist_lookup_uint8_array(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_DATA, &data, &c));
	ndata = kmem_alloc(sizeof (uint64_t) * vdc->vdc_groups, KM_SLEEP);
	for (int i = 0; i < vdc->vdc_groups; i++)
		ndata[i] = data[i];
	vdc->vdc_data = ndata;

	VERIFY0(nvlist_lookup_uint8_array(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_PERM, &perms, &c));

	vdc->vdc_base_perms = vdev_draid_create_base_perms(perms, vdc);
	vdc->vdc_zero_abd = NULL;

	return (vdc);
}

static void
vdev_draid_config_destroy(vdev_draid_config_t *vdc)
{
	size_t sz = sizeof (uint64_t) * vdc->vdc_bases * vdc->vdc_children;

	kmem_free(vdc->vdc_base_perms, P2ROUNDUP(sz, PAGESIZE));
	kmem_free(vdc, sizeof (*vdc));
}

static int
vdev_draid_open(vdev_t *vd, uint64_t *asize, uint64_t *max_asize,
    uint64_t *ashift)
{
	vdev_t *cvd;
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t nparity = vd->vdev_nparity;
	int lasterror = 0;
	int numerrors = 0;

	ASSERT(nparity > 0);

	if (nparity > VDEV_RAIDZ_MAXPARITY || vd->vdev_children < nparity + 1) {
		vd->vdev_stat.vs_aux = VDEV_AUX_BAD_LABEL;
		return (SET_ERROR(EINVAL));
	}

	if (vdc == NULL) {
		vdc = vdev_draid_config_create(vd);
		if (vdc == NULL)
			return (SET_ERROR(EINVAL));

		vd->vdev_tsd = vdc;

		/*
		 * Used to generate dRAID spare names and calculate the min
		 * asize even when the vdev_draid_config_t is not available
		 * because the open fails below and the vdc is freed.
		 */
		vd->vdev_spares = vdc->vdc_spares;
		vd->vdev_groups = vdc->vdc_groups;
	} else {
		ASSERT(vd->vdev_reopening);
	}

	/* vd->vdev_tsd must be set before vdev_open_children(vd) */
	vdev_open_children(vd);

	/* Find the size of the smallest child */
	for (int c = 0; c < vd->vdev_children; c++) {
		cvd = vd->vdev_child[c];

		if (cvd->vdev_open_error != 0) {
			lasterror = cvd->vdev_open_error;
			numerrors++;
			continue;
		}

		/* Find the smallest disk and largest sector size */
		if (cvd->vdev_ops != &vdev_draid_spare_ops) {
			*asize = MIN(*asize - 1, cvd->vdev_asize - 1) + 1;
			*max_asize =
			    MIN(*max_asize - 1, cvd->vdev_max_asize - 1) + 1;
			*ashift = MAX(*ashift, cvd->vdev_ashift);
		}
	}

	if (vdc->vdc_zero_abd == NULL) {
		abd_t *zabd;
		size_t sz = 1ULL << MAX(*ashift, vd->vdev_ashift);

		zabd = abd_alloc_linear(sz, B_TRUE);
		abd_zero(zabd, sz);

		vdc->vdc_zero_abd = zabd;
	}

	*asize *= vd->vdev_children - vdc->vdc_spares;
	*max_asize *= vd->vdev_children - vdc->vdc_spares;

	if (numerrors > nparity) {
		vd->vdev_stat.vs_aux = VDEV_AUX_NO_REPLICAS;
		vdev_draid_config_destroy(vd->vdev_tsd);
		vd->vdev_tsd = NULL;
		return (lasterror);
	}

	return (0);
}

static void
vdev_draid_close(vdev_t *vd)
{
	abd_t *zabd;
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	for (int c = 0; c < vd->vdev_children; c++)
		vdev_close(vd->vdev_child[c]);

	if (vd->vdev_reopening || vdc == NULL)
		return;

	zabd = vdc->vdc_zero_abd;
	ASSERT(zabd != NULL);
	abd_free(zabd);

	vdev_draid_config_destroy(vdc);
	vd->vdev_tsd = NULL;
}

uint64_t
vdev_draid_asize_by_type(const vdev_t *vd, uint64_t offset, uint64_t psize)
{
	uint64_t group;
	uint64_t asize;
	uint64_t ashift = vd->vdev_top->vdev_ashift;
	uint64_t nparity = vd->vdev_nparity;
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	group = vdev_draid_offset_to_group(vd, offset);
	group = group % vdc->vdc_groups;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	asize = ((psize - 1) >> ashift) + 1;

	uint64_t ndata = vdc->vdc_data[group];
	ASSERT3U(ndata, !=, 0);
	asize = roundup(asize, ndata);
	asize += nparity * (asize / ndata);

	ASSERT0(asize % (nparity + ndata));
	ASSERT(asize != 0);

	return (asize << ashift);
}

static uint64_t
vdev_draid_asize(vdev_t *vd, uint64_t offset, uint64_t psize)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t last = vdc->vdc_groups - 1;
	uint64_t ndata = vdc->vdc_data[0];
	uint64_t nparity = vdc->vdc_parity;
	uint64_t asizef = ((psize - 1) >> vd->vdev_top->vdev_ashift) + 1;
	uint64_t asizel = ((psize - 1) >> vd->vdev_top->vdev_ashift) + 1;

	if (offset != -1)
		return (vdev_draid_asize_by_type(vd, offset, psize));
	/*
	 * We can't compute precise asize unless we know the offset.
	 * Over-estimate the size and correct later at allocation.
	 */

	/* compute asize using first group size */
	asizef = roundup(asizef, ndata);
	asizef += nparity * (asizef / ndata);
	ASSERT0(asizef % (nparity + ndata));

	/* compute asize using last group size */
	ndata = vdc->vdc_data[last];
	asizel = roundup(asizel, ndata);
	asizel += nparity * (asizel / ndata);
	ASSERT0(asizel % (nparity + ndata));

	/* return larger of the two sizes computed */
	if (asizef > asizel)
		return (asizef << vd->vdev_top->vdev_ashift);
	else
		return (asizel << vd->vdev_top->vdev_ashift);
}

uint64_t
vdev_draid_asize_to_psize(vdev_t *vd, uint64_t asize, uint64_t offset)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t ashift __maybe_unused = vd->vdev_top->vdev_ashift;
	uint64_t psize, ndata;
	uint64_t group = vdev_draid_offset_to_group(vd, offset);

	ASSERT0(P2PHASE(asize, 1ULL << ashift));
	ASSERT0(P2PHASE(offset, 1ULL << ashift));

	group = group % vdc->vdc_groups;
	ndata = vdc->vdc_data[group];

	ASSERT0((asize >> ashift) % (ndata + vd->vdev_nparity));
	psize = (asize / (ndata + vd->vdev_nparity)) * ndata;

	ASSERT3U(psize, <=, spa_maxblocksize(vd->vdev_spa));

	return (psize);
}

uint64_t
vdev_draid_max_rebuildable_asize(vdev_t *vd, uint64_t offset, uint64_t maxpsize)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t ashift = vd->vdev_top->vdev_ashift;
	uint64_t group = vdev_draid_offset_to_group(vd, offset);

	/*
	 * When the maxpsize >> ashift does not divide evenly by the number
	 * of data drives, the remainder must be discarded. Otherwise the skip
	 * sectors will cause vdev_draid_asize_to_psize() to get a psize larger
	 * than the maximum allowed block size.
	 */
	group = group % vdc->vdc_groups;
	maxpsize >>= ashift;
	maxpsize /= vdc->vdc_data[group];
	maxpsize *= vdc->vdc_data[group];
	maxpsize <<= ashift;

	return (vdev_draid_asize_by_type(vd, offset, maxpsize));
}

static boolean_t
vdev_draid_need_resilver(vdev_t *vd, uint64_t offset, size_t psize)
{
	/* A block cannot cross redundancy group boundary */
	ASSERT3U(vdev_draid_offset_to_group(vd, offset), ==,
	    vdev_draid_offset_to_group(vd, offset + psize - 1));

	return (vdev_draid_group_degraded(vd, NULL, offset, psize));
}

static void
vdev_draid_skip_io_done(zio_t *zio)
{
	/*
	 * HH: handle skip IO error
	 * raidz_col_t *rc = zio->io_private;
	 */
}

/*
 * Start an IO operation on a dRAID VDev
 *
 * Outline:
 * - For write operations:
 *   1. Generate the parity data
 *   2. Create child zio write operations to each column's vdev, for both
 *      data and parity.
 *   3. If the column skips any sectors for padding, create optional dummy
 *      write zio children for those areas to improve aggregation continuity.
 * - For read operations:
 *   1. Create child zio read operations to each data column's vdev to read
 *      the range of data required for zio.
 *   2. If this is a scrub or resilver operation, or if any of the data
 *      vdevs have had errors, then create zio read operations to the parity
 *      columns' VDevs as well.
 */
static void
vdev_draid_io_start(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;
	uint64_t ashift = vd->vdev_top->vdev_ashift;
	vdev_t *cvd;
	raidz_map_t *rm;
	raidz_col_t *rc;
	int c, i;
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	rm = vdev_draid_map_alloc(zio, NULL);
	ASSERT3U(rm->rm_asize, ==,
	    vdev_draid_asize_by_type(vd, zio->io_offset, zio->io_size));

	/* XXX - should construct a meta-abd with skip abd at end */
	if (zio->io_type == ZIO_TYPE_WRITE) {
		vdev_raidz_generate_parity(rm);

		for (c = 0; c < rm->rm_cols; c++) {
			rc = &rm->rm_col[c];
			cvd = vd->vdev_child[rc->rc_devidx];
			zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
			    rc->rc_offset, rc->rc_abd, rc->rc_size,
			    zio->io_type, zio->io_priority, 0,
			    vdev_raidz_child_done, rc));
		}

		/*
		 * Unlike raidz, it's mandatory to fill skip sectors with zero.
		 */
		for (c = rm->rm_skipstart, i = 0; i < rm->rm_nskip; c++, i++) {
			ASSERT3U(c, <, rm->rm_scols);
			ASSERT3U(c, >, rm->rm_firstdatacol);

			rc = &rm->rm_col[c];
			cvd = vd->vdev_child[rc->rc_devidx];
			zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
			    rc->rc_offset + rc->rc_size, vdc->vdc_zero_abd,
			    1ULL << ashift, zio->io_type, zio->io_priority,
			    0, vdev_draid_skip_io_done, rc));
		}

		zio_execute(zio);
		return;
	}

	ASSERT(zio->io_type == ZIO_TYPE_READ);

	/*
	 * Sequential rebuild must do IO at redundancy group boundary,
	 * i.e. rm->rm_nskip must be 0.
	 */
	IMPLY(zio->io_priority == ZIO_PRIORITY_REBUILD, rm->rm_nskip == 0);

	/*
	 * Iterate over the columns in reverse order so that we hit the parity
	 * last -- any errors along the way will force us to read the parity.
	 */
	for (c = rm->rm_cols - 1; c >= 0; c--) {
		rc = &rm->rm_col[c];
		cvd = vd->vdev_child[rc->rc_devidx];
		if (!vdev_draid_readable(cvd, rc->rc_offset)) {
			if (c >= rm->rm_firstdatacol)
				rm->rm_missingdata++;
			else
				rm->rm_missingparity++;
			rc->rc_error = SET_ERROR(ENXIO);
			rc->rc_tried = 1;	/* don't even try */
			rc->rc_skipped = 1;
			continue;
		}
		if (vdev_draid_missing(cvd, rc->rc_offset, zio->io_txg, 1)) {
			if (c >= rm->rm_firstdatacol)
				rm->rm_missingdata++;
			else
				rm->rm_missingparity++;
			rc->rc_error = SET_ERROR(ESTALE);
			rc->rc_skipped = 1;
			continue;
		}
		if (c >= rm->rm_firstdatacol || rm->rm_missingdata > 0 ||
		    (zio->io_flags & (ZIO_FLAG_SCRUB | ZIO_FLAG_RESILVER))) {
			zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
			    rc->rc_offset, rc->rc_abd, rc->rc_size,
			    zio->io_type, zio->io_priority, 0,
			    vdev_raidz_child_done, rc));
		}
	}

	/*
	 * Check skip sectors for scrub/resilver. For sequential rebuild,
	 * this is a no-op because rm->rm_nskip is always zero.
	 */
	if ((zio->io_flags & (ZIO_FLAG_SCRUB | ZIO_FLAG_RESILVER))) {
		for (c = rm->rm_skipstart, i = 0; i < rm->rm_nskip; c++, i++) {
			abd_t *abd;

			ASSERT3U(c, <, rm->rm_scols);
			ASSERT3U(c, >, rm->rm_firstdatacol);

			rc = &rm->rm_col[c];
			cvd = vd->vdev_child[rc->rc_devidx];

			if (!vdev_draid_readable(cvd,
			    rc->rc_offset + rc->rc_size)) {
				rc->rc_abd_skip = NULL;
				continue;
			}

			abd = abd_get_offset_size(rm->rm_abd_skip,
			    i << ashift, 1ULL << ashift);
			*((int *)abd_to_buf(abd)) = 1;
			rc->rc_abd_skip = abd;

			/* Skip sector to be written in vdev_draid_io_done() */
			if (vdev_draid_missing(cvd,
			    rc->rc_offset + rc->rc_size, zio->io_txg, 1))
				continue;

			zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
			    rc->rc_offset + rc->rc_size, abd,
			    1ULL << ashift, ZIO_TYPE_READ,
			    zio->io_priority, 0, vdev_draid_skip_io_done, rc));
		}
	}

	zio_execute(zio);
}

int
vdev_draid_hide_skip_sectors(raidz_map_t *rm)
{
	size_t size = rm->rm_col[0].rc_size;
	vdev_t *vd = rm->rm_vdev;
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	int cols;

	ASSERT(vdev_raidz_map_declustered(rm));

	for (int c = rm->rm_cols; c < rm->rm_scols; c++) {
		raidz_col_t *rc = &rm->rm_col[c];

		ASSERT0(rc->rc_size);
		ASSERT0(rc->rc_error);
		ASSERT0(rc->rc_tried);
		ASSERT0(rc->rc_skipped);
		ASSERT(rc->rc_abd == NULL);

		rc->rc_size = size;
		rc->rc_abd = vdc->vdc_zero_abd;
	}

	cols = rm->rm_cols;
	rm->rm_cols = rm->rm_scols;
	return (cols);
}

void
vdev_draid_restore_skip_sectors(raidz_map_t *rm, int cols)
{
	int c;

	ASSERT3U(cols, >, rm->rm_firstdatacol);
	ASSERT3U(cols, <=, rm->rm_scols);
	ASSERT(vdev_raidz_map_declustered(rm));

	for (c = cols; c < rm->rm_scols; c++) {
		raidz_col_t *rc = &rm->rm_col[c];

		ASSERT0(rc->rc_error);
		ASSERT0(rc->rc_tried);
		ASSERT0(rc->rc_skipped);
		ASSERT(rc->rc_abd != NULL);

		rc->rc_size = 0;
		rc->rc_abd = NULL;
	}

	rm->rm_cols = cols;
}

void
vdev_draid_fix_skip_sectors(zio_t *zio)
{
	int c, i;
	char *zero;
	vdev_t *vd = zio->io_vd;
	raidz_map_t *rm = zio->io_vsd;
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	const uint64_t size = 1ULL << vd->vdev_top->vdev_ashift;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);
	ASSERT3P(rm->rm_vdev, ==, vd);

	if (rm->rm_abd_skip == NULL)
		return;

	zero = abd_to_buf(vdc->vdc_zero_abd);
	for (c = rm->rm_skipstart, i = 0; i < rm->rm_nskip; c++, i++) {
		char *skip;
		boolean_t good_skip;
		raidz_col_t *rc = &rm->rm_col[c];

		ASSERT3U(c, <, rm->rm_scols);
		ASSERT3U(c, >, rm->rm_firstdatacol);

		if (rc->rc_abd_skip == NULL)
			continue;

		skip = abd_to_buf(rc->rc_abd_skip);
		good_skip = (memcmp(skip, zero, size) == 0);
		abd_put(rc->rc_abd_skip);
		rc->rc_abd_skip = NULL;

		if (good_skip || !spa_writeable(zio->io_spa))
			continue;

		zio_nowait(zio_vdev_child_io(zio, NULL,
		    vd->vdev_child[rc->rc_devidx],
		    rc->rc_offset + rc->rc_size, vdc->vdc_zero_abd,
		    size, ZIO_TYPE_WRITE, ZIO_PRIORITY_ASYNC_WRITE,
		    ZIO_FLAG_IO_REPAIR, NULL, NULL));
	}
}

static void
vdev_draid_io_done(zio_t *zio)
{
	vdev_raidz_ops.vdev_op_io_done(zio);
}

static void
vdev_draid_state_change(vdev_t *vd, int faulted, int degraded)
{
	if (faulted > vd->vdev_nparity)
		vdev_set_state(vd, B_FALSE, VDEV_STATE_CANT_OPEN,
		    VDEV_AUX_NO_REPLICAS);
	else if (degraded + faulted != 0)
		vdev_set_state(vd, B_FALSE, VDEV_STATE_DEGRADED, VDEV_AUX_NONE);
	else
		vdev_set_state(vd, B_FALSE, VDEV_STATE_HEALTHY, VDEV_AUX_NONE);
}

/*
 * Return the index at which the specified vdev id appears in the permutation
 * chunk for the provided offset.
 */
#if 0
static uint64_t
vdev_draid_find_idx(uint64_t nr, uint64_t idx, vdev_draid_config_t *vdc)
{
	uint64_t ncols = vdc->vdc_children;
	uint64_t off = nr % (vdc->vdc_bases * ncols);
	uint64_t base = off / ncols;
	uint64_t dev = off % ncols;

	for (uint64_t i = 0; i < ncols; i++) {
		const uint64_t *base_perm = vdc->vdc_base_perms +
		    (base * ncols);

		if ((base_perm[i] + dev) % ncols == idx)
			return (i);
	}

	/* Unreachable */
	return (UINT64_MAX);
}
#endif

/*
 * XXX - This is mostly correct, but we still do see a small number of
 * checksum errors when scrubbing the pool after a TRIM.  Disable until
 * we can determine why.
 */
static void
vdev_draid_xlate(vdev_t *cvd, const range_seg64_t *in, range_seg64_t *res)
{
#if 0
	vdev_t *raidvd = cvd->vdev_parent;
	ASSERT(raidvd->vdev_ops == &vdev_draid_ops);

	vdev_draid_config_t *vdc = raidvd->vdev_tsd;
	uint64_t ashift = raidvd->vdev_top->vdev_ashift;
	uint64_t width = raidvd->vdev_children - vdc->vdc_spares;

	/* the logical column stored at this offset from the permutation */
	uint64_t start_tgt_col = vdev_draid_find_idx(
	    in->rs_start >> DRAID_SLICESHIFT, cvd->vdev_id, vdc);
	uint64_t end_tgt_col = vdev_draid_find_idx(
	    in->rs_end >> DRAID_SLICESHIFT, cvd->vdev_id, vdc);
	ASSERT3U(start_tgt_col, <, raidvd->vdev_children);
	ASSERT3U(end_tgt_col, <, raidvd->vdev_children);

	/* make sure the offsets are block-aligned */
	ASSERT0(in->rs_start % (1 << ashift));
	ASSERT0(in->rs_end % (1 << ashift));
	uint64_t b_start = in->rs_start >> ashift;
	uint64_t b_end = in->rs_end >> ashift;

	uint64_t start_row = 0;
	if (b_start > start_tgt_col) /* avoid underflow */
		start_row = ((b_start - start_tgt_col - 1) / width) + 1;

	uint64_t end_row = 0;
	if (b_end > end_tgt_col)
		end_row = ((b_end - end_tgt_col - 1) / width) + 1;

	res->rs_start = start_row << ashift;
	res->rs_end = end_row << ashift;
#else
	res->rs_start = in->rs_start;
	res->rs_end = in->rs_start;
#endif

	ASSERT3U(res->rs_start, <=, in->rs_start);
	ASSERT3U(res->rs_end - res->rs_start, <=, in->rs_end - in->rs_start);
}

vdev_ops_t vdev_draid_ops = {
	.vdev_op_open = vdev_draid_open,
	.vdev_op_close = vdev_draid_close,
	.vdev_op_asize = vdev_draid_asize,
	.vdev_op_io_start = vdev_draid_io_start,
	.vdev_op_io_done = vdev_draid_io_done,
	.vdev_op_state_change = vdev_draid_state_change,
	.vdev_op_need_resilver = vdev_draid_need_resilver,
	.vdev_op_hold = NULL,
	.vdev_op_rele = NULL,
	.vdev_op_remap = NULL,
	.vdev_op_xlate = vdev_draid_xlate,
	.vdev_op_type = VDEV_TYPE_DRAID,	/* name of this vdev type */
	.vdev_op_leaf = B_FALSE,		/* not a leaf vdev */
};

typedef struct {
	vdev_t	*vds_draid_vdev;
	uint64_t vds_spare_id;
} vdev_draid_spare_t;

static vdev_t *
vdev_dspare_get_child(vdev_t *vd, uint64_t offset)
{
	vdev_draid_spare_t *vds = vd->vdev_tsd;
	vdev_draid_config_t *vdc;
	vdev_t *tvd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_spare_ops);
	ASSERT3U(offset, <, vd->vdev_psize -
	    (VDEV_LABEL_START_SIZE + VDEV_LABEL_END_SIZE));
	ASSERT(vds != NULL);

	tvd = vds->vds_draid_vdev;
	ASSERT3P(tvd->vdev_ops, ==, &vdev_draid_ops);
	vdc = tvd->vdev_tsd;

	ASSERT3U(vds->vds_spare_id, <, vdc->vdc_spares);

	uint64_t *perm, spare_idx;
	perm = kmem_alloc(sizeof (perm[0]) * tvd->vdev_children, KM_SLEEP);
	vdev_draid_get_permutation(perm, offset >> DRAID_SLICESHIFT, vdc);
	spare_idx = perm[(tvd->vdev_children - 1) - vds->vds_spare_id];
	kmem_free(perm, sizeof (perm[0]) * tvd->vdev_children);

	return (tvd->vdev_child[spare_idx]);
}

vdev_t *
vdev_draid_spare_get_parent(vdev_t *vd)
{
	vdev_draid_spare_t *vds = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_spare_ops);
	ASSERT(vds != NULL);
	ASSERT(vds->vds_draid_vdev != NULL);

	return (vds->vds_draid_vdev);
}

/*
 * Generates a valid label for the dRAID distributed spare in order
 * for it to convincingly simulate a physical leaf vdev.
 */
nvlist_t *
vdev_draid_spare_read_config(vdev_t *vd)
{
	spa_t *spa = vd->vdev_spa;
	spa_aux_vdev_t *sav = &spa->spa_spares;
	nvlist_t *nv = fnvlist_alloc();

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_spare_ops);

	fnvlist_add_uint64(nv, ZPOOL_CONFIG_IS_SPARE, 1);
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_CREATE_TXG, vd->vdev_crtxg);
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_VERSION, spa_version(spa));
	fnvlist_add_string(nv, ZPOOL_CONFIG_POOL_NAME, spa_name(spa));
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_POOL_GUID, spa_guid(spa));
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_POOL_TXG, spa->spa_config_txg);
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_TOP_GUID, vd->vdev_top->vdev_guid);

	/*
	 * We are in use if our parent is spare_ops
	 */
	if (vd->vdev_parent != NULL &&
	    vd->vdev_parent->vdev_ops == &vdev_spare_ops) {
		fnvlist_add_uint64(nv,
		    ZPOOL_CONFIG_POOL_STATE, POOL_STATE_ACTIVE);
	} else {
		fnvlist_add_uint64(nv,
		    ZPOOL_CONFIG_POOL_STATE, POOL_STATE_SPARE);
	}

	uint64_t guid = vd->vdev_guid;
	for (int i = 0; i < sav->sav_count; i++) {
		if (sav->sav_vdevs[i]->vdev_ops == &vdev_draid_spare_ops &&
		    strcmp(sav->sav_vdevs[i]->vdev_path, vd->vdev_path) == 0) {
			guid = sav->sav_vdevs[i]->vdev_guid;
			break;
		}
	}
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_GUID, guid);

	return (nv);
}

/*
 * Opening a dRAID spare device is done by extracting the top-level vdev id
 * and dRAID spare number from the provided vd->vdev_path identified.  Any
 * additional information encoded in the identifier is solely used to
 * verification cross-checks and is not strictly required.
 */
static int
vdev_dspare_open(vdev_t *vd, uint64_t *psize, uint64_t *max_psize,
    uint64_t *ashift)
{
	uint64_t parity, groups, spares, vdev_id, spare_id;
	uint64_t asize, max_asize;
	vdev_draid_config_t *vdc;
	vdev_draid_spare_t *vds;
	vdev_t *tvd, *rvd;
	int error;

	if (vd->vdev_tsd != NULL) {
		ASSERT(vd->vdev_reopening);
		vds = vd->vdev_tsd;
		tvd = vds->vds_draid_vdev;
		vdc = tvd->vdev_tsd;
		goto skip_open;
	}

	/* Extract dRAID configuration values from the provided vdev */
	error = vdev_draid_spare_values(vd->vdev_path, &parity, &groups,
	    &spares, &vdev_id, &spare_id);
	if (error)
		return (error);

	if (vdev_id >= vd->vdev_spa->spa_root_vdev->vdev_children)
		return (SET_ERROR(EINVAL));

	rvd = vd->vdev_spa->spa_root_vdev;
	tvd = rvd->vdev_child[vdev_id];
	vdc = tvd->vdev_tsd;

	/* Spare name references a known top-level dRAID vdev */
	if (tvd->vdev_ops != &vdev_draid_ops || tvd->vdev_nparity != parity)
		return (SET_ERROR(EINVAL));

	/* Spare name dDRAID settings agree with top-level dRAID vdev */
	if (vdc == NULL || vdc->vdc_parity != parity ||
	    vdc->vdc_groups != groups || vdc->vdc_spares != spares ||
	    vdc->vdc_spares <= spare_id) {
		return (SET_ERROR(EINVAL));
	}

	vds = kmem_alloc(sizeof (vdev_draid_spare_t), KM_SLEEP);
	vds->vds_draid_vdev = tvd;
	vds->vds_spare_id = spare_id;
	vd->vdev_tsd = vds;

skip_open:
	asize = tvd->vdev_asize / (tvd->vdev_children - vdc->vdc_spares);

	/* If parent max_asize not yet set, use asize */
	if (tvd->vdev_max_asize == 0) {
		max_asize = asize;
	} else {
		max_asize = tvd->vdev_max_asize /
		    (tvd->vdev_children - vdc->vdc_spares);
	}

	*ashift = tvd->vdev_ashift;
	*psize = asize + VDEV_LABEL_START_SIZE + VDEV_LABEL_END_SIZE;
	*max_psize = max_asize + VDEV_LABEL_START_SIZE + VDEV_LABEL_END_SIZE;

	return (0);
}

static void
vdev_dspare_close(vdev_t *vd)
{
	vdev_draid_spare_t *vds = vd->vdev_tsd;

	if (vd->vdev_reopening || vds == NULL)
		return;

	vd->vdev_tsd = NULL;
	kmem_free(vds, sizeof (vdev_draid_spare_t));
}

static uint64_t
vdev_dspare_asize(vdev_t *vd, uint64_t offset, uint64_t psize)
{
	/* HH: this function should never get called */
	ASSERT0(offset);
	ASSERT0(psize);
	return (0);
}

static void
vdev_dspare_child_done(zio_t *zio)
{
	zio_t *pio = zio->io_private;

	pio->io_error = zio->io_error;
}

static void
vdev_dspare_io_start(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;
	vdev_t *cvd;
	uint64_t offset = zio->io_offset;

	/* HH: if dspare gets a FLUSH, so do all children of the draid vdev */
	if (zio->io_type == ZIO_TYPE_IOCTL) {
		zio->io_error = 0;
		zio_execute(zio);
		return;
	}

	/*
	 * HH: at pool creation, dspare gets some writes with
	 * ZIO_FLAG_SPECULATIVE and ZIO_FLAG_NODATA.
	 * Need to understand and handle them right.
	 * Leave as-is until I can investigate.
	 */
	if (zio->io_flags & ZIO_FLAG_NODATA) {
		zio->io_error = 0;
		zio_execute(zio);
		return;
	}

	if (offset < VDEV_LABEL_START_SIZE ||
	    offset >= vd->vdev_psize - VDEV_LABEL_END_SIZE) {
		ASSERT(zio->io_flags & ZIO_FLAG_PHYSICAL);

		/*
		 * HH: dspare should not get any label IO as it is pretending
		 * to be a leaf disk. Later should catch and fix all places
		 * that still does label IO to dspare.
		 */
		zio->io_error = SET_ERROR(EIO);
		zio_interrupt(zio);
		return;
	}

	offset -= VDEV_LABEL_START_SIZE; /* See zio_vdev_child_io() */
	cvd = vdev_dspare_get_child(vd, offset);
	if (zio->io_type == ZIO_TYPE_READ && !vdev_readable(cvd)) {
		zio->io_error = SET_ERROR(ENXIO);
		zio_interrupt(zio);
		/*
		 * Parent vdev should have avoided reading from me in the first
		 * place, unless this is a mirror scrub.
		 */
		zfs_dbgmsg("read from dead spare %s:%s:%s at %llu",
		    vd->vdev_path, cvd->vdev_ops->vdev_op_type,
		    cvd->vdev_path != NULL ? cvd->vdev_path : "NA", offset);
		return;
	}

	/* dspare IO does not cross slice boundary */
	ASSERT3U(offset >> DRAID_SLICESHIFT, ==,
	    (offset + zio->io_size - 1) >> DRAID_SLICESHIFT);
	zio_nowait(zio_vdev_child_io(zio, NULL, cvd, offset, zio->io_abd,
	    zio->io_size, zio->io_type, zio->io_priority, 0,
	    vdev_dspare_child_done, zio));
	zio_execute(zio);
}

static void
vdev_dspare_io_done(zio_t *zio)
{
}

/*
 * Initializing and trimming of distributed spares is currently disabled.
 */
static void
vdev_dspare_xlate(vdev_t *cvd, const range_seg64_t *in, range_seg64_t *res)
{
	res->rs_start = in->rs_start;
	res->rs_end = in->rs_start;
}

vdev_ops_t vdev_draid_spare_ops = {
	.vdev_op_open = vdev_dspare_open,
	.vdev_op_close = vdev_dspare_close,
	.vdev_op_asize = vdev_dspare_asize,
	.vdev_op_io_start = vdev_dspare_io_start,
	.vdev_op_io_done = vdev_dspare_io_done,
	.vdev_op_state_change = NULL,
	.vdev_op_need_resilver = NULL,
	.vdev_op_hold = NULL,
	.vdev_op_rele = NULL,
	.vdev_op_remap = NULL,
	.vdev_op_xlate = vdev_dspare_xlate,
	.vdev_op_type = VDEV_TYPE_DRAID_SPARE,
	.vdev_op_leaf = B_TRUE,
};
