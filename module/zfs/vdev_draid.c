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

#ifdef ZFS_DEBUG
#include <sys/vdev.h>	/* For vdev_xlate() in vdev_draid_io_verify() */
#endif

/*
 * dRAID is a distributed parity implementation for ZFS.
 */

static uint64_t vdev_draid_psize_to_asize(const vdev_t *, uint64_t, uint64_t);
static vdev_t *vdev_draid_spare_get_child(vdev_t *vd, uint64_t offset);

/* A child vdev is divided into slices */
static unsigned int slice_shift = 0;
#define	DRAID_SLICESHIFT (SPA_MAXBLOCKSHIFT + slice_shift)
/* 2 ** slice_shift * SPA_MAXBLOCKSIZE */
#define	DRAID_SLICESIZE  (1ULL << DRAID_SLICESHIFT)
#define	DRAID_SLICEMASK  (DRAID_SLICESIZE - 1)

/*
 * Lookup the permutation base array and iteration id for the provided offset.
 */
static void
vdev_draid_get_perm(vdev_draid_config_t *vdc, uint64_t pindex,
    uint64_t **base, uint64_t *iter)
{
	uint64_t ncols = vdc->vdc_children;
	uint64_t poff = pindex % (vdc->vdc_bases * ncols);

	*base = vdc->vdc_base_perms + (poff / ncols) * ncols;
	*iter = poff % ncols;
}

static inline uint64_t
vdev_draid_permute_id(vdev_draid_config_t *vdc,
    uint64_t *base, uint64_t iter, uint64_t index)
{
	return ((base[index] + iter) % vdc->vdc_children);
}

/*
 * Full stripe writes.  For "big columns" it's sufficient to map the correct
 * range of the zio ABD.  Partial columns require allocating a gang ABD in
 * order to zero fill the skip sectors.  When the column is empty a zero
 * filled skip sector must be mapped.  In all cases the data ABDs must be
 * the same size as the parity ABDs.
 *
 * Both rm->cols and rc->rc_size are increased to calculate the parity over
 * the full stripe width.  All zero filled skip sectors must be written to
 * disk for the benefit of the device rebuild feature which is unaware of
 * individual block boundaries.
 */
static void
vdev_draid_map_alloc_write(zio_t *zio, raidz_map_t *rm)
{
	uint64_t skip_size = 1ULL << zio->io_vd->vdev_top->vdev_ashift;
	uint64_t parity_size = rm->rm_col[0].rc_size;
	uint64_t abd_off = 0;

	ASSERT3U(zio->io_type, ==, ZIO_TYPE_WRITE);
	ASSERT3U(parity_size, ==, abd_get_size(rm->rm_col[0].rc_abd));

	for (uint64_t c = rm->rm_firstdatacol; c < rm->rm_scols; c++) {
		raidz_col_t *rc = &rm->rm_col[c];

		if (rm->rm_skipstart == 0 || c < rm->rm_skipstart) {
			/* this is a "big column */
			ASSERT3U(rc->rc_size, ==, parity_size);
			rc->rc_abd = abd_get_offset_size(zio->io_abd,
			    abd_off, rc->rc_size);
		} else if (c < rm->rm_cols) {
			/* short data column, add a skip sector */
			ASSERT3U(rc->rc_size + skip_size, ==, parity_size);
			rc->rc_abd = abd_alloc_gang_abd();
			abd_gang_add(rc->rc_abd, abd_get_offset_size(
			    zio->io_abd, abd_off, rc->rc_size), B_TRUE);
			abd_gang_add(rc->rc_abd, abd_get_zeros(skip_size),
			    B_TRUE);
		} else {
			ASSERT0(rc->rc_size);
			ASSERT3U(skip_size, ==, parity_size);
			/* empty data column (small write), allocate skip sector */
			rc->rc_abd = abd_get_zeros(skip_size);
		}

		ASSERT3U(abd_get_size(rc->rc_abd), ==, parity_size);

		abd_off += rc->rc_size;
		rc->rc_size = parity_size;
	}
	ASSERT3U(abd_off, ==, zio->io_size);
	rm->rm_cols = rm->rm_scols;
}

/*
 * Scrub/resilver reads.  In order to store the contents of the skip sectors
 * an additional ABD is allocated.  The columns are handled in the same way
 * as a full stripe write except instead of using the zero ABD the newly
 * allocated skip ABD is used to back the skip sectors.  In all cases the
 * data ABD must be the same size as the parity ABDs.
 *
 * Both the rm->rm_cols and rc->rc_size are increased to allow the parity
 * to be calculated for the stripe.
 */
static void
vdev_draid_map_alloc_scrub(zio_t *zio, raidz_map_t *rm)
{
	uint64_t skip_size = 1ULL << zio->io_vd->vdev_top->vdev_ashift;
	uint64_t abd_off = 0;

	ASSERT3U(zio->io_type, ==, ZIO_TYPE_READ);

	rm->rm_abd_skip = abd_alloc_linear(rm->rm_nskip * skip_size, B_TRUE);

	for (uint64_t c = rm->rm_firstdatacol; c < rm->rm_scols; c++) {
		raidz_col_t *rc = &rm->rm_col[c];
		int skip_idx = c - rm->rm_skipstart;

		if (rm->rm_skipstart == 0 || c < rm->rm_skipstart) {
			rc->rc_abd = abd_get_offset_size(zio->io_abd,
			    abd_off, rc->rc_size);
		} else if (c < rm->rm_cols) {
			rc->rc_abd = abd_alloc_gang_abd();
			abd_gang_add(rc->rc_abd, abd_get_offset_size(
			    zio->io_abd, abd_off, rc->rc_size), B_TRUE);
			abd_gang_add(rc->rc_abd, abd_get_offset_size(
			    rm->rm_abd_skip, skip_idx * skip_size, skip_size),
			    B_TRUE);
		} else {
			rc->rc_abd = abd_get_offset_size(rm->rm_abd_skip,
			    skip_idx * skip_size, skip_size);
		}

		uint64_t abd_size = abd_get_size(rc->rc_abd);
		ASSERT3U(abd_size, ==, abd_get_size(rm->rm_col[0].rc_abd));

		abd_off += rc->rc_size;
		rc->rc_size = abd_size;
	}

	rm->rm_cols = rm->rm_scols;
}

/*
 * Normal reads.  This is the common case, it is sufficient to map the zio's
 * ABD in to the raid map columns.  If the checksum cannot be verified the
 * raid map is expanded by vdev_draid_map_include_skip_sectors() to allow
 * reconstruction from parity data.
 */
static void
vdev_draid_map_alloc_read(zio_t *zio, raidz_map_t *rm)
{
	uint64_t abd_off = 0;

	ASSERT3U(zio->io_type, ==, ZIO_TYPE_READ);

	for (uint64_t c = rm->rm_firstdatacol; c < rm->rm_cols; c++) {
		raidz_col_t *rc = &rm->rm_col[c];

		rc->rc_abd = abd_get_offset_size(zio->io_abd, abd_off,
		    rc->rc_size);
		abd_off += rc->rc_size;
	}
}

static uint64_t
vdev_draid_logical_to_physical(vdev_t *vd, uint64_t loff)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	/* The starting dRAID (parent) vdev sector of the block. */
	const uint64_t ashift = vd->vdev_top->vdev_ashift;
	const uint64_t b = loff >> ashift;

	/* The zio's size in units of the vdev's minimum sector size. */
	const uint64_t slice = DRAID_SLICESIZE >> ashift;

	/*
	 * We cycle through the disk permutations every SLICE * groupsz * ndisks
	 * chunk of address space. This results in ndisks groups in each
	 * permutation chunk (and guarantees alignment). So, for example,
	 * if our slice size is 16MB, our group size is 10, and there are 13
	 * data drives in the draid, then we will change permutation every
	 * 2.08GB and each disk will have 150MB of data per chunk.
	 */
	uint64_t ndata = vdc->vdc_data;
	uint64_t groupsz = ndata + vdc->vdc_parity;
	uint64_t ndisks = vdc->vdc_children - vdc->vdc_spares;
	uint64_t offset = b % (slice * ndisks * groupsz);
	uint64_t group = b / (slice * groupsz);
	uint64_t groupstart = (group * groupsz) % ndisks;

	ASSERT3U(groupsz, >, 0);
	ASSERT3U(groupstart + groupsz, <=, ndisks + groupstart);

	/*
	 * If there is less than groupsz drives available after the group
	 * start, the group is going to wrap onto the next row. 'wrap' is the
	 * group disk number that starts on the next row.
	 */
	uint64_t wrap = groupsz;
	if (groupstart + groupsz > ndisks)
		wrap = ndisks - groupstart;

	/* now make offset a sector offset within a group chunk */
	offset = offset % (slice * groupsz);
	ASSERT0(offset % groupsz);

	/*
	 * Find the starting byte offset on each child vdev:
	 * - each permutation covers a groupsz * slice portion of the disk
	 * - within a permutation there are ndisks groups spread over groupsz
	 *   rows, where each row covers a slice portion of the disk
	 * - so we need to find the row where this IO group target begins
	 */
	uint64_t perm = group / ndisks;
	uint64_t row = (perm * groupsz) + ((group % ndisks) * groupsz) / ndisks;
	return (((slice * row) + (offset / groupsz)) << ashift);
}

/*
 * Allocate the raidz mapping to be applied to the dRAID I/O.  The parity
 * calculations for dRAID are identical to raidz.  The only caveat is that
 * dRAID always allocates a full stripe width.  Zero filled skip sectors
 * are added to pad out the buffer and must be written to disk.
 */
static raidz_map_t *
vdev_draid_map_alloc(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	/* The starting dRAID (parent) vdev sector of the block. */
	const uint64_t ashift = vd->vdev_top->vdev_ashift;
	const uint64_t b = zio->io_offset >> ashift;

	/* The zio's size in units of the vdev's minimum sector size. */
	const uint64_t psize = zio->io_size >> ashift;
	const uint64_t slice = DRAID_SLICESIZE >> ashift;

	/*
	 * We cycle through the disk permutations every SLICE * groupsz * ndisks
	 * chunk of address space. This results in ndisks groups in each
	 * permutation chunk (and guarantees alignment). So, for example,
	 * if our slice size is 16MB, our group size is 10, and there are 13
	 * data drives in the draid, then we will change permutation every
	 * 2.08GB and each disk will have 150MB of data per chunk.
	 */
	uint64_t ndata = vdc->vdc_data;
	uint64_t groupsz = ndata + vdc->vdc_parity;
	uint64_t ndisks = vdc->vdc_children - vdc->vdc_spares;
	uint64_t offset = b % (slice * ndisks * groupsz);
	uint64_t group = b / (slice * groupsz);
	uint64_t groupstart = (group * groupsz) % ndisks;

	ASSERT3U(groupsz, >, 0);
	ASSERT3U(groupstart + groupsz, <=, ndisks + groupstart);

	/*
	 * If there is less than groupsz drives available after the group
	 * start, the group is going to wrap onto the next row. 'wrap' is the
	 * group disk number that starts on the next row.
	 */
	uint64_t wrap = groupsz;
	if (groupstart + groupsz > ndisks)
		wrap = ndisks - groupstart;

	/* now make offset a sector offset within a group chunk */
	offset = offset % (slice * groupsz);
	ASSERT0(offset % groupsz);

	/*
	 * Find the starting byte offset on each child vdev:
	 * - each permutation covers a groupsz * slice portion of the disk
	 * - within a permutation there are ndisks groups spread over groupsz
	 *   rows, where each row covers a slice portion of the disk
	 * - so we need to find the row where this IO group target begins
	 */
	uint64_t perm = group / ndisks;
	uint64_t row = (perm * groupsz) + ((group % ndisks) * groupsz) / ndisks;
	uint64_t o = ((slice * row) + (offset / groupsz)) << ashift;

	/*
	 * "Quotient": The number of data sectors for this stripe on all but
	 * the "big column" child vdevs that also contain "remainder" data.
	 */
	uint64_t q = psize / ndata;

	/*
	 * "Remainder": The number of partial stripe data sectors in this I/O.
	 * This will add a sector to some, but not all, child vdevs.
	 */
	uint64_t r = psize - q * ndata;

	/* The number of "big columns" - those which contain remainder data. */
	uint64_t bc = (r == 0 ? 0 : r + vdc->vdc_parity);
	ASSERT3U(bc, <, groupsz);

	/* The total number of data and parity sectors for this I/O. */
	uint64_t tot = psize + (vdc->vdc_parity * (q + (r == 0 ? 0 : 1)));

	raidz_map_t *rm = kmem_alloc(offsetof(raidz_map_t, rm_col[groupsz]),
	    KM_SLEEP);

	rm->rm_cols = (q == 0) ? bc : groupsz;
	rm->rm_scols = groupsz;
	rm->rm_bigcols = bc;
	rm->rm_skipstart = bc;
	rm->rm_missingdata = 0;
	rm->rm_missingparity = 0;
	rm->rm_firstdatacol = vdc->vdc_parity;
	rm->rm_abd_copy = NULL;
	rm->rm_abd_skip = NULL;
	rm->rm_reports = 0;
	rm->rm_freed = 0;
	rm->rm_ecksuminjected = 0;
	rm->rm_include_skip = 1;

	uint64_t *base, iter, asize = 0;
	vdev_draid_get_perm(vdc, perm, &base, &iter);
	for (uint64_t c = 0; c < groupsz; c++) {
		uint64_t i = (groupstart + c) % ndisks;

		/* increment the offset if we wrap to the next row */
		if (c == wrap)
			o += slice << ashift;

		rm->rm_col[c].rc_devidx =
		    vdev_draid_permute_id(vdc, base, iter, i);
		rm->rm_col[c].rc_offset = o;
		rm->rm_col[c].rc_abd = NULL;
		rm->rm_col[c].rc_gdata = NULL;
		rm->rm_col[c].rc_error = 0;
		rm->rm_col[c].rc_tried = 0;
		rm->rm_col[c].rc_skipped = 0;
		rm->rm_col[c].rc_repair = 0;

		/* resolve dspare to actual leaf child */
		vdev_t *cvd = vd->vdev_child[rm->rm_col[c].rc_devidx];
		if (cvd->vdev_ops == &vdev_draid_spare_ops) {
			cvd = vdev_draid_spare_get_child(cvd, o);
			ASSERT3P(cvd->vdev_parent, ==, vd);
			rm->rm_col[c].rc_devidx = cvd->vdev_id;
		}

		if (c >= rm->rm_cols)
			rm->rm_col[c].rc_size = 0;
		else if (c < bc)
			rm->rm_col[c].rc_size = (q + 1) << ashift;
		else
			rm->rm_col[c].rc_size = q << ashift;

		asize += rm->rm_col[c].rc_size;
	}

	ASSERT3U(asize, ==, tot << ashift);
	rm->rm_asize = roundup(asize, groupsz << ashift);
	rm->rm_nskip = roundup(tot, groupsz) - tot;
	IMPLY(bc > 0, rm->rm_nskip == groupsz - bc);
	ASSERT3U(rm->rm_asize - asize, ==, rm->rm_nskip << ashift);
	ASSERT3U(rm->rm_nskip, <, ndata);
	ASSERT3U((offset << ashift) + rm->rm_asize, <=, (slice * groupsz) << ashift);

	/* Allocate buffers for the parity columns */
	/* XXX - could we delay/avoid this on read until reconstruction? */
	for (uint64_t c = 0; c < rm->rm_firstdatacol; c++) {
		raidz_col_t *rc = &rm->rm_col[c];
		rc->rc_abd = abd_alloc_linear(rc->rc_size, B_TRUE);
	}

	/*
	 * Map buffers for data columns and allocate/map buffers for skip
	 * sectors.  There are three distinct cases for dRAID which are
	 * required to support sequential rebuild.
	 */
	if (zio->io_type == ZIO_TYPE_WRITE) {
		vdev_draid_map_alloc_write(zio, rm);
	} else if ((rm->rm_nskip > 0) &&
	    (zio->io_flags & (ZIO_FLAG_SCRUB | ZIO_FLAG_RESILVER))) {
		vdev_draid_map_alloc_scrub(zio, rm);
	} else {
		ASSERT3U(zio->io_type, ==, ZIO_TYPE_READ);
		vdev_draid_map_alloc_read(zio, rm);
	}

	rm->rm_ops = vdev_raidz_math_get_ops();
	zio->io_vsd = rm;
	zio->io_vsd_ops = &vdev_raidz_vsd_ops;
#if 0
#ifndef _KERNEL
printf("map %llu %d ->", (u_longlong_t)zio->io_offset, (int)zio->io_size);
for (int i = 0; i < rm->rm_scols; i++)
	printf(" %d: <%llu,%d>", (int)rm->rm_col[i].rc_devidx, (u_longlong_t)rm->rm_col[i].rc_offset, (int)rm->rm_col[i].rc_size);
printf("\n");
#endif
#endif
	return (rm);
}

/*
 * Converts a dRAID read raidz_map_t to a dRAID scrub raidz_map_t.  The
 * key difference is that an ABD is allocated to back up skip sectors
 * so they may be read, verified, and repaired if needed.
 */
void
vdev_draid_map_include_skip_sectors(zio_t *zio)
{
	raidz_map_t *rm = zio->io_vsd;

	ASSERT3U(zio->io_type, ==, ZIO_TYPE_READ);
	ASSERT3P(rm->rm_abd_skip, ==, NULL);

	for (uint64_t c = rm->rm_firstdatacol; c < rm->rm_cols; c++) {
		ASSERT(!abd_is_gang(rm->rm_col[c].rc_abd));
		abd_put(rm->rm_col[c].rc_abd);
	}

	vdev_draid_map_alloc_scrub(zio, rm);
}

/*
 * Return the asize of a full dRAID slice across all child vdevs.
 * Each disk has groupsz "rows" in a dRAID slice.
 */
static inline uint64_t
vdev_draid_permutation_asize(vdev_draid_config_t *vdc)
{
	uint64_t groupsz =
	    (vdc->vdc_data + vdc->vdc_parity) << DRAID_SLICESHIFT;
	return ((vdc->vdc_children - vdc->vdc_spares) * groupsz);
}

/*
 * Converts a logical offset to the corresponding group number.
 */
uint64_t
vdev_draid_offset_to_group(const vdev_t *vd, uint64_t offset)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t groupsz = 
	    (vdc->vdc_data + vd->vdev_nparity) << DRAID_SLICESHIFT;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	return (offset / groupsz);
}

/*
 * Converts a group number to the logical starting offset for that group.
 */
uint64_t
vdev_draid_group_to_offset(const vdev_t *vd, uint64_t group)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	uint64_t groupsz = 
	    (vdc->vdc_data + vd->vdev_nparity) << DRAID_SLICESHIFT;
	return (group * groupsz);
}

/*
 * Given a offset into a draid, compute a group aligned offset.
 */
uint64_t
vdev_draid_get_astart(const vdev_t *vd, const uint64_t start)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t align = (vdc->vdc_data + vd->vdev_nparity) << vd->vdev_ashift;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	return (roundup(start, align));
}

/*
 * Check if there's enough space left for the block in the group.  The passed
 * starting offset must be properly align.  If the block fits return an
 * updated 'asizep' and the starting offset to be used, this may be in the
 * next group if there wasn't space in the original group.
 */
uint64_t
vdev_draid_check_block(const vdev_t *vd, uint64_t start, uint64_t *asizep)
{
	uint64_t group = vdev_draid_offset_to_group(vd, start);
	uint64_t asize = vdev_draid_psize_to_asize(vd, start, *asizep);
	uint64_t end = start + asize - 1;

	/* An allocation may not span metaslabs. */
	if (start >> vd->vdev_ms_shift != end >> vd->vdev_ms_shift)
		return (-1ULL);

	/* A block is good if it does not cross a group boundary. */
	if (group == vdev_draid_offset_to_group(vd, end)) {
		ASSERT3U(start, ==, vdev_draid_get_astart(vd, start));
		*asizep = asize;
		return (start);
	}

	/* Advance to the next group. */
	group++;
	start = vdev_draid_group_to_offset(vd, group);
	asize = vdev_draid_psize_to_asize(vd, start, *asizep);
	end = start + asize - 1;

	ASSERT3U(group, ==, vdev_draid_offset_to_group(vd, end));

	*asizep = asize;

	return (start);
}

/*
 * A dRAID spare does not fit into the DTL model. While it has child vdevs,
 * there is no redundancy among them, and the effective child vdev is
 * determined by offset. Moreover, DTLs of a child vdev before the spare
 * becomes active are invalid because the spare blocks were not in use yet.
 *
 * Here we are essentially doing a vdev_dtl_reassess() on the fly, by replacing
 * a dRAID spare with the child vdev under the offset. Note that it is a
 * recursive process because the child vdev can be another dRAID spare, and so
 * on.
 */
boolean_t
vdev_draid_missing(vdev_t *vd, uint64_t offset, uint64_t txg, uint64_t size)
{
	if (vdev_dtl_contains(vd, DTL_MISSING, txg, size))
		return (B_TRUE);

	if (vd->vdev_ops == &vdev_draid_spare_ops) {
		vd = vdev_draid_spare_get_child(vd, offset);
		if (vd == NULL)
			return (B_TRUE);
	}

	if (vd->vdev_ops != &vdev_spare_ops)
		return (vdev_dtl_contains(vd, DTL_MISSING, txg, size));

	if (vdev_dtl_contains(vd, DTL_MISSING, txg, size))
		return (B_TRUE);

	for (int c = 0; c < vd->vdev_children; c++) {
		vdev_t *cvd = vd->vdev_child[c];

		if (!vdev_readable(cvd))
			continue;

		if (!vdev_draid_missing(cvd, offset, txg, size))
			return (B_FALSE);
	}

	return (B_TRUE);
}

/*
 * Determine if the vdev is readable at the given offset.
 */
boolean_t
vdev_draid_readable(vdev_t *vd, uint64_t offset)
{
	if (vd->vdev_ops == &vdev_draid_spare_ops) {
		vd = vdev_draid_spare_get_child(vd, offset);
		if (vd == NULL)
			return (B_FALSE);
	}

	return (vdev_readable(vd));
}

/*
 * Returns B_TRUE if the guid exists in the vdev tree rooted at 'vd'.
 */
static boolean_t
vdev_draid_guid_exists(vdev_t *vd, uint64_t guid, uint64_t offset)
{
	if (vd->vdev_ops == &vdev_draid_spare_ops) {
		vd = vdev_draid_spare_get_child(vd, offset);
		if (vd == NULL)
			return (B_FALSE);
	}

	if (vd->vdev_ops->vdev_op_leaf)
		return (vd->vdev_guid == guid);

	for (int c = 0; c < vd->vdev_children; c++)
		if (vdev_draid_guid_exists(vd->vdev_child[c], guid, offset))
			return (B_TRUE);

	return (B_FALSE);
}

/*
 * Returns the first distributed spare found under the provided vdev tree.
 */
static vdev_t *
vdev_draid_find_spare(vdev_t *vd)
{
	if (vd->vdev_ops == &vdev_draid_spare_ops)
		return (vd);

	for (int c = 0; c < vd->vdev_children; c++) {
		vdev_t *svd = vdev_draid_find_spare(vd->vdev_child[c]);
		if (svd != NULL)
			return (svd);
	}

	return (NULL);
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

/*
 * Determine if the dRAID block at the logical offset and size would be
 * degraded if the fault_vdev was unavailable.
 */
boolean_t
vdev_draid_group_degraded(vdev_t *vd, vdev_t *fault_vdev, uint64_t offset,
    uint64_t size)
{
	uint64_t *base, iter;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	uint64_t group = vdev_draid_offset_to_group(vd, offset);
	ASSERT3U(group, ==,
	    vdev_draid_offset_to_group(vd, offset + size - 1));

	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t perm = group / vdc->vdc_groups;
	vdev_draid_get_perm(vdc, perm, &base, &iter);

	/*
	 * For the purposes of vdev_draid_vd_degraded(), we need to
	 * find the disk offset for the permutation slice in order to
	 * determine the mapping of distributed spare drives.
	 */
	uint64_t groupsz = vdc->vdc_data + vdc->vdc_parity;
	uint64_t coff = perm * (groupsz << DRAID_SLICESHIFT);
	for (int c = 0; c < vdc->vdc_children; c++) {
		uint64_t cid = vdev_draid_permute_id(vdc, base, iter, c);
		vdev_t *cvd = vd->vdev_child[cid];

		if (vdev_draid_vd_degraded(cvd, fault_vdev, coff))
			return (B_TRUE);
	}
	return (B_FALSE);
}

/*
 * Allocate memory for and copy the dRAID base permutations.
 */
static uint64_t *
vdev_draid_create_base_permutations(const uint8_t *perms,
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

/*
 * Create the vdev_draid_config_t structure from dRAID configuration stored
 * as an nvlist in the pool configuration.
 */
static vdev_draid_config_t *
vdev_draid_config_create(vdev_t *vd)
{
	uint_t c;
	uint8_t *perms = NULL;
	nvlist_t *nvl = vd->vdev_cfg;

	if (vdev_draid_config_validate(nvl, 0, vd->vdev_nparity, 0,
	    vd->vdev_children) != DRAIDCFG_OK) {
		return (NULL);
	}

	vdev_draid_config_t *vdc = kmem_alloc(sizeof (*vdc), KM_SLEEP);
	vdc->vdc_children = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_CHILDREN);
	vdc->vdc_groups = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_GROUPS);
	vdc->vdc_data = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_DATA);
	vdc->vdc_parity = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_PARITY);
	vdc->vdc_spares = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_SPARES);
	vdc->vdc_bases = fnvlist_lookup_uint64(nvl, ZPOOL_CONFIG_DRAIDCFG_BASE);

	VERIFY0(nvlist_lookup_uint8_array(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_PERM, &perms, &c));

	vdc->vdc_base_perms = vdev_draid_create_base_permutations(perms, vdc);

	return (vdc);
}

/*
 * Destroy the vdev_draid_config_t structure.
 */
static void
vdev_draid_config_destroy(vdev_draid_config_t *vdc)
{
	size_t sz = sizeof (uint64_t) * vdc->vdc_bases * vdc->vdc_children;

	kmem_free(vdc->vdc_base_perms, P2ROUNDUP(sz, PAGESIZE));
	kmem_free(vdc, sizeof (*vdc));
}

/*
 * Find the smallest child asize and largest sector size to calculate the
 * available capacity.  Distributed spares are ignored since their capacity
 * is also based of the minimum child size in the top-level dRAID.
 */
static void
vdev_draid_calculate_asize(vdev_t *vd, uint64_t *asizep,
    uint64_t *max_asizep, uint64_t *ashiftp)
{
	uint64_t asize = 0, max_asize = 0, ashift = 0;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	for (int c = 0; c < vd->vdev_children; c++) {
		vdev_t *cvd = vd->vdev_child[c];

		if (cvd->vdev_ops != &vdev_draid_spare_ops) {
			asize = MIN(asize - 1, cvd->vdev_asize - 1) + 1;
			max_asize = MIN(max_asize - 1,
			    cvd->vdev_max_asize - 1) + 1;
			ashift = MAX(ashift, cvd->vdev_ashift);
		}
	}

	*asizep = asize;
	*max_asizep = max_asize;
	*ashiftp = ashift;
}

/*
 * Close a top-level dRAID vdev.
 */
static void
vdev_draid_close(vdev_t *vd)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	for (int c = 0; c < vd->vdev_children; c++)
		vdev_close(vd->vdev_child[c]);

	if (vd->vdev_reopening || vdc == NULL)
		return;

	vdev_draid_config_destroy(vdc);
	vd->vdev_tsd = NULL;
}

/*
 * Open spare vdevs.
 */
static boolean_t
vdev_draid_open_spares(vdev_t *vd)
{
	return (vd->vdev_ops == &vdev_draid_spare_ops ||
	    vd->vdev_ops == &vdev_replacing_ops ||
	    vd->vdev_ops == &vdev_spare_ops);
}

/*
 * Open all children, excluding spares.
 */
static boolean_t
vdev_draid_open_children(vdev_t *vd)
{
	return (!vdev_draid_open_spares(vd));
}

/*
 * Open a top-level dRAID vdev.
 */
static int
vdev_draid_open(vdev_t *vd, uint64_t *asize, uint64_t *max_asize,
    uint64_t *ashift)
{
	vdev_draid_config_t *vdc;
	uint64_t nparity = vd->vdev_nparity;
	int open_errors = 0;

	if (vd->vdev_tsd != NULL) {
		/*
		 * When reopening all children must be closed and opened.
		 * The dRAID configuration itself remains valid and care
		 * is taken to avoid destroying and recreating it.
		 */
		ASSERT(vd->vdev_reopening);
		vdc = vd->vdev_tsd;
	} else {
		if (nparity > VDEV_RAIDZ_MAXPARITY ||
		    vd->vdev_children < nparity + 1) {
			vd->vdev_stat.vs_aux = VDEV_AUX_BAD_LABEL;
			return (SET_ERROR(EINVAL));
		}

		vdc = vdev_draid_config_create(vd);
		if (vdc == NULL)
			return (SET_ERROR(EINVAL));

		/*
		 * Used to generate dRAID spare names and calculate the min
		 * asize even when the vdev_draid_config_t is not available
		 * because the open fails below and the vdc is freed.
		 */
		vd->vdev_spares = vdc->vdc_spares;
		vd->vdev_groups = vdc->vdc_groups;
		vd->vdev_tsd = vdc;
	}

	/*
	 * First open the normal children then the distributed spares.  This
	 * ordering is important to ensure the distributed spares calculate
	 * the correct psize in the event that the dRAID vdevs were expanded.
	 */
	vdev_open_children_subset(vd, vdev_draid_open_children);
	vdev_open_children_subset(vd, vdev_draid_open_spares);

	/* Verify enough of the children are available to continue. */
	for (int c = 0; c < vd->vdev_children; c++) {
		if (vd->vdev_child[c]->vdev_open_error != 0) {
			if ((++open_errors) > nparity) {
				vd->vdev_stat.vs_aux = VDEV_AUX_NO_REPLICAS;
				return (SET_ERROR(ENXIO));
			}
		}
	}

	vdev_draid_calculate_asize(vd, asize, max_asize, ashift);

	*asize = *asize * (vd->vdev_children - vdc->vdc_spares);
	*max_asize = *max_asize * (vd->vdev_children - vdc->vdc_spares);

	return (0);
}

/*
 * dRAID asize is psize rounded up to a full group stripe multiple
 * plus parity.
 */
static uint64_t
vdev_draid_psize_to_asize(const vdev_t *vd, uint64_t offset, uint64_t psize)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t ashift = vd->vdev_top->vdev_ashift;
	uint64_t nparity = vd->vdev_nparity;
	uint64_t asize = ((psize - 1) >> ashift) + 1;
	uint64_t ndata = vdc->vdc_data;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);
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
	return (vdev_draid_psize_to_asize(vd, offset, psize));
}

/*
 * Deflate the asize to the psize for a block at the provided offset.
 * This involves stripping parity.
 */
uint64_t
vdev_draid_asize_to_psize(vdev_t *vd, uint64_t asize, uint64_t offset)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t dataplusparity = vdc->vdc_data + vd->vdev_nparity;

	return ((asize / dataplusparity) * vdc->vdc_data);
}

/*
 * Return the asize of the largest block which can be reconstructed.
 */
uint64_t
vdev_draid_max_rebuildable_asize(vdev_t *vd, uint64_t offset, uint64_t maxpsize)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t ashift = vd->vdev_top->vdev_ashift;
	uint64_t ndata = vdc->vdc_data;

	/*
	 * When the maxpsize >> ashift does not divide evenly by the number
	 * of data drives, the remainder must be discarded. Otherwise the skip
	 * sectors will cause vdev_draid_asize_to_psize() to get a psize larger
	 * than the maximum allowed block size.
	 */
	maxpsize >>= ashift;
	maxpsize /= ndata;
	maxpsize *= ndata;
	maxpsize <<= ashift;

	return (vdev_draid_psize_to_asize(vd, offset, maxpsize));
}

/*
 * Returns the number of active distributed spares in the dRAID vdev tree.
 */
static int
vdev_draid_active_spares(vdev_t *vd)
{
	int spares = 0;

	if (vd->vdev_ops == &vdev_draid_spare_ops)
		return (1);

	for (int c = 0; c < vd->vdev_children; c++)
		spares += vdev_draid_active_spares(vd->vdev_child[c]);

	return (spares);
}

/*
 * The DVA needs to be resilvered when:
 *   1. There are multiple active distributed spares.
 *      See the comment in vdev_draid_io_start(); or
 *   2. The DVA is within the missing range of the DTL; or
 *   3. The DVA is degraded and needs to be resilvered.
 */
static boolean_t
vdev_draid_need_resilver(vdev_t *vd, const dva_t *dva, size_t psize,
    uint64_t phys_birth)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t offset = DVA_GET_OFFSET(dva);

	if (vdc->vdc_spares > 1 && vdev_draid_active_spares(vd) > 1)
		return (B_TRUE);

	if (!vdev_dtl_contains(vd, DTL_PARTIAL, phys_birth, 1))
		return (B_FALSE);

	return (vdev_draid_group_degraded(vd, NULL, offset, psize));
}

static void
vdev_draid_io_verify(zio_t *zio, raidz_map_t *rm, int col)
{
#ifdef ZFS_DEBUG
	vdev_t *vd = zio->io_vd;

	range_seg64_t logical_rs, physical_rs, remain_rs;
	logical_rs.rs_start = zio->io_offset;
	logical_rs.rs_end = logical_rs.rs_start +
	    vdev_draid_asize(zio->io_vd, zio->io_offset, zio->io_size);

	raidz_col_t *rc = &rm->rm_col[col];
	vdev_t *cvd = vd->vdev_child[rc->rc_devidx];

	vdev_xlate(cvd, &logical_rs, &physical_rs, &remain_rs);
	ASSERT3U(rc->rc_offset, ==, physical_rs.rs_start);
	ASSERT3U(rc->rc_offset, <, physical_rs.rs_end);
	ASSERT3U(rc->rc_offset + rc->rc_size, ==, physical_rs.rs_end);
	ASSERT(vdev_xlate_is_empty(&remain_rs));
#endif
}

/*
 * Start an IO operation on a dRAID vdev
 *
 * Outline:
 * - For write operations:
 *   1. Generate the parity data
 *   2. Create child zio write operations to each column's vdev, for both
 *      data and parity.  A gang ABD is allocated by vdev_draid_map_alloc()
 *      if a skip sector needs to be added to a column.
 * - For read operations:
 *   1. The vdev_draid_map_alloc() function will create a minimal raidz
 *      mapping for the read based on the zio->io_flags.  There are two
 *      possible mappings either 1) a normal read, or 2) a scrub/resilver.
 *   2. Create the zio read operations.  This will include all parity
 *      columns and skip sectors for a scrub/resilver.
 */
static void
vdev_draid_io_start(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;
	raidz_map_t *rm;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	rm = vdev_draid_map_alloc(zio);

	if (zio->io_type == ZIO_TYPE_WRITE) {
		vdev_raidz_generate_parity(rm);

		/*
		 * Unlike raidz, skip sectors are zero filled and all
		 * columns must always be written.
		 */
		for (int c = 0; c < rm->rm_scols; c++) {
			raidz_col_t *rc = &rm->rm_col[c];
			vdev_t *cvd = vd->vdev_child[rc->rc_devidx];

			/*
			 * Verify physical to logical translation.
			 */
			vdev_draid_io_verify(zio, rm, c);

			zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
			    rc->rc_offset, rc->rc_abd, rc->rc_size,
			    zio->io_type, zio->io_priority, 0,
			    vdev_raidz_child_done, rc));
		}

		zio_execute(zio);
		return;
	}

	ASSERT(zio->io_type == ZIO_TYPE_READ);

	/* Scrub/resilver must verify skip sectors => expanded raidz map */
	IMPLY(zio->io_flags & (ZIO_FLAG_SCRUB | ZIO_FLAG_RESILVER),
	    rm->rm_cols == rm->rm_scols);

	/* Sequential rebuild must do IO at redundancy group boundary. */
	IMPLY(zio->io_priority == ZIO_PRIORITY_REBUILD, rm->rm_nskip == 0);

	/*
	 * Iterate over the columns in reverse order so that we hit the parity
	 * last.  Any errors along the way will force us to read the parity.
	 * For scrub/resilver IOs which verify skip sectors, a gang ABD will
	 * have been allocated to store them and rc->rc_size is increased.
	 */
	for (int c = rm->rm_cols - 1; c >= 0; c--) {
		raidz_col_t *rc = &rm->rm_col[c];
		vdev_t *cvd = vd->vdev_child[rc->rc_devidx];
		vdev_t *svd;

		if (!vdev_draid_readable(cvd, rc->rc_offset)) {
			if (c >= rm->rm_firstdatacol)
				rm->rm_missingdata++;
			else
				rm->rm_missingparity++;
			rc->rc_error = SET_ERROR(ENXIO);
			rc->rc_tried = 1;
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

		/*
		 * If this child is a distributed spare and we're resilvering
		 * then this offset might reside on the vdev being replaced.
		 * In which case this data must be written to the new device.
		 * Failure to do so would result in checksum errors when the
		 * old device is detached and the pool is scrubbed.
		 */
		if (zio->io_flags & ZIO_FLAG_RESILVER &&
		    (svd = vdev_draid_find_spare(cvd)) != NULL) {
			svd = vdev_draid_spare_get_child(svd, rc->rc_offset);
			if (svd && (svd->vdev_ops == &vdev_spare_ops ||
			    svd->vdev_ops == &vdev_replacing_ops)) {
				rc->rc_repair = 1;
			}
		}

		if (c >= rm->rm_firstdatacol || rm->rm_missingdata > 0 ||
		    (zio->io_flags & (ZIO_FLAG_SCRUB | ZIO_FLAG_RESILVER))) {
			zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
			    rc->rc_offset, rc->rc_abd, rc->rc_size,
			    zio->io_type, zio->io_priority, 0,
			    vdev_raidz_child_done, rc));
		}
	}

	zio_execute(zio);
}

/*
 * Complete an IO operation on a dRAID vdev.  The raidz logic can be applied
 * to dRAID since the layout is fully described by the raidz_map_t.
 */
static void
vdev_draid_io_done(zio_t *zio)
{
	vdev_raidz_io_done(zio);
}

static void
vdev_draid_state_change(vdev_t *vd, int faulted, int degraded)
{
	vdev_raidz_state_change(vd, faulted, degraded);
}

static void
vdev_draid_xlate(vdev_t *cvd, const range_seg64_t *in, range_seg64_t *res)
{
	vdev_t *raidvd = cvd->vdev_parent;
	ASSERT(raidvd->vdev_ops == &vdev_draid_ops);

	vdev_draid_config_t *vdc = raidvd->vdev_tsd;
	uint64_t ashift = raidvd->vdev_top->vdev_ashift;
	uint64_t children = vdc->vdc_children;
	uint64_t spares = vdc->vdc_spares;

	/* Make sure the offsets are block-aligned */
	ASSERT0(in->rs_start % (1 << ashift));
	ASSERT0(in->rs_end % (1 << ashift));
	uint64_t b_start = in->rs_start >> ashift;
	uint64_t b_end = in->rs_end >> ashift;
	uint64_t b_size = b_end - b_start;

	/*
	 * Translation requests can never span three or more slices.  Doing so
	 * could result in distributed spare space being incorrectly included
	 * in the physical range.  Therefore, vdev_xlate() limits the input
	 * size to a single group.  This is stricter than absolutely necessary
	 * but helps simplify the logic below.
	 */
	ASSERT3U(vdev_draid_offset_to_group(raidvd, in->rs_start), ==,
	    vdev_draid_offset_to_group(raidvd, in->rs_end - 1));

	/*
	 * Figure out in which group the IO will fall and use it to set the
	 * group start index, size, and number of columns.
	 */
	uint64_t width = vdc->vdc_data + vdc->vdc_parity;
	uint64_t groupsz = width * (DRAID_SLICESIZE >> ashift);
	uint64_t group = b_start / groupsz;
	uint64_t groupstart = (group * width) % (children - spares);

	ASSERT3U(vdc->vdc_groups, >, 0);
	ASSERT3U(groupsz, >, 0);

	/* Set the starting row for the permutation group. */
	uint64_t perm = group / (children - spares);
	uint64_t start_row = (perm * width) +
	    ((group % (children - spares)) * width) / (children - spares);
	uint64_t end_row = start_row;
	uint64_t b_offset = b_start % groupsz;
	uint64_t start = vdev_draid_logical_to_physical(raidvd, in->rs_start);
	uint64_t end = start;

	ASSERT0(b_offset % width);

	uint64_t *base, iter, id;
	vdev_draid_get_perm(vdc, perm, &base, &iter);

	/*
	 * Check if the passed child falls within the group.  If it does
	 * update the start_row and end_row to reflect the physical range.
	 * Otherwise, leave them unmodified which will result in an empty
	 * (zero-length) physical range being returned.
	 */
	for (uint64_t i = 0; i < width; i++) {
		uint64_t c = (groupstart + i) % (children - spares);

		if (c == 0 && i != 0) {
			/* the group wrapped, increment the start row */
			start_row += 1;
			end_row += 1;
			start += DRAID_SLICESIZE;
			end = start;
		}
		id = vdev_draid_permute_id(vdc, base, iter, c);
		if (id == cvd->vdev_id) {
			if (b_offset > 0) {
				ASSERT3U(b_offset, >, i);
				/* XXX - this seems wrong. Why is 'i' involved?
				start_row += ((b_offset - i - 1) / width) + 1;
				*/
				start_row += ((b_offset - 1) / width) + 1;
			}
			ASSERT3U(b_size, >, 0);
			end_row += ((b_offset + b_size - 1) / width) + 1;
			end = start + ((b_size / width) << ashift);
			break;
		}
	}
/*
	res->rs_start = start_row << ashift;
	res->rs_end = end_row << ashift;
*/
	res->rs_start = start;
	res->rs_end = end;

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
	.vdev_op_type = VDEV_TYPE_DRAID,
	.vdev_op_leaf = B_FALSE,
};


/*
 * A dRAID distributed spare is a virtual leaf vdev which is included in the
 * parent dRAID configuration.  The last N columns of the dRAID permutation
 * table are used to determine on which dRAID children a specific offset
 * should be written.  These spare leaf vdevs can only be used to replace
 * faulted children in the same dRAID configuration.
 */

/*
 * Distributed spare state.  All fields are set when the distributed spare is
 * first opened and are immutable.
 */
typedef struct {
	vdev_t *vds_draid_vdev;		/* top-level parent dRAID vdev */
	uint64_t vds_spare_id;		/* spare id (0 - vdc->vdc_spares-1) */
} vdev_draid_spare_t;

/*
 * Returns the parent dRAID vdev to which the distributed spare belongs.
 * This may be safely called even when the vdev is not open.
 */
vdev_t *
vdev_draid_spare_get_parent(vdev_t *vd)
{
	uint64_t parity, groups, spares, vdev_id, spare_id;
	vdev_t *rvd = vd->vdev_spa->spa_root_vdev;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_spare_ops);
	if (vdev_draid_spare_values(vd->vdev_path, &parity, &groups,
	    &spares, &vdev_id, &spare_id) != 0) {
		return (NULL);
	}

	if (vdev_id >= rvd->vdev_children)
		return (NULL);

	return (rvd->vdev_child[vdev_id]);
}

/*
 * A dRAID space is active when it's the child of a vdev using the
 * vdev_spare_ops, vdev_replacing_ops or vdev_draid_ops.
 */
boolean_t
vdev_draid_spare_is_active(vdev_t *vd)
{
	vdev_t *pvd = vd->vdev_parent;

	if (pvd != NULL && (pvd->vdev_ops == &vdev_spare_ops ||
	    pvd->vdev_ops == &vdev_replacing_ops ||
	    pvd->vdev_ops == &vdev_draid_ops)) {
		return (B_TRUE);
	} else {
		return (B_FALSE);
	}
}

/*
 * Given a dRAID distribute spare vdev, returns the physical child vdev
 * on which the provided offset resides.  This may involve recursing through
 * multiple layers of distributed spares.
 * Note that offset is relative to this vdev.
 */
static vdev_t *
vdev_draid_spare_get_child(vdev_t *vd, uint64_t offset)
{
	vdev_draid_spare_t *vds = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_spare_ops);

	/* The vdev is closed or an invalid offset was provided. */
	if (vds == NULL || offset >= vd->vdev_psize -
	    (VDEV_LABEL_START_SIZE + VDEV_LABEL_END_SIZE)) {
		return (NULL);
	}

	vdev_t *tvd = vds->vds_draid_vdev;
	vdev_draid_config_t *vdc = tvd->vdev_tsd;

	ASSERT3P(tvd->vdev_ops, ==, &vdev_draid_ops);
	ASSERT3U(vds->vds_spare_id, <, vdc->vdc_spares);

	uint64_t *base, iter;
	uint64_t slicesz =
	    (vdc->vdc_data + vdc->vdc_parity) << DRAID_SLICESHIFT;
	uint64_t perm = offset / slicesz;
	vdev_draid_get_perm(vdc, perm, &base, &iter);
	uint64_t cid = vdev_draid_permute_id(vdc, base, iter,
	    (tvd->vdev_children - 1) - vds->vds_spare_id);
	vdev_t *cvd = tvd->vdev_child[cid];

	if (cvd->vdev_ops == &vdev_draid_spare_ops)
		return (vdev_draid_spare_get_child(cvd, offset));

	return (cvd);
}

#if 0
#ifndef _KERNEL
#include <execinfo.h>
#endif
#endif
/*
 * Close a dRAID spare device.
 */
static void
vdev_draid_spare_close(vdev_t *vd)
{
	vdev_draid_spare_t *vds = vd->vdev_tsd;

#if 0
#ifndef _KERNEL
printf("Entering close %s: %p %d\n", vd->vdev_path, vd->vdev_tsd, vd->vdev_reopening);
void *buffer[100];
int nptrs = backtrace(buffer, 100);
backtrace_symbols_fd(buffer, nptrs, 1);
#endif
#endif
	if (vd->vdev_reopening || vds == NULL)
		return;

	vd->vdev_tsd = NULL;
	kmem_free(vds, sizeof (vdev_draid_spare_t));
}

/*
 * Opening a dRAID spare device is done by extracting the top-level vdev id
 * and dRAID spare number from the provided vd->vdev_path identifier.  Any
 * additional information encoded in the identifier is solely used for
 * verification cross-checks and is not strictly required.
 */
static int
vdev_draid_spare_open(vdev_t *vd, uint64_t *psize, uint64_t *max_psize,
    uint64_t *ashift)
{
	uint64_t parity, groups, spares, vdev_id, spare_id;
	uint64_t asize, max_asize;
	vdev_draid_config_t *vdc;
	vdev_draid_spare_t *vds;
	vdev_t *tvd, *rvd = vd->vdev_spa->spa_root_vdev;
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

	if (vdev_id >= rvd->vdev_children)
		return (SET_ERROR(EINVAL));

	tvd = rvd->vdev_child[vdev_id];
	vdc = tvd->vdev_tsd;

	/* Spare name references a known top-level dRAID vdev */
	if (tvd->vdev_ops != &vdev_draid_ops || vdc == NULL)
		return (SET_ERROR(EINVAL));

	/* Spare name dRAID settings agree with top-level dRAID vdev */
	if (vdc->vdc_parity != parity ||
	    vdc->vdc_spares != spares || vdc->vdc_spares <= spare_id) {
		return (SET_ERROR(EINVAL));
	}

	vds = kmem_alloc(sizeof (vdev_draid_spare_t), KM_SLEEP);
	vds->vds_draid_vdev = tvd;
	vds->vds_spare_id = spare_id;
	vd->vdev_tsd = vds;

skip_open:
	/*
	 * Neither tvd->vdev_asize or tvd->vdev_max_asize can be used here
	 * because the caller may be vdev_draid_open() in which case the
	 * values are stale as they haven't yet been updated by vdev_open().
	 * To avoid this always recalculate the dRAID asize and max_asize.
	 */
	vdev_draid_calculate_asize(tvd, &asize, &max_asize, ashift);

	*psize = asize + VDEV_LABEL_START_SIZE + VDEV_LABEL_END_SIZE;
	*max_psize = max_asize + VDEV_LABEL_START_SIZE + VDEV_LABEL_END_SIZE;

	return (0);
}

/*
 * Completed distributed spare IO.  Store the result in the parent zio
 * as if it had performed the operation itself.  Only the first error is
 * preserved if there are multiple errors.
 */
static void
vdev_draid_spare_child_done(zio_t *zio)
{
	zio_t *pio = zio->io_private;

	if (pio->io_error == 0)
		pio->io_error = zio->io_error;
}

/*
 * Returns a valid label config for the distributed spare vdev.  This is
 * used to bypass the IO pipeline to avoid the complexity of constructing
 * a complete label with valid checksum to return when read.
 */
nvlist_t *
vdev_draid_read_config_spare(vdev_t *vd)
{
	spa_t *spa = vd->vdev_spa;
	spa_aux_vdev_t *sav = &spa->spa_spares;
	uint64_t guid = vd->vdev_guid;

	nvlist_t *nv = fnvlist_alloc();
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_IS_SPARE, 1);
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_CREATE_TXG, vd->vdev_crtxg);
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_VERSION, spa_version(spa));
	fnvlist_add_string(nv, ZPOOL_CONFIG_POOL_NAME, spa_name(spa));
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_POOL_GUID, spa_guid(spa));
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_POOL_TXG, spa->spa_config_txg);
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_TOP_GUID, vd->vdev_top->vdev_guid);
	fnvlist_add_uint64(nv, ZPOOL_CONFIG_POOL_STATE,
	    vdev_draid_spare_is_active(vd) ?
	    POOL_STATE_ACTIVE : POOL_STATE_SPARE);

	/* Set the vdev guid based on the vdev list in sav_count. */
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
 * Handle any ioctl requested of the distributed spare.  Only flushes
 * are supported in which case all children must be flushed.
 */
static int
vdev_draid_spare_ioctl(zio_t *zio)
{
	vdev_t *vd = zio->io_vd;
	int error = 0;

	if (zio->io_cmd == DKIOCFLUSHWRITECACHE) {
		for (int c = 0; c < vd->vdev_children; c++) {
			zio_nowait(zio_vdev_child_io(zio, NULL,
			    vd->vdev_child[c], zio->io_offset, zio->io_abd,
			    zio->io_size, zio->io_type, zio->io_priority, 0,
			    vdev_draid_spare_child_done, zio));
		}
	} else {
		error = SET_ERROR(ENOTSUP);
	}

	return (error);
}

/*
 * Initiate an IO to the distributed spare.  For normal IOs this entails using
 * the zio->io_offset and permutation table to calculate which child dRAID vdev
 * is responsible for the data.  Then passing along the zio to that child to
 * perform the actual IO.  The label ranges are not stored on disk and require
 * some special handling which is described below.
 */
static void
vdev_draid_spare_io_start(zio_t *zio)
{
	vdev_t *cvd = NULL, *vd = zio->io_vd;
	vdev_draid_spare_t *vds = vd->vdev_tsd;
	uint64_t offset = zio->io_offset - VDEV_LABEL_START_SIZE;

	/*
	 * If the vdev is closed, it's likely in the REMOVED or FAULTED state.
	 * Nothing to be done here but return failure.
	 */
	if (vds == NULL) {
		zio->io_error = ENXIO;
		zio_interrupt(zio);
		return;
	}

	/* Distributed spare IO never crosses a slice boundary. */
	IMPLY(zio->io_type != ZIO_TYPE_IOCTL, offset >> DRAID_SLICESHIFT ==
	    (offset + zio->io_size - 1) >> DRAID_SLICESHIFT);

	switch (zio->io_type) {
	case ZIO_TYPE_IOCTL:
		zio->io_error = vdev_draid_spare_ioctl(zio);
		break;

	case ZIO_TYPE_WRITE:
		if (VDEV_OFFSET_IS_LABEL(vd, zio->io_offset)) {
			/*
			 * Accept probe IOs and config writers to simulate the
			 * existence of an on disk label.  vdev_label_sync(),
			 * vdev_uberblock_sync() and vdev_copy_uberblocks()
			 * skip the distributed spares.  This only leaves
			 * vdev_label_init() which is allowed to succeed to
			 * avoid adding special cases the function.
			 */
			if (zio->io_flags & ZIO_FLAG_PROBE ||
			    zio->io_flags & ZIO_FLAG_CONFIG_WRITER) {
				zio->io_error = 0;
			} else {
				zio->io_error = SET_ERROR(EIO);
			}
		} else {
			cvd = vdev_draid_spare_get_child(vd, offset);

			if (cvd == NULL || !vdev_writeable(cvd)) {
				zio->io_error = SET_ERROR(ENXIO);
			} else {
				zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
				    offset, zio->io_abd, zio->io_size,
				    zio->io_type, zio->io_priority, 0,
				    vdev_draid_spare_child_done, zio));
			}
		}
		break;

	case ZIO_TYPE_READ:
		if (VDEV_OFFSET_IS_LABEL(vd, zio->io_offset)) {
			/*
			 * Accept probe IOs to simulate the existence of a
			 * label.  vdev_label_read_config() bypasses the
			 * pipeline to read the label configuration and
			 * vdev_uberblock_load() skips distributed spares
			 * when attempting to locate the best uberblock.
			 */
			if (zio->io_flags & ZIO_FLAG_PROBE) {
				zio->io_error = 0;
			} else {
				zio->io_error = SET_ERROR(EIO);
			}
		} else {
			cvd = vdev_draid_spare_get_child(vd, offset);

			if (cvd == NULL || !vdev_readable(cvd)) {
				zio->io_error = SET_ERROR(ENXIO);
			} else {
				zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
				    offset, zio->io_abd, zio->io_size,
				    zio->io_type, zio->io_priority, 0,
				    vdev_draid_spare_child_done, zio));
			}
		}
		break;

	case ZIO_TYPE_TRIM:
		/* The vdev label ranges are never trimmed */
		ASSERT0(VDEV_OFFSET_IS_LABEL(vd, zio->io_offset));

		cvd = vdev_draid_spare_get_child(vd, offset);

		if (cvd == NULL || !cvd->vdev_has_trim) {
			zio->io_error = SET_ERROR(ENXIO);
		} else {
			zio_nowait(zio_vdev_child_io(zio, NULL, cvd,
			    offset, zio->io_abd, zio->io_size,
			    zio->io_type, zio->io_priority, 0,
			    vdev_draid_spare_child_done, zio));
		}
		break;

	default:
		zio->io_error = SET_ERROR(ENOTSUP);
		break;
	}

	zio_execute(zio);
}

/* ARGSUSED */
static void
vdev_draid_spare_io_done(zio_t *zio)
{
}

vdev_ops_t vdev_draid_spare_ops = {
	.vdev_op_open = vdev_draid_spare_open,
	.vdev_op_close = vdev_draid_spare_close,
	.vdev_op_asize = vdev_default_asize,
	.vdev_op_io_start = vdev_draid_spare_io_start,
	.vdev_op_io_done = vdev_draid_spare_io_done,
	.vdev_op_state_change = NULL,
	.vdev_op_need_resilver = NULL,
	.vdev_op_hold = NULL,
	.vdev_op_rele = NULL,
	.vdev_op_remap = NULL,
	.vdev_op_xlate = vdev_default_xlate,
	.vdev_op_type = VDEV_TYPE_DRAID_SPARE,
	.vdev_op_leaf = B_TRUE,
};
