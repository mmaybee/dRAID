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
 * dRAID is a distributed parity implementation for ZFS. A dRAID vdev is
 * comprised of multiple raidz redundancy groups which are spread over the
 * dRAID children. To ensure an even distribution, and avoid hot spots, a
 * permutation mapping is applied to the order of the dRAID children.
 * This mixing effectively distributes the parity columns evenly over all
 * of the disks in the dRAID.
 *
 * This is beneficial because it means when resilvering all of the disks
 * can participate thereby increasing the available IOPs and bandwidth.
 * Furthermore, by reserving a small fraction of each child's total capacity
 * virtual distributed spare disks can be created. These spares similarly
 * benefit from the performance gains of spanning all of the children. The
 * consequence of which is that resilvering to a distributed spare can
 * substantially reduce the time required to restore full parity to pool
 * with a failed disks.
 *
 * === dRAID group layout ===
 *
 * First, let's define a "row" in the configuration to be a 16M chunk from
 * each physical drive at the same offset. This is the minimum allowable
 * size since it must be possible to store a full 16M block when there is
 * only a single data column. Next, we defined a "group" to be a set of
 * sequential disks containing both the parity and data columns. We allow
 * groups to span multiple rows in order to align any group size to any
 * number of physical drives. Finally, a "slice" is comprised of the rows
 * which contains the target number of groups. The permutation mappings
 * are applied in a round robin fashion to each slice.
 *
 * Given n drives in a group (including parity drives) and m physical drives
 * (not including the spare drives), we can distribute the groups across r
 * rows without remainder by selecting the least common multiple of n and m
 * as the number groups; i.e. ngroups = LCM(n, m).
 *
 * In the example below, there are 17 physical drives in the configuration,
 * with two drives worth of spare capacity. The group size is 8, there are
 * 15 groups and 8 rows per group. If the row chunk size is 16M, the slice
 * chunk size will be 128M.
 *
 *
 *                              data disks                         spares
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *    | 5 | 11| 7 | 13| 6 | 9 | 12| 1 | 4 | 2 | 0 | 10| 15| 8 | 3 | 14| 16|
 * s  |         group 0               |          group 1          |       |
 * l  |   |           group 2             |        group 3        |       |
 * i  |       |           group 4             |        group 5    |       |
 * c  |           |           group 6             |        group 7|       |
 * e  |               |           group 8             |           |       |
 * 1  |  group 9          |           group 10            |       |       |
 *    |      group 11         |           group 12            |   |       |
 *    |          group 13         |           group 14            |       |
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *    | 6 | 12| 8 | 14| 7 | 10| 13| 2 | 5 | 3 | 1 | 11| 16| 9 | 4 | 15| 0 |
 * s  |         group 15              |          group 16         |       |
 * l  |   |           group 17            |        group 18       |       |
 * i  |       |           group 19            |        group 20   |       |
 * c  |           |           group 21            |       group 22|       |
 * e  |               |           group 23            |           |       |
 * 2  |  group 24         |           group 25            |       |       |
 *    |      group 26         |           group 27            |   |       |
 *    |          group 28         |           group 29            |       |
 *    +---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
 *
 *    ...
 *
 * Notes:
 * 1. In this configuration the number of groups equals the number of data
 *    disks. This will occur but it should never be depended upon.
 *
 * 2. The top row of values (5, 11, 7, ...) reflects an example disk
 *    permutation mapping which has been applied to the slice.
 *
 * This layout has several advantages:
 *
 * 1. The group count is not a relevant parameter when defining a dRAID
 *    layout. Only the group size is needed, and *all* groups will have the
 *    desired size.
 *
 * 2. All possible group sizes (<= physical disk count) can be supported.
 *
 * 3. The logic within vdev_draid.c is simplified when the group size is
 *    the same for all groups (although some of the logic around computing
 *    permutation numbers and drive offsets is more complicated).
 */

static vdev_t *vdev_draid_spare_get_child(vdev_t *vd, uint64_t offset);

/*
 * Lookup the permutation base array and iteration id for the provided offset.
 */
static void
vdev_draid_get_perm(vdev_draid_config_t *vdc, uint64_t pindex,
    uint64_t **base, uint64_t *iter)
{
	uint64_t ncols = vdc->vdc_children;
	uint64_t poff = pindex % (vdc->vdc_nbases * ncols);

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
			/* empty data column (small write), add a skip sector */
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

/*
 * Given a logical address within a dRAID config, return the physical
 * address on the first drive in the group that this address maps to
 * (at position 'start' in permutation number 'perm').
 */
static uint64_t
vdev_draid_logical_to_physical(vdev_t *vd, uint64_t logical_offset,
    uint64_t *perm, uint64_t *start)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	/* b is the dRAID (parent) sector offset. */
	uint64_t ashift = vd->vdev_top->vdev_ashift;
	uint64_t b_offset = logical_offset >> ashift;

	/*
	 * The size of a ROW in units of the vdev's minimum sector size.
	 * ROW is the amount of data written to each disk of each group
	 * in a given permutation.
	 */
	uint64_t blocks_per_row = DRAID_ROWSIZE >> ashift;

	/*
	 * We cycle through a disk permutation every groupsz * ngroups chunk
	 * of address space. Note that ngroups * groupsz must be a multiple
	 * of the number of data drives (ndisks) in order to guarantee
	 * alignment. So, for example, if our row size is 16MB, our group
	 * size is 10, and there are 13 data drives in the draid, then ngroups
	 * will be 13, we will change permutation every 2.08GB and each
	 * disk will have 160MB of data per chunk.
	 */
	uint64_t groupwidth = vdc->vdc_groupwidth;
	uint64_t ngroups = vdc->vdc_ngroups;
	uint64_t ndisks = vdc->vdc_ndisks;

	/*
	 * groupstart is where the group this IO will land in "starts" in
	 * the permutation array.
	 */
	uint64_t group = logical_offset / vdc->vdc_groupsz;
	uint64_t groupstart = (group * groupwidth) % ndisks;
	ASSERT3U(groupstart + groupwidth, <=, ndisks + groupstart);
	*start = groupstart;

	/* b_offset is the sector offset within a group chunk */
	b_offset = b_offset % (blocks_per_row * groupwidth);
	ASSERT0(b_offset % groupwidth);

	/*
	 * Find the starting byte offset on each child vdev:
	 * - within a permutation there are ngroups groups spread over the
	 *   rows, where each row covers a slice portion of the disk
	 * - each permutation has (groupwidth * ngroups) / ndisks rows
	 * - so each permutation covers rows * slice portion of the disk
	 * - so we need to find the row where this IO group target begins
	 */
	*perm = group / ngroups;
	uint64_t row = (*perm * ((groupwidth * ngroups) / ndisks)) +
	    (((group % ngroups) * groupwidth) / ndisks);

	return (((blocks_per_row * row) + (b_offset / groupwidth)) << ashift);
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

	/* Lookup starting byte offset on each child vdev */
	uint64_t groupstart, perm;
	uint64_t physical_offset = vdev_draid_logical_to_physical(vd,
	    zio->io_offset, &perm, &groupstart);

	/*
	 * If there is less than groupwidth drives available after the group
	 * start, the group is going to wrap onto the next row. 'wrap' is the
	 * group disk number that starts on the next row.
	 */
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t ndisks = vdc->vdc_ndisks;
	uint64_t groupwidth = vdc->vdc_groupwidth;
	uint64_t wrap = groupwidth;

	if (groupstart + groupwidth > ndisks)
		wrap = ndisks - groupstart;

	/* The zio's size in units of the vdev's minimum sector size. */
	const uint64_t ashift = vd->vdev_top->vdev_ashift;
	const uint64_t psize = zio->io_size >> ashift;

	/*
	 * "Quotient": The number of data sectors for this stripe on all but
	 * the "big column" child vdevs that also contain "remainder" data.
	 */
	uint64_t q = psize / vdc->vdc_ndata;

	/*
	 * "Remainder": The number of partial stripe data sectors in this I/O.
	 * This will add a sector to some, but not all, child vdevs.
	 */
	uint64_t r = psize - q * vdc->vdc_ndata;

	/* The number of "big columns" - those which contain remainder data. */
	uint64_t bc = (r == 0 ? 0 : r + vdc->vdc_nparity);
	ASSERT3U(bc, <, groupwidth);

	/* The total number of data and parity sectors for this I/O. */
	uint64_t tot = psize + (vdc->vdc_nparity * (q + (r == 0 ? 0 : 1)));

	raidz_map_t *rm = kmem_alloc(offsetof(raidz_map_t, rm_col[groupwidth]),
	    KM_SLEEP);

	rm->rm_cols = (q == 0) ? bc : groupwidth;
	rm->rm_scols = groupwidth;
	rm->rm_bigcols = bc;
	rm->rm_skipstart = bc;
	rm->rm_missingdata = 0;
	rm->rm_missingparity = 0;
	rm->rm_firstdatacol = vdc->vdc_nparity;
	rm->rm_abd_copy = NULL;
	rm->rm_abd_skip = NULL;
	rm->rm_reports = 0;
	rm->rm_freed = 0;
	rm->rm_ecksuminjected = 0;
	rm->rm_include_skip = 1;

	uint64_t *base, iter, asize = 0;
	vdev_draid_get_perm(vdc, perm, &base, &iter);
	for (uint64_t i = 0; i < groupwidth; i++) {
		uint64_t c = (groupstart + i) % ndisks;

		/* increment the offset if we wrap to the next row */
		if (i == wrap)
			physical_offset += DRAID_ROWSIZE;

		rm->rm_col[i].rc_devidx =
		    vdev_draid_permute_id(vdc, base, iter, c);
		rm->rm_col[i].rc_offset = physical_offset;
		rm->rm_col[i].rc_abd = NULL;
		rm->rm_col[i].rc_gdata = NULL;
		rm->rm_col[i].rc_error = 0;
		rm->rm_col[i].rc_tried = 0;
		rm->rm_col[i].rc_skipped = 0;
		rm->rm_col[i].rc_repair = 0;

		if (i >= rm->rm_cols)
			rm->rm_col[i].rc_size = 0;
		else if (i < bc)
			rm->rm_col[i].rc_size = (q + 1) << ashift;
		else
			rm->rm_col[i].rc_size = q << ashift;

		asize += rm->rm_col[i].rc_size;
	}

	ASSERT3U(asize, ==, tot << ashift);
	rm->rm_asize = roundup(asize, groupwidth << ashift);
	rm->rm_nskip = roundup(tot, groupwidth) - tot;
	IMPLY(bc > 0, rm->rm_nskip == groupwidth - bc);
	ASSERT3U(rm->rm_asize - asize, ==, rm->rm_nskip << ashift);
	ASSERT3U(rm->rm_nskip, <, vdc->vdc_ndata);

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
 * Converts a logical offset to the corresponding group number.
 */
uint64_t
vdev_draid_offset_to_group(vdev_t *vd, uint64_t offset)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	return (offset / vdc->vdc_groupsz);
}

/*
 * Converts a group number to the logical starting offset for that group.
 */
uint64_t
vdev_draid_group_to_offset(vdev_t *vd, uint64_t group)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	return (group * vdc->vdc_groupsz);
}

/*
 * Given a offset into a draid, compute a group aligned offset.
 */
uint64_t
vdev_draid_get_astart(vdev_t *vd, const uint64_t start)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	return (roundup(start, vdc->vdc_groupwidth << vd->vdev_ashift));
}

/*
 * Return the asize which is the psize rounded up to a full group width.
 * i.e. vdev_draid_psize_to_asize().
 */
static uint64_t
vdev_draid_asize(vdev_t *vd, uint64_t psize)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;
	uint64_t ashift = vd->vdev_ashift;
	uint64_t asize = ((psize - 1) >> ashift) + 1;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	asize = roundup(asize, vdc->vdc_ndata);
	asize += vdc->vdc_nparity * (asize / vdc->vdc_ndata);

	ASSERT3U(asize, !=, 0);
	ASSERT3U(asize, <, vdc->vdc_groupsz);
	ASSERT3U(asize % (vdc->vdc_groupwidth), ==, 0);

	return (asize << ashift);
}

/*
 * Deflate the asize to the psize for a block at the provided offset.
 * This involves stripping parity.
 */
uint64_t
vdev_draid_asize_to_psize(vdev_t *vd, uint64_t asize, uint64_t offset)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	return ((asize / vdc->vdc_groupwidth) * vdc->vdc_ndata);
}

/*
 * Check if there's enough space left for the block in the group.  The passed
 * starting offset must be properly align.  If the block fits return an
 * updated 'asizep' and the starting offset to be used, this may be in the
 * next group if there wasn't space in the original group.
 */
uint64_t
vdev_draid_check_block(vdev_t *vd, uint64_t start, uint64_t *asizep)
{
	uint64_t group = vdev_draid_offset_to_group(vd, start);
	uint64_t asize = vdev_draid_asize(vd, *asizep);
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
	asize = vdev_draid_asize(vd, *asizep);
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
vdev_draid_missing(vdev_t *vd, uint64_t physical_offset, uint64_t txg,
    uint64_t size)
{
	if (vdev_dtl_contains(vd, DTL_MISSING, txg, size))
		return (B_TRUE);

	if (vd->vdev_ops == &vdev_draid_spare_ops) {
		vd = vdev_draid_spare_get_child(vd, physical_offset);
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

		if (!vdev_draid_missing(cvd, physical_offset, txg, size))
			return (B_FALSE);
	}

	return (B_TRUE);
}

/*
 * Determine if the vdev is readable at the given offset.
 */
boolean_t
vdev_draid_readable(vdev_t *vd, uint64_t physical_offset)
{
	if (vd->vdev_ops == &vdev_draid_spare_ops) {
		vd = vdev_draid_spare_get_child(vd, physical_offset);
		if (vd == NULL)
			return (B_FALSE);
	}

	return (vdev_readable(vd));
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

/*
 * Returns B_TRUE if the passed in vdev is currently "faulted".
 * Faulted, in this context, means that the vdev represents a
 * replacing or sparing vdev tree.
 */
static boolean_t
vdev_draid_faulted(vdev_t *vd, uint64_t physical_offset)
{
	if (vd->vdev_ops == &vdev_draid_spare_ops) {
		vd = vdev_draid_spare_get_child(vd, physical_offset);
		if (vd == NULL)
			return (B_FALSE);

		/*
		 * After resolving the distributed spare to a leaf vdev
		 * check the parent to determine if it's "faulted".
		 */
		vd = vd->vdev_parent;
	}

	return (vd->vdev_ops == &vdev_replacing_ops ||
	    vd->vdev_ops == &vdev_spare_ops);
}

static boolean_t
vdev_draid_vd_degraded(vdev_t *vd, uint64_t physical_offset,
    uint64_t phys_birth)
{
	if (!vdev_draid_faulted(vd, physical_offset))
		return (vdev_dtl_contains(vd, DTL_PARTIAL, phys_birth, 1));

	return (B_TRUE);
}

/*
 * Determine if the dRAID block at the logical offset is degraded.
 */
boolean_t
vdev_draid_group_degraded(vdev_t *vd, uint64_t offset, uint64_t psize,
    uint64_t phys_birth)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);

	uint64_t groupstart, perm;
	uint64_t physical_offset = vdev_draid_logical_to_physical(vd,
	    offset, &perm, &groupstart);

	uint64_t *base, iter;
	vdev_draid_get_perm(vdc, perm, &base, &iter);

	for (uint64_t i = 0; i < vdc->vdc_groupwidth; i++) {
		uint64_t c = (groupstart + i) % vdc->vdc_ndisks;
		uint64_t cid = vdev_draid_permute_id(vdc, base, iter, c);
		vdev_t *cvd = vd->vdev_child[cid];

		if (vdev_draid_vd_degraded(cvd, physical_offset, phys_birth))
			return (B_TRUE);

		if (vdev_dtl_contains(cvd, DTL_PARTIAL, phys_birth, 1))
			return (B_TRUE);
	}

	return (B_FALSE);
}

/*
 * Allocate memory for and copy the dRAID base permutations.
 */
static uint64_t *
vdev_draid_create_base_permutations(const uint8_t *perms,
    vdev_draid_config_t *vdc)
{
	uint64_t children = vdc->vdc_children, *base_perms;
	size_t sz = sizeof (uint64_t) * vdc->vdc_nbases * children;

	base_perms = kmem_alloc(sz, KM_SLEEP);
	for (int i = 0; i < vdc->vdc_nbases; i++)
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

	/*
	 * Values read from the ZPOOL_CONFIG_DRAIDCFG.
	 */
	vdc->vdc_guid = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_GUID);
	vdc->vdc_seed = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_SEED);
	vdc->vdc_ndata = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_DATA);
	vdc->vdc_nparity = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_PARITY);
	vdc->vdc_nspares = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_SPARES);
	vdc->vdc_children = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_CHILDREN);
	vdc->vdc_ngroups = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_GROUPS);
	vdc->vdc_nbases = fnvlist_lookup_uint64(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_BASES);
	VERIFY0(nvlist_lookup_uint8_array(nvl,
	    ZPOOL_CONFIG_DRAIDCFG_PERM, &perms, &c));
	vdc->vdc_base_perms = vdev_draid_create_base_permutations(perms, vdc);

	/*
	 * Derived constants.
	 */
	vdc->vdc_groupwidth = vdc->vdc_ndata + vdc->vdc_nparity;
	vdc->vdc_ndisks = vdc->vdc_children - vdc->vdc_nspares;
	vdc->vdc_groupsz = vdc->vdc_groupwidth * DRAID_ROWSIZE;
	vdc->vdc_devslicesz = (vdc->vdc_groupsz * vdc->vdc_ngroups) /
	    vdc->vdc_ndisks;

	ASSERT3U(vdc->vdc_groupwidth, >=, 2);
	ASSERT3U(vdc->vdc_groupwidth, <=, vdc->vdc_ndisks);
	ASSERT3U(vdc->vdc_groupsz, >=, 2 * DRAID_ROWSIZE);
	ASSERT3U(vdc->vdc_devslicesz, >=, DRAID_ROWSIZE);
	ASSERT3U(vdc->vdc_devslicesz % DRAID_ROWSIZE, ==, 0);
	ASSERT3U((vdc->vdc_groupwidth * vdc->vdc_ngroups) %
	    vdc->vdc_ndisks, ==, 0);

	return (vdc);
}

/*
 * Destroy the vdev_draid_config_t structure.
 */
static void
vdev_draid_config_destroy(vdev_draid_config_t *vdc)
{
	size_t sz = sizeof (uint64_t) * vdc->vdc_nbases * vdc->vdc_children;

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
		vd->vdev_nspares = vdc->vdc_nspares;
		vd->vdev_ngroups = vdc->vdc_ngroups;
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

	*asize = *asize * (vd->vdev_children - vdc->vdc_nspares);
	*max_asize = *max_asize * (vd->vdev_children - vdc->vdc_nspares);

	return (0);
}

/*
 * Return the asize of the largest block which can be reconstructed.
 */
uint64_t
vdev_draid_max_rebuildable_asize(vdev_t *vd, uint64_t max_segment)
{
	vdev_draid_config_t *vdc = vd->vdev_tsd;

	uint64_t psize = MIN(P2ROUNDUP(max_segment * vdc->vdc_ndata,
	    1 << vd->vdev_ashift), SPA_MAXBLOCKSIZE);

	/*
	 * When the maxpsize >> ashift does not divide evenly by the number
	 * of data drives, the remainder must be discarded. Otherwise the skip
	 * sectors will cause vdev_draid_asize_to_psize() to get a psize larger
	 * than the maximum allowed block size.
	 */
	psize >>= vd->vdev_ashift;
	psize /= vdc->vdc_ndata;
	psize *= vdc->vdc_ndata;
	psize <<= vd->vdev_ashift;

	return (vdev_draid_asize(vd, psize));
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
	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_ops);
	uint64_t offset = DVA_GET_OFFSET(dva);

	if (vdev_dtl_contains(vd, DTL_PARTIAL, phys_birth, 1))
		return (!vdev_draid_group_degraded(vd, offset, psize,
		    phys_birth));

	return (B_FALSE);
}

static void
vdev_draid_io_verify(zio_t *zio, raidz_map_t *rm, int col)
{
#ifdef ZFS_DEBUG
	vdev_t *vd = zio->io_vd;

	range_seg64_t logical_rs, physical_rs, remain_rs;
	logical_rs.rs_start = zio->io_offset;
	logical_rs.rs_end = logical_rs.rs_start +
	    vdev_draid_asize(zio->io_vd, zio->io_size);

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

	/* Make sure the offsets are block-aligned */
	ASSERT0(in->rs_start % (1 << ashift));
	ASSERT0(in->rs_end % (1 << ashift));

	/*
	 * Translation requests can never span three or more slices.  Doing so
	 * could result in distributed spare space being incorrectly included
	 * in the physical range.  Therefore, vdev_xlate() limits the input
	 * size to a single group.  This is stricter than absolutely necessary
	 * but helps simplify the logic below.
	 */
	ASSERT3U(vdev_draid_offset_to_group(raidvd, in->rs_start), ==,
	    vdev_draid_offset_to_group(raidvd, in->rs_end - 1));

	/* Find the starting offset for each vdev in the group */
	uint64_t perm, groupstart;
	uint64_t start = vdev_draid_logical_to_physical(raidvd, in->rs_start,
	    &perm, &groupstart);
	uint64_t end = start;

	uint64_t *base, iter, id;
	vdev_draid_get_perm(vdc, perm, &base, &iter);

	/*
	 * Check if the passed child falls within the group.  If it does
	 * update the start and end to reflect the physical range.
	 * Otherwise, leave them unmodified which will result in an empty
	 * (zero-length) physical range being returned.
	 */
	for (uint64_t i = 0; i < vdc->vdc_groupwidth; i++) {
		uint64_t c = (groupstart + i) % vdc->vdc_ndisks;

		if (c == 0 && i != 0) {
			/* the group wrapped, increment the start */
			start += DRAID_ROWSIZE;
			end = start;
		}

		id = vdev_draid_permute_id(vdc, base, iter, c);
		if (id == cvd->vdev_id) {
			uint64_t b_size = (in->rs_end >> ashift) -
			    (in->rs_start >> ashift);
			ASSERT3U(b_size, >, 0);
			end = start + ((((b_size - 1) /
			    vdc->vdc_groupwidth) + 1) << ashift);
			break;
		}
	}
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
	uint64_t vds_spare_id;		/* spare id (0 - vdc->vdc_nspares-1) */
} vdev_draid_spare_t;

/*
 * Returns the parent dRAID vdev to which the distributed spare belongs.
 * This may be safely called even when the vdev is not open.
 */
vdev_t *
vdev_draid_spare_get_parent(vdev_t *vd)
{
	uint64_t spare_id, nparity, vdev_id;
	vdev_t *rvd = vd->vdev_spa->spa_root_vdev;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_spare_ops);
	if (vdev_draid_spare_values(vd->vdev_path, &spare_id, &nparity,
	    &vdev_id) != 0) {
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
 * multiple layers of distributed spares.  Note that offset is relative to
 * this vdev.
 */
static vdev_t *
vdev_draid_spare_get_child(vdev_t *vd, uint64_t physical_offset)
{
	vdev_draid_spare_t *vds = vd->vdev_tsd;

	ASSERT3P(vd->vdev_ops, ==, &vdev_draid_spare_ops);

	/* The vdev is closed or an invalid offset was provided. */
	if (vds == NULL || physical_offset >= vd->vdev_psize -
	    (VDEV_LABEL_START_SIZE + VDEV_LABEL_END_SIZE)) {
		return (NULL);
	}

	vdev_t *tvd = vds->vds_draid_vdev;
	vdev_draid_config_t *vdc = tvd->vdev_tsd;

	ASSERT3P(tvd->vdev_ops, ==, &vdev_draid_ops);
	ASSERT3U(vds->vds_spare_id, <, vdc->vdc_nspares);

	uint64_t *base, iter;
	uint64_t perm = physical_offset / vdc->vdc_devslicesz;

	vdev_draid_get_perm(vdc, perm, &base, &iter);

	uint64_t cid = vdev_draid_permute_id(vdc, base, iter,
	    (tvd->vdev_children - 1) - vds->vds_spare_id);
	vdev_t *cvd = tvd->vdev_child[cid];

	if (cvd->vdev_ops == &vdev_draid_spare_ops)
		return (vdev_draid_spare_get_child(cvd, physical_offset));

	return (cvd);
}

/*
 * Close a dRAID spare device.
 */
static void
vdev_draid_spare_close(vdev_t *vd)
{
	vdev_draid_spare_t *vds = vd->vdev_tsd;

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
	uint64_t spare_id, nparity, vdev_id;
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
	error = vdev_draid_spare_values(vd->vdev_path, &spare_id, &nparity,
	    &vdev_id);
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
	if (vdc->vdc_nparity != nparity || vdc->vdc_nspares <= spare_id)
		return (SET_ERROR(EINVAL));

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
	IMPLY(zio->io_type != ZIO_TYPE_IOCTL, offset >> DRAID_ROWSHIFT ==
	    (offset + zio->io_size - 1) >> DRAID_ROWSHIFT);

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
