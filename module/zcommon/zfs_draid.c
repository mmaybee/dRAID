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
#include <sys/vdev_impl.h>
#include <sys/vdev_draid_impl.h>
#include <sys/nvpair.h>
#include <zfs_draid.h>

/*
 * Verify that the provided dRAID configuration describes a valid
 * configuration.  Returns DRAIDCFG_OK for a correct configuration.
 * Otherwise, the returned draidcfg_err_t enum will describe the
 * problem encountered with the configuration.
 *
 * The required_* arguments are optional and can be used to crosscheck the
 * configuration when these values are known.
 */
draidcfg_err_t
vdev_draid_config_validate(nvlist_t *config,
    uint64_t required_data, uint64_t required_parity,
    uint64_t required_spares, uint64_t required_children)
{
	uint_t c;
	uint8_t *perm = NULL;
	uint64_t n, g, d, p, s, b;

	/* Validate configuration data exists and is within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_DATA, &d))
		return (DRAIDCFG_ERR_DATA_MISSING);

	if (d == 0 || d > VDEV_DRAID_MAX_GROUPSIZE)
		return (DRAIDCFG_ERR_DATA_INVALID);

	if (required_data != 0 && d != required_data)
		return (DRAIDCFG_ERR_DATA_MISMATCH);

	/* Validate configuration parity exists and is within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_PARITY, &p))
		return (DRAIDCFG_ERR_PARITY_MISSING);

	if (p == 0 || p > VDEV_DRAID_MAX_PARITY)
		return (DRAIDCFG_ERR_PARITY_INVALID);

	if (required_parity != 0 && p != required_parity)
		return (DRAIDCFG_ERR_PARITY_MISMATCH);

	/* Validate configuration children exists and are within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_CHILDREN, &n))
		return (DRAIDCFG_ERR_CHILDREN_MISSING);

	if (n == 0 || (n - 1) > VDEV_DRAID_MAX_CHILDREN)
		return (DRAIDCFG_ERR_CHILDREN_INVALID);

	if (required_children != 0 && n != required_children)
		return (DRAIDCFG_ERR_CHILDREN_MISMATCH);

	/* Validate configuration spares exists and are within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_SPARES, &s))
		return (DRAIDCFG_ERR_SPARES_MISSING);

	if (s == 0 || s > VDEV_DRAID_MAX_SPARES || s > (n - (d + p)))
		return (DRAIDCFG_ERR_SPARES_INVALID);

	if (required_spares != 0 && s != required_spares)
		return (DRAIDCFG_ERR_SPARES_MISMATCH);

	/* Validate configuration groups exists and are within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_GROUPS, &g))
		return (DRAIDCFG_ERR_GROUPS_MISSING);

	if (g == 0 || g > VDEV_DRAID_MAX_GROUPS)
		return (DRAIDCFG_ERR_GROUPS_INVALID);

	if (required_data != 0 && required_parity != 0 &&
	    required_spares != 0 && required_children != 0 &&
	    (g * (required_data + required_parity)) %
	    (required_children - required_spares) != 0) {
		return (DRAIDCFG_ERR_GROUPS_MISMATCH);
	}

	/* Validate configuration base exists and is within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_NPERMS, &b))
		return (DRAIDCFG_ERR_NPERMS_MISSING);

	if (b == 0)
		return (DRAIDCFG_ERR_NPERMS_INVALID);

	/*
	 * Validate the minimum number of children exist per group for the
	 * specified parity level (draid1 >= 3, draid2 >= 4, draid3 >= 5).
	 */
	if (n < (d + p + s))
		return (DRAIDCFG_ERR_CHILDREN_INVALID);

	/*
	 * Validate that the number of groups fits evenly in to a slice.
	 */
	if (g * (d + p) % (n - s) != 0)
		return (DRAIDCFG_ERR_LAYOUT);

	if (nvlist_lookup_uint8_array(config,
	    ZPOOL_CONFIG_DRAIDCFG_PERMS, &perm, &c)) {
		return (DRAIDCFG_ERR_PERMS_MISSING);
	}

	/*
	 * Validate the permutation array size matches the expected size,
	 * that its elements are within the allowed range, and that there
	 * are no duplicates.
	 */
	if (c != b * n)
		return (DRAIDCFG_ERR_PERMS_MISMATCH);

	for (uint64_t i = 0; i < b; i++) {
		for (uint64_t j = 0; j < n; j++) {
			uint64_t val = perm[i * n + j];

			if (val >= n)
				return (DRAIDCFG_ERR_PERMS_INVALID);

			for (uint64_t k = 0; k < j; k++) {
				if (val == perm[i * n + k])
					return (DRAIDCFG_ERR_PERMS_DUPLICATE);
			}
		}
	}

	return (DRAIDCFG_OK);
}

/*
 * Output a dRAID top-level vdev name in to the provided buffer.  The
 * children argument is optional, if provided it will always be appended
 * to the end of the string.
 */
char *
vdev_draid_name(char *name, int len, uint64_t data, uint64_t parity,
    uint64_t spares, uint64_t children)
{
	bzero(name, len);
	snprintf(name, len - 1, "%s%llu:%llud:%lluc:%llus",
	    VDEV_TYPE_DRAID, (u_longlong_t)parity, (u_longlong_t)data,
	    (u_longlong_t)children, (u_longlong_t)spares);

	return (name);
}

/*
 * Output a dRAID top-level vdev name in to the provided buffer given
 * the ZPOOL_CONFIG_DRAIDCFG nvlist_t.
 */
char *
vdev_draid_name_nv(char *name, int len, nvlist_t *nv)
{
	uint64_t data, parity, spares, children;

	VERIFY0(nvlist_lookup_uint64(nv, ZPOOL_CONFIG_DRAIDCFG_DATA,
	    &data));
	VERIFY0(nvlist_lookup_uint64(nv, ZPOOL_CONFIG_DRAIDCFG_PARITY,
	    &parity));
	VERIFY0(nvlist_lookup_uint64(nv, ZPOOL_CONFIG_DRAIDCFG_SPARES,
	    &spares));
	VERIFY0(nvlist_lookup_uint64(nv, ZPOOL_CONFIG_DRAIDCFG_CHILDREN,
	    &children));

	return (vdev_draid_name(name, len, data, parity, spares, children));
}

/*
 * Output a dRAID spare vdev name in to the provided buffer.
 */
char *
vdev_draid_spare_name(char *name, int len, uint64_t spare_id,
    uint64_t parity, uint64_t vdev_id)
{
	bzero(name, len);
	(void) snprintf(name, len - 1, "s%llu-%s%llu-%llu",
	    (u_longlong_t)spare_id, VDEV_TYPE_DRAID, (u_longlong_t)parity,
	    (u_longlong_t)vdev_id);

	return (name);
}

/*
 * Parse dRAID configuration information out of the passed dRAID spare name.
 */
int
vdev_draid_spare_values(const char *name, uint64_t *spare_id,
    uint64_t *parity, uint64_t *vdev_id)
{
	if (sscanf(name, "s%llu-" VDEV_TYPE_DRAID "%llu-%llu",
	    (u_longlong_t *)spare_id, (u_longlong_t *)parity,
	    (u_longlong_t *)vdev_id) != 3) {
		return (EINVAL);
	}

	return (0);
}

/*
 * Return B_TRUE is the provided name is a dRAID spare name.
 */
boolean_t
vdev_draid_is_spare(const char *name)
{
	uint64_t spare_id, parity, vdev_id;

	return (!vdev_draid_spare_values(name, &spare_id, &parity, &vdev_id));
}

/*
 * The following functionality is only required in user space and is
 * primarily responsible for generating a valid dRAID configuration.
 * Additional helper functions are provided to facilitate dumping,
 * saving, and restoring a dRAID configuration.
 */
#if !defined(_KERNEL)

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <libintl.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <sys/stat.h>


typedef struct draid_map {
	uint64_t dm_nperms;	/* # of permutation rows */
	uint64_t dm_children;	/* # of permuation columns */
	uint64_t *dm_perms;	/* base permutation array */
} draid_map_t;

typedef struct draid_pair {
	int value;
	int order;
} draid_pair_t;

/*
 * Permute the values provided into a different order and store them in 'out'.
 */
static void
permute_devs(uint64_t *in, uint64_t *out, int ndevs)
{
	/* Swap two devices */
	if (ndevs == 2) {
		int i = in[0];
		int j = in[1];
		out[0] = j;
		out[1] = i;
		return;
	}
	int size = ndevs * sizeof (draid_pair_t);
	draid_pair_t *tmp = kmem_alloc(size, KM_SLEEP);

	/* Assign random order */
	for (int i = 0; i < ndevs; i++) {
		tmp[i].value = in[i];
		tmp[i].order = mrand48();
	}

	/* Sort */
	for (int i = 1; i < ndevs; i++) {
		for (int j = 0; j < i; j++) {
			if (tmp[i].order < tmp[j].order) {
				draid_pair_t t = tmp[i];
				tmp[i] = tmp[j];
				tmp[j] = t;
			}
		}
	}

	for (int i = 0; i < ndevs; i++)
		out[i] = tmp[i].value;

	kmem_free(tmp, size);
}

/*
 * Verify the map is valid.
 */
static int
check_map(draid_map_t *map)
{
	uint64_t dev, devcounts[VDEV_DRAID_MAX_CHILDREN] = { 0 };

	/* Ensure each device index appears exactly once in every row */
	for (int i = 0; i < map->dm_nperms; i++) {
		for (int j = 0; j < map->dm_children; j++) {
			dev = map->dm_perms[(i * map->dm_children) + j];
			ASSERT3U(0, <=, dev);
			ASSERT3U(dev, <, map->dm_children);
			ASSERT3U(devcounts[dev], ==, i);
			devcounts[dev]++;
		}
	}

	return (0);
}

/*
 * Allocate an empty permutation map.
 */
static draid_map_t *
alloc_map(uint64_t children, int nperms)
{
	draid_map_t *map = kmem_alloc(sizeof (draid_map_t), KM_SLEEP);
	map->dm_children = children;
	map->dm_nperms = nperms;
	map->dm_perms = kmem_alloc(sizeof (uint64_t) * children * nperms,
	    KM_SLEEP);

	return (map);
}

/*
 * Free a permutation map.
 */
static void
free_map(draid_map_t *map)
{
	kmem_free(map->dm_perms, sizeof (uint64_t) *
	    map->dm_children * map->dm_nperms);
	kmem_free(map, sizeof (draid_map_t));
}

/*
 * Duplicate a permutation map.
 */
static draid_map_t *
dup_map(draid_map_t *oldmap)
{
	draid_map_t *newmap;

	newmap = alloc_map(oldmap->dm_children, oldmap->dm_nperms);
	newmap->dm_children = oldmap->dm_children;
	newmap->dm_nperms = oldmap->dm_nperms;
	memcpy(newmap->dm_perms, oldmap->dm_perms,
	    sizeof (uint64_t) * oldmap->dm_children * oldmap->dm_nperms);

	ASSERT0(check_map(newmap));

	return (newmap);
}

/*
 * Check if dev is in the provided list of faulted devices.
 */
static inline boolean_t
is_faulted(int *faulted_devs, int nfaulted, int dev)
{
	for (int i = 0; i < nfaulted; i++)
		if (faulted_devs[i] == dev)
			return (B_TRUE);

	return (B_FALSE);
}

/*
 * Evaluate how resilvering I/O will be distributed given a list of faulted
 * vdevs.  As a simplification we assume one IO is sufficient to repair each
 * damaged device in a group.  The total numbers of read and write IOs to
 * repair all degraded groups for all rows is stored in total_required_ios.
 * The max_child_ios argument is the largest number of IO which must be
 * preformed by a child.  For an ideally balanced layout this means that;
 * max_child_ios * children ~= total_required_ios.
 */
static void
eval_resilver(vdev_draid_config_t *vdc, draid_map_t *map, int *faulted_devs,
    int nfaulted, int *max_child_ios, int *total_required_ios)
{
	uint64_t groupwidth = vdc->vdc_groupwidth;
	uint64_t ngroups = vdc->vdc_ngroups;
	uint64_t nspares = vdc->vdc_nspares;
	uint64_t ndisks = vdc->vdc_ndisks;

	int *ios = kmem_zalloc(sizeof (uint64_t) * map->dm_children, KM_SLEEP);

	/* resilver all rows */
	for (int i = 0; i < map->dm_nperms; i++) {
		uint64_t *row = &map->dm_perms[i * map->dm_children];

		/* resilver all groups with faulted drives */
		for (int j = 0; j < ngroups; j++) {
			uint64_t spareidx = map->dm_children - nspares;
			boolean_t repair_needed = B_FALSE;

			/* See if any devices in this group are faulted */
			int groupstart = (j * groupwidth) % ndisks;

			for (int k = 0; k < groupwidth; k++) {
				int groupidx = (groupstart + k) % ndisks;

				repair_needed = is_faulted(faulted_devs,
				    nfaulted, row[groupidx]);
				if (repair_needed)
					break;
			}

			if (repair_needed == B_FALSE)
				continue;

			/*
			 * This group is degraded. Calculate the number of
			 * reads the non-faulted drives require and the number
			 * of writes to the distributed hot spare for this row.
			 */
			for (int k = 0; k < groupwidth; k++) {
				int groupidx = (groupstart + k) % ndisks;

				if (!is_faulted(faulted_devs, nfaulted,
				    row[groupidx])) {
					ios[row[groupidx]]++;
				} else {
					while (is_faulted(faulted_devs,
					    nfaulted, row[spareidx])) {
						spareidx++;
					}

					ASSERT3U(spareidx, <, map->dm_children);
					ios[row[spareidx]]++;
					spareidx++;
				}
			}
		}
	}

	*max_child_ios = 0;
	*total_required_ios = 0;

	/* Find the drives with most I/O required */
	for (int i = 0; i < map->dm_children; i++) {
		if (ios[i] > *max_child_ios)
			*max_child_ios = ios[i];

		*total_required_ios += ios[i];
	}

	kmem_free(ios, sizeof (uint64_t) * map->dm_children);
}

/*
 * Evaluate the permutation mapping for all possible device failures.
 */
static double
eval_decluster(vdev_draid_config_t *vdc, draid_map_t *map, draidcfg_eval_t eval,
    int faults)
{
	int child_ios, required_ios;
	int n = 0, max_child_ios = 0, total_required_ios = 0;
	long sum = 0;
	long sumsq = 0;
	int faulted_devs[2];

	/* Only consider up to two simultaneous faults. */
	VERIFY3S(faults, >, 0);
	VERIFY3S(faults, <=, 2);

	for (int f1 = 0; f1 < map->dm_children; f1++) {
		faulted_devs[0] = f1;

		if (faults == 1) {
			/* Evaluate single failure */
			eval_resilver(vdc, map, faulted_devs, faults,
			    &child_ios, &required_ios);
			if (max_child_ios < child_ios)
				max_child_ios = child_ios;

			sum += child_ios;
			sumsq += child_ios * child_ios;
			total_required_ios += required_ios;
			n++;

			continue;
		} else if (faults == 2) {
			/* Evaluate double failure */
			for (int f2 = f1 + 1; f2 < map->dm_children; f2++) {
				faulted_devs[1] = f2;

				eval_resilver(vdc, map, faulted_devs, faults,
				    &child_ios, &total_required_ios);
				if (max_child_ios < child_ios)
					max_child_ios = child_ios;

				sum += child_ios;
				sumsq += child_ios * child_ios;
				total_required_ios += required_ios;
				n++;
			}
		}
	}

	double value = 0;

	switch (eval) {
	case DRAIDCFG_EVAL_WORST:
		/*
		 * Imbalance from worst possible drive failure
		 * insensitive to failures handled better.
		 */
		value = max_child_ios;
		break;
	case DRAIDCFG_EVAL_MEAN:
		/*
		 * Average over all possible drive failures
		 * sensitive to all possible failures.
		 */
		value = ((double)sum) / n;
		break;
	case DRAIDCFG_EVAL_RMS:
		/*
		 * Root mean square over all possible drive failures
		 * penalizes higher imbalance more.
		 */
		value = sqrt(((double)sumsq) / n);
		break;
	default:
		VERIFY(0);
	}

	/* Average number of resilver IOs per child. */
	double child_avg = (total_required_ios / n) / map->dm_children;

	return (value / child_avg);
}

/*
 * Return a random vaule in the requested range.
 */
static int
rand_in_range(int min, int count)
{
	return (min + drand48() * count);
}

/*
 * Permute a random percentage of rows in the map.
 */
static void
permute_map(draid_map_t *map, int percent)
{
	int nperms, row;

	/* Select a percentage of the total rows. */
	if (percent < 1) {
		nperms = 1;
	} else if (percent > 100) {
		nperms = map->dm_nperms;
	} else {
		nperms = rand_in_range(1, (map->dm_nperms * percent) / 100);
	}

	/* Select a random starting row. */
	row = rand_in_range(0, map->dm_nperms - nperms);

	for (int i = row; i < row + nperms; i++)
		permute_devs(&map->dm_perms[i * map->dm_children],
		    &map->dm_perms[i * map->dm_children], map->dm_children);
}

static int
get_urandom(uint8_t *buf, size_t bytes)
{
	int fd = open("/dev/urandom", O_RDONLY);
	if (fd < 0)
		return (fd);

	ssize_t bytes_read = 0;
	while (bytes_read < bytes) {
		ssize_t rc = read(fd, buf + bytes_read, bytes - bytes_read);
		if (rc < 0)
			break;
		bytes_read += rc;
	}

	(void) close(fd);

	return (bytes_read);
}

/*
 * Optimize the map to evenly distribute IO over the children when
 * resilvering to a distributed spare.
 */
static draid_map_t *
optimize_map(vdev_draid_config_t *vdc, draid_map_t *map, draidcfg_eval_t eval,
    int faults)
{
	double temp = 100.0;
	double alpha = 0.995;
	double epsilon = 0.001;
	double score = eval_decluster(vdc, map, eval, faults);

	while (temp > epsilon) {
		draid_map_t *pmap = dup_map(map);

		permute_map(pmap, (int)temp);

		double pscore = eval_decluster(vdc, map, eval, faults);
		double delta = (pscore - score);

		/*
		 * Use the new map if it is an improvement.  On occasion
		 * accept a worst map to avoid local optimaa
		 */
		if (delta < 0 || exp(-10000 * delta / temp) > drand48()) {
			free_map(map);
			map = pmap;
			score = pscore;
		} else {
			free_map(pmap);
		}

		temp *= alpha;
	}

	return (map);
}

/*
 * Generate a new candidate map for the vdc and optimized it.
 */
static draid_map_t *
generate_map(vdev_draid_config_t *vdc, uint64_t nperms, draidcfg_eval_t eval,
    int faults)
{
	uint64_t children = vdc->vdc_children;
	draid_map_t *map = alloc_map(children, nperms);

	/* Initialize the first row to a known pattern */
	for (int i = 0; i < children; i++)
		map->dm_perms[i] = i;

	for (int i = 1; i < nperms; i++) {
		uint64_t *prev_row = &map->dm_perms[(i - 1) * children];
		uint64_t *curr_row = &map->dm_perms[i * children];

		/* Permute the previous row to create the next row */
		permute_devs(prev_row, curr_row, children);
	}

	ASSERT0(check_map(map));

	/*
	 * Optimize the map.
	 */
	draid_map_t *opt_map = dup_map(map);
	opt_map = optimize_map(vdc, opt_map, eval, faults);

	double score = eval_decluster(vdc, map, eval, faults);
	double opt_score = eval_decluster(vdc, opt_map, eval, faults);

	if (score > opt_score) {
		/*
		 * optimize_map() may create a worse map, because the
		 * simulated annealing process may accept worse
		 * neighbors to avoid getting stuck in local optima
		 */
		free_map(opt_map);
		return (map);
	} else {
		free_map(map);
		return (opt_map);
	}
}


static int
draid_permutation_generate(vdev_draid_config_t *vdc, int passes,
    draidcfg_eval_t eval, int faults)
{
	long int best_seed = 0;
	draid_map_t *best_map = NULL;
	int i;

	/*
	 * In general, the larger the number of devices in the dRAID the
	 * more rows should be used in the mapping to improve the overall
	 * distribution.  However, due to the limited space in the vdev
	 * label (112k) no more than 32 rows are used.
	 *
	 * Example usage for a 255 vdev dRAID pool created using:
	 *
	 *   zpool create tank draid /var/tmp/testdir/file*
	 *
	 * ZFS Label NVList Config Stats:
	 *   86812 bytes used, 27836 bytes free (using 75.7%)
	 *
	 *   integers:  789  27432 bytes (31.60%)
	 *    strings:  513  22156 bytes (25.52%)
	 *   booleans:    3    132 bytes ( 0.15%)
	 *    nvlists:  259  37092 bytes (42.73%)
	 *
	 *  leaf vdevs:  255    210 bytes average
	 *                      212 bytes largest
	 */
	int nperms = 64;

	/*
	 * Perform the requested number of passes progressively developing
	 * the map by randomly permuting it.  The best version is kept.
	 */
	for (i = 0; i < passes; i++) {
		long int seed;

		/*
		 * Generate a random seed to be used to create the map.
		 * /dev/urandom provides sufficient randomness for this.
		 */
		(void) get_urandom((void *)&seed, sizeof (seed));
		srand48(seed);

		/*
		 * Generate a new random candidate map.
		 */
		draid_map_t *map = generate_map(vdc, nperms, eval, faults);

		if (best_map == NULL ||
		    eval_decluster(vdc, map, eval, faults) <
		    eval_decluster(vdc, best_map, eval, faults)) {
			if (best_map != NULL)
				free_map(best_map);
			best_map = map;
			best_seed = seed;
		} else {
			free_map(map);
		}
	}

	if (best_map != NULL) {
		ASSERT3S(best_seed, !=, 0);
		ASSERT3S(best_map->dm_nperms, ==, nperms);
		ASSERT3S(best_map->dm_children, ==, vdc->vdc_children);

		vdc->vdc_seed = best_seed;
		vdc->vdc_nperms = best_map->dm_nperms;
		vdc->vdc_perms = best_map->dm_perms;

		/*
		 * Single fault map scores for are added to the configuration
		 * to allow for easy assessment of the map quality.  See the
		 * comment above the draidcfg_eval_t typedef for a brief
		 * explanation of how to interpret the scores.  The original
		 * double is stored as a uint64_t by multiplying it by 1000.
		 */
		vdc->vdc_scores[0] = (uint64_t)(eval_decluster(vdc, best_map,
		    DRAIDCFG_EVAL_WORST, 1) * 1000.0);
		vdc->vdc_scores[1] = (uint64_t)(eval_decluster(vdc, best_map,
		    DRAIDCFG_EVAL_MEAN, 1) * 1000.0);
		vdc->vdc_scores[2] = (uint64_t)(eval_decluster(vdc, best_map,
		    DRAIDCFG_EVAL_RMS, 1) * 1000.0);

		kmem_free(best_map, sizeof (draid_map_t));

		return (0);
	} else {
		return (-1);
	}
}

/*
 * Known dRAID configurations provided by some pool layouts.
 *
 * As an optimization these configurations may be used instead of randomly
 * generating a new configuration.  Additional configurations can be added
 * as they are discovered.
 */
static nvlist_t *
find_known_config(uint64_t data, uint64_t parity, uint64_t spares,
    uint64_t children, char *fullpath)
{
	char *dirpath = DRAIDCFG_DEFAULT_DIR;
	char key[MAXNAMELEN];
	struct dirent *ent;
	nvlist_t *default_cfg = NULL;
	nvlist_t *vendor_cfg = NULL;
	int error;

	DIR *dir = opendir(dirpath);
	if (dir == NULL)
		return (NULL);

	while ((ent = readdir(dir)) != NULL) {
		(void) snprintf(fullpath, MAXPATHLEN - 1, "%s/%s",
		    dirpath, ent->d_name);
		(void) vdev_draid_name(key, sizeof (key), data, parity,
		    spares, children);

		/*
		 * Store the default configuration separately, it will be
		 * used if no vendor provided configuration was found.
		 */
		if (strcmp(ent->d_name, DRAIDCFG_DEFAULT_FILE) == 0 &&
		    default_cfg == NULL) {
			(void) vdev_draid_config_read_file(fullpath, key,
			    &default_cfg);
		} else {
			error = vdev_draid_config_read_file(fullpath, key,
			    &vendor_cfg);
			if (error == 0) {
				nvlist_free(default_cfg);
				closedir(dir);
				return (vendor_cfg);
			}
		}
	}

	closedir(dir);

	return (default_cfg);
}

static vdev_draid_config_t *
create_config(uint64_t data, uint64_t parity, uint64_t spares,
    uint64_t children, uint64_t ngroups, int passes, draidcfg_eval_t eval,
    int faults)
{
	vdev_draid_config_t *vdc;

	ASSERT(data != 0);
	ASSERT3U(data + parity, <=, children - spares);

	vdc = kmem_zalloc(sizeof (vdev_draid_config_t), KM_SLEEP);

	/*
	 * A unique guid which can be used to identify the map.
	 */
	(void) get_urandom((void *)&vdc->vdc_guid, sizeof (uint64_t));

	vdc->vdc_ndata = data;
	vdc->vdc_nparity = parity;
	vdc->vdc_nspares = spares;
	vdc->vdc_children = children;
	vdc->vdc_ngroups = ngroups;

	/*
	 * Derived constants.
	 */
	vdc->vdc_groupwidth = vdc->vdc_ndata + vdc->vdc_nparity;
	vdc->vdc_ndisks = vdc->vdc_children - vdc->vdc_nspares;
	vdc->vdc_groupsz = vdc->vdc_groupwidth * DRAID_ROWSIZE;
	vdc->vdc_devslicesz = (vdc->vdc_groupsz * vdc->vdc_ngroups) /
	    vdc->vdc_ndisks;

	/*
	 * Generate permutation map.
	 */
	if (draid_permutation_generate(vdc, passes, eval, faults) != 0) {
		kmem_free(vdc, sizeof (vdev_draid_config_t));
		return (NULL);
	}

	ASSERT3U(vdc->vdc_groupwidth, >=, 2);
	ASSERT3U(vdc->vdc_groupwidth, <=, vdc->vdc_ndisks);
	ASSERT3U(vdc->vdc_groupsz, >=, 2 * DRAID_ROWSIZE);
	ASSERT3U(vdc->vdc_devslicesz, >=, DRAID_ROWSIZE);
	ASSERT3U(vdc->vdc_devslicesz % DRAID_ROWSIZE, ==, 0);
	ASSERT3U((vdc->vdc_groupwidth * vdc->vdc_ngroups) %
	    vdc->vdc_ndisks, ==, 0);

	return (vdc);
}

static void
free_config(vdev_draid_config_t *vdc)
{
	kmem_free((void *)vdc->vdc_perms,
	    sizeof (uint64_t) * vdc->vdc_nperms + vdc->vdc_children);
	kmem_free(vdc, sizeof (*vdc));
}

/*
 * Generate a dRAID configuration for the given configuration.
 * The caller is responsible for freeing the new dRAID configuration.
 *
 * Inputs:
 * - data:     number of data blocks per group (1-32)
 * - parity:   number of parity blocks per group (1-3)
 * - spares:   number of distributed spare (1-100)
 * - children: total number of devices (1-255)
 *
 * Valid values must be specified for the data, parity, spares and
 * children arguments.
 *
 * A dRAID configuration is derived by creating slices made up of
 * groups (each group has data + parity logical drives).
 */
draidcfg_err_t
vdev_draid_config_generate(uint64_t data, uint64_t parity, uint64_t spares,
    uint64_t children, int passes, draidcfg_eval_t eval, int faults,
    nvlist_t **cfgp)
{
	vdev_draid_config_t *vdc;
	uint64_t ngroups = 1;
	uint8_t *value;
	nvlist_t *cfg;

	/*
	 * Verify the maximum allow group size is never exceeded.
	 */
	if (data == 0 || (data + parity > VDEV_DRAID_MAX_GROUPSIZE)) {
		return (DRAIDCFG_ERR_DATA_INVALID);
	}

	if (parity == 0 || parity > VDEV_DRAID_MAX_PARITY)
		return (DRAIDCFG_ERR_PARITY_INVALID);

	/*
	 * Verify the requested number of spares can be satisfied.
	 */
	if (spares == 0 || spares > VDEV_DRAID_MAX_SPARES ||
	    spares > (children - (data + parity))) {
		return (DRAIDCFG_ERR_SPARES_INVALID);
	}

	/*
	 * Verify the requested number children is sufficient.
	 */
	if (children == 0 || children > VDEV_DRAID_MAX_CHILDREN ||
	    children < (data + parity + spares)) {
		return (DRAIDCFG_ERR_CHILDREN_INVALID);
	}

	/*
	 * Calculate the minimum number of groups required to fill a slice.
	 * This is the LCM of the stripe width (data + parity) and the
	 * number of data drives (children - spares).
	 */
	while (ngroups * (data + parity) % (children - spares) != 0)
		ngroups++;

	if (ngroups > VDEV_DRAID_MAX_GROUPS)
		return (DRAIDCFG_ERR_GROUPS_INVALID);

	if (passes == 0)
		passes = DRAIDCFG_DEFAULT_PASSES;

	/*
	 * Verify the dRAID configuration constraints have been satisfied.
	 */
	if (ngroups * (data + parity) % (children - spares) != 0)
		return (DRAIDCFG_ERR_LAYOUT);

	/*
	 * Determine if an existing known optimized configuration exists
	 * for the given layout.  If it does, then use it.  Otherwise
	 * generate a new one to the requested level of optimization.
	 */
	char fullpath[MAXNAMELEN];
	cfg = find_known_config(data, parity, spares, children, fullpath);
	if (cfg != NULL) {
		draidcfg_err_t error = vdev_draid_config_validate(cfg,
		    data, parity, spares, children);
		if (error == DRAIDCFG_OK) {
			*cfgp = cfg;
			return (DRAIDCFG_OK);
		}

		/*
		 * The known configuration read from disk failed validation.
		 * This shouldn't happen unless the packed nvlists on disk
		 * have somehow been damaged.  Generate a new configuration.
		 */
		fprintf(stderr, "Invalid dRAID configuration '%s': %d\n"
		    "Generating new configuration for use\n", fullpath, error);

		vdev_draid_config_free(cfg);
	}

	/* Generate configuration, should not fail after this point */
	vdc = create_config(data, parity, spares, children, ngroups, passes,
	    eval, faults);
	if (vdc == NULL)
		return (DRAIDCFG_ERR_INTERNAL);

	cfg = fnvlist_alloc();

	/* Store the basic dRAID configuration. */
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_GUID, vdc->vdc_guid);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_SEED, vdc->vdc_seed);
	fnvlist_add_uint64_array(cfg, ZPOOL_CONFIG_DRAIDCFG_SCORES,
	    (uint64_t *)vdc->vdc_scores, 3);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_DATA, data);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_PARITY, parity);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_SPARES, spares);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_CHILDREN, children);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_GROUPS, ngroups);

	/* Store the number of permutations followed by the permutations */
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_NPERMS, vdc->vdc_nperms);

	int size = children * vdc->vdc_nperms * sizeof (uint8_t);
	value = kmem_zalloc(size, KM_SLEEP);

	for (int i = 0; i < vdc->vdc_nperms; i++) {
		for (int j = 0; j < children; j++) {
			uint64_t c = vdc->vdc_perms[i * children + j];
			ASSERT3U(c, <, children);
			ASSERT3U(c, <=, VDEV_DRAID_MAX_CHILDREN);
			value[i * children + j] = (uint8_t)c;
		}
	}
	fnvlist_add_uint8_array(cfg, ZPOOL_CONFIG_DRAIDCFG_PERMS,
	    value, children * vdc->vdc_nperms);
	kmem_free(value, size);

	/*
	 * Verify the generated configuration.  A failure indicates an
	 * internal error with the dRAID configuration generation code.
	 */
	VERIFY0(vdev_draid_config_validate(cfg, data, parity, spares,
	    children));
	free_config(vdc);

	*cfgp = cfg;

	return (DRAIDCFG_OK);
}

/*
 * Free an allocated dRAID configuration.
 */
void
vdev_draid_config_free(nvlist_t *cfg)
{
	nvlist_free(cfg);
}

static int
vdev_draid_config_read_file_impl(const char *path, nvlist_t **allcfgs)
{
	int error, fd;

	fd = open(path, O_RDONLY);
	if (fd < 0)
		return (errno);

	struct stat64 stat;
	if (fstat64(fd, &stat) != 0) {
		error = errno;
		(void) close(fd);
		return (error);
	}

	if (!S_ISREG(stat.st_mode) || stat.st_size == 0) {
		(void) close(fd);
		return (EINVAL);
	}

	char *buf = kmem_alloc(stat.st_size, KM_SLEEP);
	if (buf == NULL) {
		(void) close(fd);
		return (ENOMEM);
	}

	ssize_t rc, bytes = 0;
	while (bytes < stat.st_size) {
		size_t size = MIN(stat.st_size - bytes, 131072);
		rc = read(fd, buf + bytes, size);
		if (rc < 0) {
			error = errno;
			kmem_free(buf, stat.st_size);
			(void) close(fd);
			return (error);
		} else if (rc == 0) {
			break;
		} else {
			bytes += rc;
		}
	}

	(void) close(fd);
	error = nvlist_unpack(buf, stat.st_size, allcfgs, 0);
	kmem_free(buf, stat.st_size);

	return (error);
}

/*
 * Read a dRAID configuration from the file at the specific path.  Each
 * file can contain multiple dRAID configurations which are indexed by a
 * unique name (i.e draid2:2g:1s:21c).  The caller is responsible for
 * validating and freeing the configuration returned via the cfg argument.
 */
int
vdev_draid_config_read_file(const char *path, char *key, nvlist_t **cfg)
{
	nvlist_t *allcfgs, *foundcfg = NULL;
	int error;

	error = vdev_draid_config_read_file_impl(path, &allcfgs);
	if (error != 0)
		return (error);

	nvlist_lookup_nvlist(allcfgs, key, &foundcfg);
	if (foundcfg != NULL) {
		nvlist_dup(foundcfg, cfg, KM_SLEEP);
		error = 0;
	} else {
		error = ENOENT;
	}

	nvlist_free(allcfgs);

	return (error);
}

/*
 * Write the provided dRAID configuration to the specified file path.
 * If a file already exists at the provided and append is set the new
 * configuration will be added to it provided no matching key already
 * exists.  Otherwise an error is returned.
 */
int
vdev_draid_config_write_file(const char *path, char *key, nvlist_t *cfg,
    boolean_t append)
{
	char tmppath[] = "/tmp/draid.cfg.XXXXXX";
	nvlist_t *allcfgs;
	int error, fd;

	if (append) {
		error = vdev_draid_config_read_file_impl(path, &allcfgs);
		if (error == ENOENT)
			allcfgs = fnvlist_alloc();
		else if (error != 0)
			return (error);
	} else {
		struct stat64 st;
		if (lstat64(path, &st) == 0)
			return (EEXIST);
		else
			allcfgs = fnvlist_alloc();
	}

	error = nvlist_add_nvlist(allcfgs, key, cfg);
	if (error) {
		nvlist_free(allcfgs);
		return (error);
	}

	size_t buflen = 0;
	error = nvlist_size(allcfgs, &buflen, NV_ENCODE_XDR);
	if (error) {
		nvlist_free(allcfgs);
		return (error);
	}

	char *buf = kmem_zalloc(buflen, KM_SLEEP);
	error = nvlist_pack(allcfgs, &buf, &buflen, NV_ENCODE_XDR, KM_SLEEP);
	if (error) {
		kmem_free(buf, buflen);
		nvlist_free(allcfgs);
		return (error);
	}

	nvlist_free(allcfgs);

	/*
	 * Atomically update the file using a temporary file and the
	 * traditional unlink then rename steps.  This code provides
	 * no locking, it only guarantees the packed nvlist on disk
	 * is updated atomically and is internally consistent.
	 */
	fd = mkstemp(tmppath);
	if (fd < 0) {
		kmem_free(buf, buflen);
		return (errno);
	}

	ssize_t rc, bytes = 0;
	while (bytes < buflen) {
		size_t size = MIN(buflen - bytes, 131072);
		rc = write(fd, buf + bytes, size);
		if (rc < 0) {
			error = errno;
			kmem_free(buf, buflen);
			(void) close(fd);
			(void) unlink(tmppath);
			return (error);
		} else if (rc == 0) {
			break;
		} else {
			bytes += rc;
		}
	}

	kmem_free(buf, buflen);
	close(fd);

	if (bytes != buflen) {
		(void) unlink(tmppath);
		return (EIO);
	}

	/*
	 * Unlink the previous config file and replace it with the updated
	 * version.  If we're able to unlink the file then directory is
	 * writable by us and the subsequent rename should never fail.
	 */
	error = unlink(path);
	if (error != 0 && errno != ENOENT) {
		(void) unlink(tmppath);
		return (errno);
	}

	error = rename(tmppath, path);
	if (error != 0) {
		(void) unlink(tmppath);
		return (errno);
	}

	return (0);
}

/*
 * Print a descriptive error message the dRAID configuration error.
 */
void
vdev_draid_config_print_error(draidcfg_err_t error)
{
	switch (error) {
	case DRAIDCFG_OK:
		(void) fprintf(stderr, "Valid dRAID configuration\n");
		break;
	case DRAIDCFG_ERR_CHILDREN_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_CHILDREN);
		break;
	case DRAIDCFG_ERR_CHILDREN_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_CHILDREN);
		break;
	case DRAIDCFG_ERR_CHILDREN_MISMATCH:
		(void) fprintf(stderr, "Inconsistent %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_CHILDREN);
		break;
	case DRAIDCFG_ERR_PARITY_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_PARITY);
		break;
	case DRAIDCFG_ERR_PARITY_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_PARITY);
		break;
	case DRAIDCFG_ERR_PARITY_MISMATCH:
		(void) fprintf(stderr, "Inconsistent %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_PARITY);
		break;
	case DRAIDCFG_ERR_GROUPS_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_GROUPS);
		break;
	case DRAIDCFG_ERR_GROUPS_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_GROUPS);
		break;
	case DRAIDCFG_ERR_GROUPS_MISMATCH:
		(void) fprintf(stderr, "Inconsistent %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_GROUPS);
		break;
	case DRAIDCFG_ERR_SPARES_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_SPARES);
		break;
	case DRAIDCFG_ERR_SPARES_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_SPARES);
		break;
	case DRAIDCFG_ERR_SPARES_MISMATCH:
		(void) fprintf(stderr, "Inconsistent %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_SPARES);
		break;
	case DRAIDCFG_ERR_DATA_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_DATA);
		break;
	case DRAIDCFG_ERR_DATA_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_DATA);
		break;
	case DRAIDCFG_ERR_DATA_MISMATCH:
		(void) fprintf(stderr, "Inconsistent %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_DATA);
		break;
	case DRAIDCFG_ERR_NPERMS_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_NPERMS);
		break;
	case DRAIDCFG_ERR_NPERMS_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_NPERMS);
		break;
	case DRAIDCFG_ERR_PERMS_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_PERMS);
		break;
	case DRAIDCFG_ERR_PERMS_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_PERMS);
		break;
	case DRAIDCFG_ERR_PERMS_MISMATCH:
		(void) fprintf(stderr, "Inconsistent %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_PERMS);
		break;
	case DRAIDCFG_ERR_PERMS_DUPLICATE:
		(void) fprintf(stderr, "Duplicate %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_PERMS);
		break;
	case DRAIDCFG_ERR_LAYOUT:
		(void) fprintf(stderr, "Invalid dRAID layout");
		break;
	case DRAIDCFG_ERR_INTERNAL:
		(void) fprintf(stderr, "Internal error\n");
		break;
	}
}
#endif /* _KERNEL */

EXPORT_SYMBOL(vdev_draid_config_validate);
EXPORT_SYMBOL(vdev_draid_spare_name);
EXPORT_SYMBOL(vdev_draid_spare_values);
EXPORT_SYMBOL(vdev_draid_is_spare);
