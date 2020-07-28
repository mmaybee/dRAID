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

	/* Validate configuration spares exists and are within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_SPARES, &s))
		return (DRAIDCFG_ERR_SPARES_MISSING);

	if (s == 0 || s > VDEV_DRAID_MAX_SPARES || s > (n - (d + p)))
		return (DRAIDCFG_ERR_SPARES_INVALID);

	if (required_spares != 0 && s != required_spares)
		return (DRAIDCFG_ERR_SPARES_MISMATCH);

	/* Validate configuration children exists and are within range. */
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_CHILDREN, &n))
		return (DRAIDCFG_ERR_CHILDREN_MISSING);

	if (n == 0 || (n - 1) > VDEV_DRAID_MAX_CHILDREN)
		return (DRAIDCFG_ERR_CHILDREN_INVALID);

	if (required_children != 0 && n != required_children)
		return (DRAIDCFG_ERR_CHILDREN_MISMATCH);

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
	if (nvlist_lookup_uint64(config, ZPOOL_CONFIG_DRAIDCFG_BASES, &b))
		return (DRAIDCFG_ERR_BASE_MISSING);

	if (b == 0)
		return (DRAIDCFG_ERR_BASE_INVALID);

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
	    ZPOOL_CONFIG_DRAIDCFG_PERM, &perm, &c)) {
		return (DRAIDCFG_ERR_PERM_MISSING);
	}

	/*
	 * Validate the permutation array size matches the expected size,
	 * that its elements are within the allowed range, and that there
	 * are no duplicates.
	 */
	if (c != b * n)
		return (DRAIDCFG_ERR_PERM_MISMATCH);

	for (uint64_t i = 0; i < b; i++) {
		for (uint64_t j = 0; j < n; j++) {
			uint64_t val = perm[i * n + j];

			if (val >= n)
				return (DRAIDCFG_ERR_PERM_INVALID);

			for (uint64_t k = 0; k < j; k++) {
				if (val == perm[i * n + k])
					return (DRAIDCFG_ERR_PERM_DUPLICATE);
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

/*
 * Additional logging which can be useful to developers when working
 * on the internal dRAID implementation.
 */
static int draid_debug = 0;

typedef struct draid_map
{
	int  ngroups;
	int *groupsz;
	int  nspares;
	int  ndevs;
	int  nrows;
	/* each row maps all drives, groups from 0, spares down from ndevs-1 */
	int **rows;
	int   nbroken; /* # broken drives */
	int  *broken; /* which drives are broken */
} draid_map_t;

typedef struct draid_pair
{
	int  value;
	int  order;
} draid_pair_t;

static void
permute_devs(int *in, int *out, int ndevs)
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

static void
print_map(draid_map_t *map)
{
	for (int i = 0; i < map->nrows; i++) {
		for (int j = 0; j < map->ndevs; j++) {
			if (j == map->ndevs - map->nspares)
				printf("S ");

			printf("%2d ", map->rows[i][j]);
		}
		printf("\n");
	}
}

static void
check_map(draid_map_t *map)
{
	int nrows = map->nrows;
	int ndevs = map->ndevs;
	int **rows = map->rows;

	ASSERT3S(map->ngroups, <=, VDEV_DRAID_MAX_GROUPS);
	ASSERT3S(map->nspares, <=, VDEV_DRAID_MAX_SPARES);
	ASSERT3S(map->nrows, <=, VDEV_DRAID_MAX_ROWS);
	ASSERT3S(map->nbroken, <=, VDEV_DRAID_MAX_SPARES);

	/* Ensure each dev appears once in every row */
	int *devcounts = kmem_zalloc(ndevs * sizeof (int), KM_SLEEP);

	for (int i = 0; i < nrows; i++) {
		int *row = rows[i];

		for (int j = 0; j < ndevs; j++) {
			int dev = row[j];

			ASSERT3S(0, <=, dev);
			ASSERT3S(dev, <, ndevs);
			ASSERT3S(devcounts[dev], ==, i);
			devcounts[dev] = i + 1;
		}
	}

	kmem_free(devcounts, ndevs * sizeof (int));

	/* Ensure broken drives only appear once */
	int *brokencounts = kmem_zalloc(ndevs * sizeof (int), KM_SLEEP);

	for (int i = 0; i < map->nbroken; i++) {
		ASSERT3S(map->broken[i], >=, 0);
		ASSERT3S(map->broken[i], <, ndevs);
		ASSERT0(brokencounts[i]); /* not used already */
		brokencounts[i] = 1;
	}

	kmem_free(brokencounts, ndevs * sizeof (int));
}

static draid_map_t *
dup_map(draid_map_t *oldmap)
{
	draid_map_t *map = kmem_alloc(sizeof (draid_map_t), KM_SLEEP);
	map->groupsz = kmem_alloc(sizeof (int) * oldmap->ngroups, KM_SLEEP);

	for (int i = 0; i < oldmap->ngroups; i++)
		map->groupsz[i] = oldmap->groupsz[i];

	map->ngroups = oldmap->ngroups;
	map->nspares = oldmap->nspares;
	map->ndevs = oldmap->ndevs;
	map->nrows = oldmap->nrows;
	map->rows = kmem_alloc(sizeof (int *) * map->nrows, KM_SLEEP);

	for (int i = 0; i < map->nrows; i++) {
		map->rows[i] = kmem_alloc(sizeof (int) * map->ndevs, KM_SLEEP);
		memcpy(map->rows[i], oldmap->rows[i],
		    sizeof (int) * map->ndevs);
	}

	/* Initialize to no failures (nothing broken) */
	map->broken = kmem_alloc(sizeof (int) * map->nspares, KM_SLEEP);
	map->nbroken = 0;

	check_map(map);

	return (map);
}

static draid_map_t *
new_map(int ndevs, int ngroups, int nspares, int nrows)
{
	draid_map_t *map = kmem_alloc(sizeof (draid_map_t), KM_SLEEP);
	int groupsz = (ndevs - nspares) / ngroups;
	int extra = (ndevs - nspares) % ngroups;

	ASSERT3S(nrows, <=, VDEV_DRAID_MAX_ROWS);
	ASSERT3S(ndevs, <=, (VDEV_DRAID_MAX_GROUPSIZE *
	    VDEV_DRAID_MAX_GROUPS) + VDEV_DRAID_MAX_SPARES);

	map->ngroups = ngroups;
	map->groupsz = kmem_alloc(sizeof (int) * ngroups, KM_SLEEP);

	for (int i = 0; i < ngroups; i++) {
		map->groupsz[i] = groupsz;
		if (i < extra) {
			map->groupsz[i] += 1;
		}
	}

	map->nspares = nspares;
	map->ndevs = ndevs;
	map->nrows = nrows;
	map->rows = kmem_alloc(sizeof (int *) * nrows, KM_SLEEP);

	for (int i = 0; i < nrows; i++) {
		map->rows[i] = kmem_alloc(sizeof (int) * ndevs, KM_SLEEP);

		if (i == 0) {
			for (int j = 0; j < ndevs; j++) {
				map->rows[i][j] = j;
			}
		} else {
			permute_devs(map->rows[i-1], map->rows[i], ndevs);
		}
	}

	/* Initialize to no failures (nothing broken) */
	map->broken = kmem_alloc(sizeof (int) * nspares, KM_SLEEP);
	map->nbroken = 0;

	check_map(map);

	return (map);
}

static void
free_map(draid_map_t *map)
{
	kmem_free(map->broken, sizeof (int) * map->nspares);
	for (int i = 0; i < map->nrows; i++)
		kmem_free(map->rows[i], sizeof (int) * map->ndevs);
	kmem_free(map->rows, sizeof (int *) * map->nrows);
	kmem_free(map, sizeof (draid_map_t));
}

static inline int
is_broken(draid_map_t *map, int dev)
{
	for (int i = 0; i < map->nbroken; i++)
		if (dev == map->broken[i])
			return (1);

	return (0);
}

/*
 * Evaluate how resilvering I/O will be distributed.
 */
static int
eval_resilver(draid_map_t *map, int print)
{
	int spare;
	int ndevs = map->ndevs;
	int nspares = map->nspares;
	int ngroups = map->ngroups;
	int groupsz;
	int nrows = map->nrows;
	int max_reads = 0;
	int max_writes = 0;
	int max_ios = 0;

	int *writes = kmem_zalloc(sizeof (int) * ndevs, KM_SLEEP);
	int *reads = kmem_zalloc(sizeof (int) * ndevs, KM_SLEEP);

	/* resilver all rows */
	for (int i = 0; i < nrows; i++) {
		int *row = map->rows[i];

		/* resilver all groups with broken drives */
		int index = 0;
		for (int j = 0; j < ngroups; j++) {
			int  fix = 0;

			/* See if any disk in this group is broken */
			groupsz = map->groupsz[j];
			ASSERT3S(index, <, ndevs - groupsz);
			for (int k = 0; k < groupsz && !fix; k++)
				fix = is_broken(map, row[index + k]);

			if (!fix) {
				index += groupsz;
				continue;
			}

			/*
			 * This group needs fixing
			 * Read all the non-broken drives and write all the
			 * broken drives to their hot spare for this row
			 */
			spare = ndevs - nspares;
			for (int k = 0; k < groupsz; k++) {
				int dev = row[index+k];

				if (!is_broken(map, dev)) {
					reads[dev]++;
				} else {
					ASSERT3S(spare, <, ndevs);

					while (is_broken(map, row[spare])) {
						spare++;
						ASSERT3S(spare, <, ndevs);
					}
					writes[row[spare++]]++;
				}
			}
			index += groupsz;
		}
	}

	/* find drives with most I/O */
	for (int i = 0; i < ndevs; i++) {
		if (reads[i] > max_reads)
			max_reads = reads[i];
		if (writes[i] > max_writes)
			max_writes = writes[i];

		if (reads[i] + writes[i] > max_ios)
			max_ios = reads[i] + writes[i];
	}

	if (print) {
		printf("Reads:  ");
		for (int i = 0; i < ndevs; i++)
			printf(" %5.3f", ((double)reads[i] * ngroups) / nrows);
		printf("\n");
		printf("Writes: ");
		for (int i = 0; i < ndevs; i++)
			printf(" %5.3f", ((double)writes[i] * ngroups) / nrows);
		printf("\n");
	}

	kmem_free(writes, sizeof (int) * ndevs);
	kmem_free(reads, sizeof (int) * ndevs);

	return (max_ios);
}

static double
eval_decluster(draid_map_t *map, draidcfg_eval_t eval, int faults, int print)
{
	int ios;
	int worst1 = -1;
	int worst2 = -1;
	int n = 0;
	long sum = 0;
	long sumsq = 0;
	long max_ios = 0;
	double val = 0;

	VERIFY0(eval_resilver(map, 0)); /* not broken already */
	VERIFY(faults == 1 || faults == 2);

	map->nbroken = faults;

	for (int f1 = 0; f1 < map->ndevs; f1++) {
		map->broken[0] = f1;

		if (faults < 2) {
			ios = eval_resilver(map, 0); /* eval single failure */
			n++;
			sum += ios;
			sumsq += ios*ios;
			if (max_ios < ios) {
				worst1 = f1;
				max_ios = ios;
			}
		} else { /* eval double failure */
			for (int f2 = f1 + 1; f2 < map->ndevs; f2++) {
				map->broken[1] = f2; /* use 2nd hot spare */

				ios = eval_resilver(map, 0);
				n++;
				sum += ios;
				sumsq += ios*ios;
				if (max_ios < ios) {
					worst1 = f1;
					worst2 = f2;
					max_ios = ios;
				}
			}
		}
	}
	map->nbroken = 0;

	if (print) {
		map->nbroken = faults;
		map->broken[0] = worst1;
		map->broken[2] = worst2;

		eval_resilver(map, 1);

		map->nbroken = 0;
	}

	switch (eval) {
	case DRAIDCFG_EVAL_WORST:
		/*
		 * Imbalance from worst possible drive failure
		 * insensitive to failures handled better.
		 */
		val = max_ios;
		break;
	case DRAIDCFG_EVAL_MEAN:
		/*
		 * Average over all possible drive failures
		 * sensitive to all possible failures.
		 */
		val = ((double)sum) / n;
		break;
	case DRAIDCFG_EVAL_RMS:
		/*
		 * Root mean square over all possible drive failures
		 * penalizes higher imbalance more.
		 */
		val = sqrt(((double)sumsq) / n);
		break;
	default:
		VERIFY(0);
	}

	return ((val / map->nrows) * map->ngroups);
}

static int
rand_in_range(int min, int count)
{
	return (min + drand48() * count);
}

static void
permute_map(draid_map_t *map, int temp)
{
	static int prev_temp;

	int nrows = (temp < 1) ? 1 : (temp > 100) ?
	    map->nrows : rand_in_range(1, (map->nrows * temp) / 100);
	int row = rand_in_range(0, map->nrows - nrows);
	int ncols = map->ndevs;
	int col = rand_in_range(0, map->ndevs - ncols);

	if (draid_debug > 0 && temp != prev_temp &&
	    (temp < 10 || (temp % 10 == 0))) {
		printf("Permute t %3d (%d-%d, %d-%d)\n",
		    temp, col, ncols, row, nrows);
	}
	prev_temp = temp;

	for (int i = row; i < row + nrows; i++)
		permute_devs(&map->rows[i][col], &map->rows[i][col], ncols);
}

static draid_map_t *
develop_map(draid_map_t *map)
{
	draid_map_t *dmap = new_map(map->ndevs, map->ngroups,
	    map->nspares, map->nrows * map->ndevs);

	for (int base = 0; base < map->nrows; base++) {
		for (int dev = 0; dev < map->ndevs; dev++) {
			for (int i = 0; i < map->ndevs; i++) {
				dmap->rows[base*map->ndevs + dev][i] =
				    (map->rows[base][i] + dev) % map->ndevs;
			}
		}
	}

	return (dmap);
}

static draid_map_t *
optimize_map(draid_map_t *map, draidcfg_eval_t eval, int faults)
{
	double temp    = 100.0;
	double alpha   = 0.995;
	double epsilon = 0.001;
	double val = eval_decluster(map, eval, faults, 0);
	int ups = 0;
	int downs = 0;
	int sames = 0;
	int iter = 0;

	while (temp > epsilon) {
		draid_map_t *map2 = dup_map(map);

		permute_map(map2, (int)temp);

		double val2  = eval_decluster(map2, eval, faults, 0);
		double delta = (val2 - val);

		if (delta < 0 || exp(-10000 * delta / temp) > drand48()) {
			if (delta > 0)
				ups++;
			else if (delta < 0)
				downs++;
			else
				sames++;

			free_map(map);
			map = map2;
			val = val2;
		} else {
			free_map(map2);
		}

		temp *= alpha;

		if ((++iter % 100) == 0) {
			if (draid_debug > 0) {
				printf("%f (%d ups, %d sames, %d downs)\n",
				    val, ups, sames, downs);
			}
			ups = downs = sames = 0;
		}
	}

	if (draid_debug > 0) {
		printf("%d iters, %d ups %d sames %d downs\n",
		    iter, ups, sames, downs);
	}

	return (map);
}

static void
print_map_stats(draid_map_t *map, draidcfg_eval_t eval, int print_ios)
{
	double score = eval_decluster(map, DRAIDCFG_EVAL_WORST, 1, 0);

	if (draid_debug) {
		printf("%6s (%2d - %2d / %2d) x %5d: %2.3f\n",
		    (eval == DRAIDCFG_EVAL_UNOPTIMIZED) ? "Unopt" :
		    (eval == DRAIDCFG_EVAL_WORST) ? "Worst" :
		    (eval == DRAIDCFG_EVAL_MEAN) ? "Mean"  : "Rms",
		    map->ndevs, map->nspares, map->ngroups, map->nrows, score);

		if (map->ndevs < 80 && score >= 1.05) {
			printf("Warning score %6.3f has over 5 percent "
			    "imbalance!\n", score);
		} else if (score >= 1.1) {
			printf("Warning score %6.3f has over 10 percent "
			    "imbalance!\n", score);
		}

		printf("Single: worst %6.3f mean %6.3f\n",
		    eval_decluster(map, DRAIDCFG_EVAL_WORST, 1, 0),
		    eval_decluster(map, DRAIDCFG_EVAL_MEAN, 1, 0));

		printf("Double: worst %6.3f mean %6.3f\n",
		    eval_decluster(map, DRAIDCFG_EVAL_WORST, 2, 0),
		    eval_decluster(map, DRAIDCFG_EVAL_MEAN, 2, 0));
	}

	if (print_ios) {
		eval_decluster(map, DRAIDCFG_EVAL_WORST, 1, 1);
		eval_decluster(map, DRAIDCFG_EVAL_WORST, 2, 1);
	}
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

static int
draid_permutation_generate(vdev_draid_config_t *vdc, int passes,
    draidcfg_eval_t eval, int faults)
{
	int nspares = vdc->vdc_nspares;
	int ngroups = vdc->vdc_ngroups;
	int ndevs = vdc->vdc_children;
	uint64_t guid;
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
	int nrows = 32;

	/*
	 * A unique guid which can be used to identify the map.
	 */
	if (get_urandom((void *)&guid, sizeof (guid)) != sizeof (guid))
		return (-1);

	/*
	 * Perform the requested number of passes progressively developing
	 * the map by randomly permuting it.  The best version is kept.
	 */
	for (i = 0; i < passes; i++) {
		draid_map_t *map, *omap;
		long int seed;

		/*
		 * Generate a random seed to be used to create the map.
		 * /dev/urandom provides sufficient randomness for this.
		 */
		if (get_urandom((void *)&seed, sizeof (seed)) != sizeof (seed))
			break;

		srand48(seed);

		map = new_map(ndevs, ngroups, nspares, nrows);
		omap = optimize_map(dup_map(map), eval, faults);
		if (eval_decluster(omap, eval, faults, 0) >
		    eval_decluster(map, eval, faults, 0)) {
			/*
			 * optimize_map() may create a worse map, because the
			 * simulated annealing process may accept worse
			 * neighbors to avoid getting stuck in local optima
			 */
			free_map(omap);
		} else {
			free_map(map);
			map = omap;
		}

		if (best_map == NULL ||
		    eval_decluster(map, eval, faults, 0) <
		    eval_decluster(best_map, eval, faults, 0)) {
			if (best_map != NULL)
				free_map(best_map);
			best_map = map;
			best_seed = seed;
		} else {
			free_map(map);
		}
	}

	if (draid_debug && (i != passes)) {
		fprintf(stderr, "Early termination at pass %d. Generated "
		    "permutations may not be optimal!\n", i + 1);
	}

	if (best_map != NULL) {
		draid_map_t *dmap;
		uint64_t *perms;

		ASSERT3S(best_seed, !=, 0);
		ASSERT3S(best_map->nrows, ==, nrows);
		ASSERT3S(best_map->ndevs, ==, vdc->vdc_children);

		perms = kmem_alloc(sizeof (*perms) * nrows * best_map->ndevs,
		    KM_SLEEP);

		for (i = 0; i < nrows; i++) {
			for (int j = 0; j < best_map->ndevs; j++) {
				perms[i * best_map->ndevs + j] =
				    best_map->rows[i][j];
			}
		}

		vdc->vdc_guid = guid;
		vdc->vdc_nbases = nrows;
		vdc->vdc_base_perms = perms;
		vdc->vdc_seed = best_seed;

		if (draid_debug > 1)
			print_map(best_map);

		dmap = develop_map(best_map);
		free_map(best_map);
		print_map_stats(dmap, eval, 0);
		free_map(dmap);
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
    uint64_t children)
{
	char *dirpath = DRAIDCFG_DEFAULT_DIR;
	char *fullpath, key[MAXNAMELEN];
	struct dirent *ent;
	nvlist_t *default_cfg = NULL;
	nvlist_t *vendor_cfg = NULL;
	int error;

	DIR *dir = opendir(dirpath);
	if (dir == NULL)
		return (NULL);

	fullpath = kmem_zalloc(MAXPATHLEN, KM_SLEEP);
	if (fullpath == NULL) {
		closedir(dir);
		return (NULL);
	}

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
				kmem_free(fullpath, MAXPATHLEN);
				closedir(dir);
				return (vendor_cfg);
			}
		}
	}

	kmem_free(fullpath, MAXPATHLEN);
	closedir(dir);

	return (default_cfg);
}

static vdev_draid_config_t *
create_config(uint64_t data, uint64_t parity, uint64_t spares,
    uint64_t children, int passes, draidcfg_eval_t eval, int faults)
{
	vdev_draid_config_t *vdc;

	ASSERT(data != 0);
	ASSERT3U(data + parity, <=, children - spares);

	vdc = kmem_zalloc(sizeof (vdev_draid_config_t), KM_SLEEP);
	vdc->vdc_ngroups = children - spares;
	vdc->vdc_ndata = data;
	vdc->vdc_nparity = parity;
	vdc->vdc_nspares = spares;
	vdc->vdc_children = children;

	vdc->vdc_nbases = 0;
	vdc->vdc_base_perms = NULL;
	if (draid_permutation_generate(vdc, passes, eval, faults) != 0) {
		kmem_free(vdc, sizeof (vdev_draid_config_t));
		return (NULL);
	}

	ASSERT3U(vdc->vdc_nbases, !=, 0);
	ASSERT3P(vdc->vdc_base_perms, !=, NULL);

	return (vdc);
}

static void
free_config(vdev_draid_config_t *vdc)
{
	kmem_free((void *)vdc->vdc_base_perms,
	    sizeof (uint64_t) * vdc->vdc_nbases + vdc->vdc_children);
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
 * (children - spares) groups (each group has data + parity logical
 * drives).
 */
draidcfg_err_t
vdev_draid_config_generate(uint64_t data, uint64_t parity, uint64_t spares,
    uint64_t children, int passes, draidcfg_eval_t eval, int faults,
    nvlist_t **cfgp)
{
	vdev_draid_config_t *vdc;
	uint64_t groups = 1;
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
	while (groups * (data + parity) % (children - spares) != 0)
		groups++;

	if (groups > VDEV_DRAID_MAX_GROUPS)
		return (DRAIDCFG_ERR_GROUPS_INVALID);

	if (passes == 0)
		passes = DRAIDCFG_DEFAULT_PASSES;

	/*
	 * Verify the dRAID configuration constraints have been satisfied.
	 */
	if (groups * (data + parity) % (children - spares) != 0)
		return (DRAIDCFG_ERR_LAYOUT);

	/*
	 * Determine if an existing known optimized configuration exists
	 * for the given layout.  If it does, then use it.  Otherwise
	 * generate a new one to the requested level of optimization.
	 */
	cfg = find_known_config(data, parity, spares, children);
	if (cfg != NULL) {
		if (vdev_draid_config_validate(cfg, data, parity, spares,
		    children) == DRAIDCFG_OK) {
			*cfgp = cfg;
			return (DRAIDCFG_OK);
		}

		/*
		 * The known configuration read from disk failed validation.
		 * This shouldn't happen unless the packed nvlists on disk
		 * have somehow been damaged.  Generate a new configuration.
		 */
		if (draid_debug)
			fprintf(stderr, "Invalid known configuration\n");

		vdev_draid_config_free(cfg);
	}

	/* Generate configuration, should not fail after this point */
	vdc = create_config(data, parity, spares, children, passes,
	    eval, faults);
	if (vdc == NULL)
		return (DRAIDCFG_ERR_INTERNAL);

	cfg = fnvlist_alloc();

	/* Store the basic dRAID configuration. */
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_DATA, data);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_PARITY, parity);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_SPARES, spares);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_CHILDREN, children);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_GROUPS, groups);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_GUID, vdc->vdc_guid);
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_SEED, vdc->vdc_seed);

	/* Store the number of permutations followed by the permutations */
	fnvlist_add_uint64(cfg, ZPOOL_CONFIG_DRAIDCFG_BASES, vdc->vdc_nbases);
	value = kmem_zalloc(children * vdc->vdc_nbases * sizeof (uint8_t),
	    KM_SLEEP);
	for (int i = 0; i < vdc->vdc_nbases; i++) {
		for (int j = 0; j < children; j++) {
			uint64_t c = vdc->vdc_base_perms[i * children + j];
			ASSERT3U(c, <, children);
			ASSERT3U(c, <=, VDEV_DRAID_MAX_CHILDREN);
			value[i * children + j] = (uint8_t)c;
		}
	}
	fnvlist_add_uint8_array(cfg, ZPOOL_CONFIG_DRAIDCFG_PERM,
	    value, children * vdc->vdc_nbases);
	kmem_free(value, children * vdc->vdc_nbases * sizeof (uint8_t));

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
 * If a file already exists at the provided path the new configuration
 * will be added to it provided no matching key already exists.
 */
int
vdev_draid_config_write_file(const char *path, char *key, nvlist_t *cfg)
{
	char tmppath[] = "/tmp/draid.cfg.XXXXXX";
	nvlist_t *allcfgs;
	int error, fd;

	error = vdev_draid_config_read_file_impl(path, &allcfgs);
	if (error != 0)
		allcfgs = fnvlist_alloc();

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
	case DRAIDCFG_ERR_BASE_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_BASES);
		break;
	case DRAIDCFG_ERR_BASE_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_BASES);
		break;
	case DRAIDCFG_ERR_PERM_MISSING:
		(void) fprintf(stderr, "Missing %s key in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_PERM);
		break;
	case DRAIDCFG_ERR_PERM_INVALID:
		(void) fprintf(stderr, "Invalid %s value in configuration\n",
		    ZPOOL_CONFIG_DRAIDCFG_PERM);
		break;
	case DRAIDCFG_ERR_PERM_MISMATCH:
		(void) fprintf(stderr, "Inconsistent %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_PERM);
		break;
	case DRAIDCFG_ERR_PERM_DUPLICATE:
		(void) fprintf(stderr, "Duplicate %s value in "
		    "configuration\n", ZPOOL_CONFIG_DRAIDCFG_PERM);
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
