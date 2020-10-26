#!/usr/bin/env python3

# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import math
import numpy as np
import pandas as pd
from abc import ABC
from astropy.coordinates import SkyCoord


__all__ = ["ColumnGenerator", "ObjIdGenerator", "FilterGenerator",
           "RaDecGenerator", "MagnitudeGenerator",
           "UniformGenerator", "PoissonGenerator",
           "ForcedSourceGenerator",
           "VisitIdGenerator", "mergeBlocks"]


def calcSeedFrom(chunk_id, seed, column_seed=0):
    """Try to keep some separation between column generator seeds.

    Parameters
    ----------
    chunk_id : int
        Chunk id number.
    seed : int
        Random number seed.
    column_seed : int
        Arbitrary column value, should be different from other column values.

    Note
    ----
    This is an attempt to avoid having chunks and columns having seeds
    near each other so that changing the seed value by 1 will not look
    like everyting was just shifted over by 1 chunk/column.
    Each ColumnGenerator should have a unique arbitrary column_seed.
    """
    return (chunk_id*10000 + seed + column_seed*100)


def mergeBlocks(block_a, block_b):
    """Merge two block together where the lists should maintain the order of
    the individual elements.

    Parameters
    ----------
    block_a, block_b : tuple of lists
        Blocks of data generated by column generators.

    Return
    ------
    Tuple of lists
        Result of merging the two blocks.
    """
    return tuple(np.concatenate(x) for x in zip(block_a, block_b))


def convertBlockToRows(block):
    """Transpose the block.

    Return
    ------
    tuple of lists
        A new block where columns of 'block' are the rows of the output block.
    """
    rows = []
    rows_in_block = len(block[0])
    for j in range(rows_in_block):
        row = []
        for x in block:
            if len(x) != rows_in_block:
                raise IndexError
            row.append(x[j])
        rows.append(row)
    return rows


def containsBlock(block_a, block_b):
    """Return True if every row in blockA has a matching row in blockB.
    The data is stored in columns and it is easier to do the
    comparison using rows.
    """
    if len(block_a) != len(block_b):
        return False
    if len(block_a) == 0:
        return True

    # Convert blockA into a list of rows.
    try:
        rows_a = convertBlockToRows(block_a)
        rows_b = convertBlockToRows(block_b)
        for row in rows_a:
            # if the row is found in rowsB, delete it in rowsB
            found = False
            for j in range(len(rows_b)):
                if row == rows_b[j]:
                    found = True
                    print("found row ", row)
                    del rows_b[j]
                    break
            if not found:
                print("missing row ", row)
                return False

    except IndexError:
        print("Malformed block A=", block_a, " B=", block_b)
        return False
    return True


class SimpleBox:
    """
    A simple RA DEC box where raA is always smaller than raB and
    decA is always smaller than decB and the units are degrees.

    Parameters
    ----------
    raA, raB : float
        RA of two opposite corners of the box.
    decA, decB : float
        Declination of two opposite corners of the box.

    Note
    ----
    This has no internal mechanism to fix wrap around from 360 to 0, so a
    box that includes lon 0 should go from -1 to 1 not 359 to 1.
    Dec values are constrained [-90, 90]
    """
    def __init__(self, raA, raB, decA, decB):
        self.raA = raA
        self.raB = raB
        self.decA = decA
        self.decB = decB
        if (raA > raB):
            tmp = raA
            raA = raB
            raB = tmp
        if (decA > decB):
            tmp = decA
            decA = decB
            decB = tmp
        if (decA > 90.0):
            decA = 90.0
        if (decA < -90.0):
            decA = -90.0
        if (decB > 90.0):
            decB = 90.0
        if (decB < -90.0):
            decB = -90.0

    def __repr__(self):
        return ('{raA=' + str(self.raA) + ' raB=' + str(self.raB) +
                ' decA=' + str(self.decA) + ' decB=' + str(self.decB) + '}')

    def __str__(self):
        return self.__repr__()

    def area(self):
        """
        Return
        ------
        area : float, square degrees
        Returns the approximate area of the box on a sphere, requires degrees
        """
        area = abs(self.raA - self.raB) * abs(self.decA - self.decB)
        avgDecDeg = (self.decA + self.decB) / 2
        avgDecRad = avgDecDeg * (math.pi/180.0)
        area = math.cos(avgDecRad) * area
        return area


class ColumnGenerator(ABC):

    def __call__(self, box, length, seed):
        """
        Parameters
        ----------
        box : SimpleBox
            Bounding box in which to generate sources.
        length : int
            Number of coordiantes to generate
        seed : int
            Seed value for the random number generator to
            generate repeatable results.

        Returns
        -------
        columns: tuple
            One or more arrays containing the data for each column
        """
        return NotImplemented


class RaDecGenerator(ColumnGenerator):
    """This class generate pseudorandom repeatanble RA and Declination.


    Parameters
    ----------
    chunker : sphgeom.Chunker
        Chunker to provide RA and Dec limits of a chunk.
    ignore_edge_only : bool
        Generate the entire chunk's worth of RA and Dec even if edge_only
        is specified.

    Note
    ----
    It starts by generating edges first so edge_only and complete chunks
    will have matching edges.
    """
    def __init__(self, ignore_edge_only=False, include_err=False):
        self.ignore_edge_only = ignore_edge_only
        self.column_seed = 1
        # avoid having the same ra and dec in different tables.
        if self.ignore_edge_only:
            self.column_seed = 2
        self.include_err = include_err

    def __call__(self, box, length, seed, edge_width=0.0, edge_only=False, unique_box_id=0, **kwargs):
        """
        Parameters
        ----------
        chunk_id : int
            Chunk id number where RA and Dec values are needed.
        length : int
            Number of coordiantes to generate.
        seed : int
            Random number seed.
        edge_width : float degrees, optional
            Width of the edge generated. Must be wide enough to cover
            overlap and must be consistent throughout chunk generation.
        edge_only : bool, optional
            When True, only generate the values near edges of chunks,
            edge_width. Otherwise, generate edge values first and then
            generate values for the middle of the chunk.

        Returns
        -------
        a tuple of a list of generated RA's and a list of generated Dec's.
        """
        np.random.seed(calcSeedFrom(unique_box_id, seed, self.column_seed))

        ra_min = box.raA
        ra_delta = box.raB - box.raA
        dec_min = box.decA
        dec_delta = box.decB - box.decA
        ra_centers = np.random.random(length)*ra_delta + ra_min
        dec_centers = np.random.random(length)*dec_delta + dec_min

        ra_centers += 360 * (ra_centers < 0.0)
        ra_centers -= 360 * (ra_centers >= 360.0)

        if not self.include_err:
            return (ra_centers, dec_centers)
        else:
            ra_err = np.random.random(length) * 2e-6
            dec_err = np.random.random(length) * 2e-6

            return (ra_centers, ra_err, dec_centers, dec_err)


class ObjIdGenerator(ColumnGenerator):

    def __call__(self, box, length, seed, unique_box_id=0, **kwargs):
        """
        Returns
        -------
        object_id : array
            Array containing unique IDs for each object
        """

        # TODO: more than 100k objects in a chunk will cause issues
        #       Replace 100000 with max_objects_per_chunk?
        return (unique_box_id * 100000) + np.arange(length)


class VisitIdGenerator(ColumnGenerator):

    def __call__(self, box, length, seed, unique_box_id=0, **kwargs):
        """
        Returns
        -------
        visit_id : array
            Array containing unique IDs for each visit
        """

        # TODO: This shouldn't have the same issue as objects/chunk
        #       but maybe replace 100000 with max_objects_per_chunk?
        return 10000000000 + (unique_box_id * 100000) + np.arange(length)


class MagnitudeGenerator(ColumnGenerator):
    """
    Parameters
    ----------
    n_mags : int
        Number of magnitude columns to make.
    min_mag : float
        Minimum value for generated magnitudes.
    max_mag : float
        Maximum value for generated magnitudes.
    column_seed : int
        Arbitrary integer that should be different from other
        column values. Used in random number generation.

    Note
    ----
    Currently generates a flat magnitude distribution. Should properly
    be some power law.
    If there is more than one call to this in a single table, there will
    be correlation between rows as the same random numbers will be used.
    """

    def __init__(self, n_mags=1, min_mag=0, max_mag=27.5, column_seed=7):
        self.n_mags = n_mags
        self.min_mag = min_mag
        self.max_mag = max_mag
        self.column_seed = column_seed  # arbitrary, but different from other columns

    def __call__(self, box, length, seed, unique_box_id=0, **kwargs):

        np.random.seed(calcSeedFrom(unique_box_id, seed, self.column_seed))

        # This needs to be made row by row not column by column, as
        # row by row results in repeatable values when doing edges first.
        magRows = []
        delta_mag = self.max_mag - self.min_mag
        for _ in range(length):
            mag = np.random.rand(self.n_mags)*delta_mag + self.min_mag
            magRows.append(mag)
        magCols = convertBlockToRows(magRows)
        return magCols


class UniformGenerator(ColumnGenerator):
    """
    Parameters
    ----------
    n_mags : int
        Number of magnitude columns to make.
    min_mag : float
        Minimum value for generated magnitudes.
    max_mag : float
        Maximum value for generated magnitudes.
    column_seed : int
        Arbitrary integer that should be different from other
        column values. Used in random number generation.

    Note
    ----
    Currently generates a flat magnitude distribution. Should properly
    be some power law.
    """

    def __init__(self, n_columns=1, min_val=0, max_val=1, column_seed=7):
        self.n_columns = n_columns
        self.min_val = min_val
        self.max_val = max_val
        self.column_seed = column_seed  # arbitrary, but different from other columns

    def __call__(self, box, length, seed, unique_box_id=0, **kwargs):

        np.random.seed(calcSeedFrom(unique_box_id, seed, self.column_seed))

        # This needs to be made row by row not column by column, as
        # row by row results in repeatable values when doing edges first.
        columns = []
        delta_value = self.max_val - self.min_val
        for _ in range(self.n_columns):
            values = delta_value * np.random.rand(length) + self.min_val
            columns.append(values)
        return columns

class PoissonGenerator(ColumnGenerator):

    def __init__(self, n_columns=1, mean_val=10, column_seed=7):
        self.n_columns = n_columns
        self.mean_value = mean_val
        self.column_seed = column_seed

    def __call__(self, box, length, seed, unique_box_id=0, **kwargs):

        np.random.seed(calcSeedFrom(unique_box_id, seed, self.column_seed))

        columns = []
        for _ in range(self.n_columns):
            values = np.random.poisson(self.mean_value, length)
            columns.append(values)
        return columns

class FilterGenerator(ColumnGenerator):
    """Class to generate random filter columns.

    Parameters
    ----------
    filters : str
        String where each character is a valid filter id.
    column_seed : int
        Arbitrary integer that should be different from other
        column values. Used in random number generation.
    """

    def __init__(self, filters="ugrizy", column_seed=6):
        self.filters = filters
        self.column_seed = column_seed

    def __call__(self, box, length, seed, unique_box_id=0, **kwargs):
        np.random.seed(calcSeedFrom(unique_box_id, seed, self.column_seed))
        return np.random.choice(list(self.filters), length)


class ForcedSourceGenerator(ColumnGenerator):
    """Class to generate ForcedSource columns from Object and Visit tables.

    Parameters
    ----------
    filters : str
        String where each character is a valid filter id. These
        should probably match the values given to FilterGenerator.
    visit_radius : float
        Distance from the visit center within which and object is
        considered part of that visit.
    column_seed : int
        Arbitrary integer that should be different from other
        column values. Used in random number generation.

    """

    def __init__(self, filters="ugrizy", visit_radius=0.30, column_seed=3):
        self.filters = filters
        self.visit_radius = visit_radius
        self.column_seed = column_seed

    def __call__(self, box, length, seed, prereq_row=None, prereq_tables=None, unique_box_id=0,
                 chunk_center=None, edge_only=False):
        assert prereq_tables is not None, "ForcedSourceGenerator requires the Visit table."
        assert chunk_center is not None, "Must supply chunk center"

        np.random.seed(calcSeedFrom(unique_box_id, seed, self.column_seed))

        visit_table = prereq_tables['CcdVisit']
        object_table = prereq_tables['Object']

        objects_inside_box = object_table[(object_table['psRa'] >= box.raA) &
                                          (object_table['psRa'] < box.raB) &
                                          (object_table['psDecl'] >= box.decA) &
                                          (object_table['psDecl'] < box.decB)]

        v_radius = self.visit_radius * 1.5  # Go a little bit bigger so nothing is missed.
        min_dec = box.decA - v_radius
        max_dec = box.decB + v_radius
        # If doing and edge_only chunk, there's no reason to fill the ForcedSource table
        # as it wont be used by the partitioner to make overlap tables. Only director
        # tables get overlap tables. Making an empty trimmed_visit table causes
        # an empty ForcedSource table, which is easier to deal with than a missing
        # ForcedSource table.
        if (edge_only):
            trimmed_visit = pd.DataFrame(data=None, columns=visit_table.columns)
        else:
            trimmed_visit = visit_table.loc[(visit_table['decl'] >= min_dec) &
                                            (visit_table['decl'] <= max_dec)]
        print(f"edge_only={edge_only} len trimmed={len(trimmed_visit)}  base={len(visit_table)}")

        visit_skycoords = SkyCoord(ra=trimmed_visit['ra'], dec=trimmed_visit['decl'], unit="deg")
        visit_deltas = chunk_center.separation(visit_skycoords).degree
        sel_matching_visits, = np.where(visit_deltas < self.visit_radius)
        n_matching_visits = len(sel_matching_visits)
        print(f"Found {n_matching_visits} matching visits")
        out_objectIds = np.repeat(objects_inside_box['objectId'].values, n_matching_visits)
        out_ccdVisitIds = np.tile(trimmed_visit['ccdVisitId'].iloc[sel_matching_visits].values,
                                  len(objects_inside_box))

        n_rows_total = n_matching_visits * len(objects_inside_box)
        psFlux = (np.repeat(objects_inside_box['gPsFlux'].values, n_matching_visits) +
                  np.random.randn(n_rows_total))
        psFluxSigma = np.zeros(n_rows_total) + 0.1
        flags = np.random.randint(200, size=n_rows_total)


        assert len(out_objectIds) == n_rows_total
        assert len(out_ccdVisitIds) == n_rows_total

        return (out_objectIds, out_ccdVisitIds, psFlux, psFluxSigma, flags)

