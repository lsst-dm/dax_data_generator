
import numpy as np
from abc import ABC
import healpy

__all__ = ["ColumnGenerator", "ObjIdGenerator", "FilterGenerator",
           "RaDecGenerator", "MagnitudeGenerator", "ForcedSourceGenerator"]


class ColumnGenerator(ABC):

    def __call__(self, cell_id, length):
        """
        Parameters
        ----------
        cell_id : int
            Identifier of which cell on the sky to in which
            coordinates should be generated

        length : int
            Number of coordiantes to generate

        Returns
        -------
        columns: tuple
            One or more arrays containing the data for each column
        """
        return NotImplemented


class CcdVisitGenerator(ColumnGenerator):

    def __init__(self, chunker, ccd_visits_per_chunk, filters="ugrizy"):
        # TODO: Is this CcdVisits or Visits?
        self.chunker = chunker
        self.filters = filters
        self.ccd_visits_per_chunk = ccd_visits_per_chunk

    def _find_hpix8_in_cell(self, chunk_id):
        chunk_bounds = self.chunker.getChunkBounds(chunk_id)
        grid_size = 9 
        lon_arr = np.linspace(chunk_bounds.lonMin, chunk_bounds.lonMax, grid_size)
        lat_arr = np.linspace(chunk_bounds.latMin, chunk_bounds.latMax, grid_size)
        xx, yy = np.meshgrid(lon_arr[1::-2], lat_arr[1::-2])
        nside = healpy.order2nside(8)
        trial_healpix = healpy.ang2pix(nside, lon_arr, lat_arr, nest=True, lonlat=True)

        return list(set(trial_healpix))

    def __call__(self, chunk_id, length):
        """
        Returns
        -------
        CcdVisitId : array
            Unique IDs for each CcdVisit.
        hpix8 : array
            The healpix level 8 pixel number for the CcdVisits.
        filterName : array
            The name of the filter for each CcdVisit.
        """

        # Generate a bunch of hpix8 values for CcdVisit centers
        hpix8 = np.random.choice(self._find_hpix8_in_cell(chunk_id),
                                 self.ccd_visits_per_chunk)
        # Multiply by the number of filters
        filterName = np.random.choice(list(self.filters),
                                      self.ccd_visits_per_chunk)
        # Generate IDs for each of them
        ccdVisitId = chunk_id * 10**9 + np.arange(self.ccd_visits_per_chunk)
        return (ccdVisitId, hpix8, filterName)


class RaDecGenerator(ColumnGenerator):

    def __call__(self, cell_id, length, **kwargs):
        """
        Parameters
        ----------
        cell_id : int
            Identifier of which cell on the sky to in which
            coordinates should be generated

        length : int
            Number of coordiantes to generate

        Returns
        -------
        ra_coords : array
            Array containing the generated RA coordinates.
        dec_coords : array
            Array containing the generated Dec coordinates.
        """

        ra_limits = (360/80 * cell_id, 360/80 * (cell_id + 1))
        dec_limits = (0.0, 5.0)
        ra_delta = max(ra_limits) - min(ra_limits)
        dec_delta = max(dec_limits) - min(dec_limits)
        ra_array = np.random.random(length)*ra_delta + min(ra_limits)
        dec_array = np.random.random(length)*dec_delta + min(dec_limits)
        return (ra_array, dec_array)


class ObjIdGenerator(ColumnGenerator):

    def __call__(self, cell_id, length, **kwargs):
        """
        Returns
        -------
        object_id : array
            Array containing unique IDs for each object
        """

        return (cell_id * 100000) + np.arange(length)


class MagnitudeGenerator(ColumnGenerator):

    def __init__(self, n_mags=1, min_mag=0, max_mag=27.5):
        self.n_mags = n_mags
        self.min_mag = min_mag
        self.max_mag = max_mag

    def __call__(self, cell_id, length, **kwargs):
        mags = []
        delta_mag = self.max_mag - self.min_mag
        for n in range(self.n_mags):
            mag = np.random.rand(length)*delta_mag + self.min_mag
            mags.append(mag)
        return mags


class FilterGenerator(ColumnGenerator):

    def __init__(self, filters="ugrizy"):
        self.filters = filters

    def __call__(self, cell_id, length, **kwargs):
        return np.random.choice(list(self.filters), length)


class ForcedSourceGenerator(ColumnGenerator):

    def __init__(self, filters="ugrizy", visit_radius=0.30):
        self.filters = filters
        self.visit_radius = visit_radius

    def __call__(self, cell_id, length, prereq_row=None, prereq_tables=None):
        assert prereq_row is not None, "ForcedSourceGenerator requires rows from Object."
        assert prereq_tables is not None, "ForcedSourceGenerator requires the Visit table."

        visit_table = prereq_tables['CcdVisit']
        object_record = prereq_row

        dists_to_visit_center = np.sqrt((visit_table['ra'] - object_record['ra'])**2 +
                                        (visit_table['decl'] - object_record['decl'])**2)
        n_matching_visits = np.sum(dists_to_visit_center < self.visit_radius)

        objectId = np.zeros(n_matching_visits) + object_record['objectId']
        psFlux = np.random.randn(n_matching_visits)
        psFluxSigma = np.zeros(n_matching_visits) + 0.1
        ccdVisitId = np.zeros(n_matching_visits)

        index_position = 0
        for filter_name in self.filters:
            sel, = np.where((visit_table['filterName'] == filter_name) &
                            (dists_to_visit_center < self.visit_radius))
            matching_filter_visitIds = visit_table['CcdVisitId'][sel]

            if(len(matching_filter_visitIds) == 0):
                continue

            n_filter_visits = len(matching_filter_visitIds)
            output_indices = slice(index_position, index_position + n_filter_visits)
            ccdVisitId[output_indices] = matching_filter_visitIds
            psFlux[output_indices] += object_record['mag_{:s}'.format(filter_name)]
            index_position += n_filter_visits

        return (objectId, ccdVisitId, psFlux, psFluxSigma)
