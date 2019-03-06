
import numpy as np
from abc import ABC

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

        ra_limits = ( 360/80 * cell_id, 360/80 * (cell_id + 1))
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

    filters = "ugrizy"
    
    def __call__(self, cell_id, length, prereq_row=None, prereq_tables=None):
        assert prereq_row is not None, "ForcedSourceGenerator requires rows from Object."
        assert prereq_tables is not None, "ForcedSourceGenerator requires the Visit table."
        
        visit_length = len(prereq_tables['CcdVisit'])
        
        objectId = np.zeros(visit_length) + prereq_row['objectId']
        ccdVisitId = prereq_tables['CcdVisit']['ccdVisitId']
        
        psFlux = np.random.randn(visit_length)
        for filter_name in self.filters:
            sel, = np.where(prereq_tables['CcdVisit']['filterName'] == filter_name)
            if(len(sel) > 0):
                psFlux[sel] += prereq_row['mag_{:s}'.format(filter_name)]

        psFluxSigma = np.zeros(visit_length) + 0.1
        
        return (objectId, ccdVisitId, psFlux, psFluxSigma)
