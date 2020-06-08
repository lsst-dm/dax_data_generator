
import math
import numpy as np
from abc import ABC
import healpy

#from lsst.dax.data_generator import Chunker
from .chunker import Chunker


__all__ = ["ColumnGenerator", "ObjIdGenerator", "FilterGenerator",
           "RaDecGenerator", "MagnitudeGenerator", "ForcedSourceGenerator",
           "ObjIdGeneratorEF", "RaDecGeneratorEF", "MagnitudeGeneratorEF",
           "FilterGeneratorEF"]


def calcSeedFrom(chunk_id, seed, columnVal):
    """ Trying to avoid having chunks and columns having seeds 
    near each other in hopes that changing the seed value by 1
    will not look like everyting was just shifted over by 1 chunk/column.
    Each ColumnGenerator should have a unique arbitrary columnVal.
    """
    return (chunk_id*10000 + seed + columnVal*100)

def mergeBlocks(blockA, blockB):
    """ 
    Merge the corresponding lists from each tuple and return the new tuple 
    The lists should maintain the order of the individual elements. 
    """
    newList = list()
    # There's probably a more pythonic way to do this.
    for j in range(len(blockA)):
        newList.append(np.append(blockA[j], blockB[j]))
    return tuple(newList)

def equalBlocks(blockA, blockB):
    """
    Each block should be a tuple of np.array. Return True if
    both blocks contain the same arrays in the same order.
    """
    if len(blockA) != len(blockB):
        return False
    for j in range(len(blockA)):
        a = blockA[j]
        b = blockB[j]
        if not np.array_equal(a, b):
            return False
    return True

def convertBlockToRows(block):
    """ Return a list of rows (as lists) created from the columns of 'block' """
    rows = list()
    rowsInBlock = len(block[0])
    for j in range(rowsInBlock):
        row = list()
        for x in block:
            if len(x) != rowsInBlock:
                raise IndexError
            row.append(x[j])
        rows.append(row)
    #print("rows=", rows)
    return rows


def containsBlock(blockA, blockB):
    """ 
    Return True if every row in blockA has a matching row in blockB.
    The data is stored in columns and it is easier to do the 
    comparison using rows.
    """
    if len(blockA) != len(blockB):
        return False
    if len(blockA) == 0:
        return True

    # Convert blockA into a list of rows.
    try:
        rowsA = convertBlockToRows(blockA)
        rowsB = convertBlockToRows(blockB)
        for row in rowsA:
            # if the row is found in rowsB, delete it in rowsB
            found = False
            for j in range(len(rowsB)):
                if row == rowsB[j]:
                    found = True
                    print("found row ", row)
                    del rowsB[j]
                    break
            if not found:
                print("missing row ", row)
                return False

    except IndexError:
        print("Malformed block A=", blockA, " B=", blockB)
        return False
    return True

class SimpleBox:
    """  
    A simple RA DEC box where raA is always smaller than raB and decA is always smaller than decB and
    the units are degrees.
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

    def __repr__(self):
        return ('{raA=' + str(self.raA) + ' raB=' + str(self.raB) + 
               ' decA=' + str(self.decA) + ' decB=' + str(self.decB) + '}')

    def area(self):
        """ return approximate area of the box on a sphere, requires degrees """
        area = (self.raA - self.raB) * (self.decA - self.decB)
        avgDecDeg = (self.decA + self.decB) / 2
        avgDecRad = avgDecDeg * (math.pi/180.0)
        area = math.cos(avgDecRad) * area
        return area


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
        chunk_box = self.chunker.getChunkBounds(chunk_id)
        grid_size = 9
        lon_arr = np.linspace(chunk_box.getLon().getA().asDegrees(),
                              chunk_box.getLon().getB().asDegrees(),
                              grid_size)
        lat_arr = np.linspace(chunk_box.getLat().getA().asDegrees(),
                              chunk_box.getLat().getB().asDegrees(),
                              grid_size)
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
        # hpix8 = np.random.choice(self._find_hpix8_in_cell(chunk_id),
        #                         self.ccd_visits_per_chunk)
        chunk_box = self.chunker.getChunkBounds(chunk_id)
        ra_delta = (chunk_box.getLon().getB().asDegrees() -
                    chunk_box.getLon().getA().asDegrees())
        dec_delta = (chunk_box.getLat().getB().asDegrees() -
                     chunk_box.getLat().getA().asDegrees())
        ra_min = chunk_box.getLon().getA().asDegrees()
        dec_min = chunk_box.getLat().getA().asDegrees()
        ra_centers = np.random.random(length)*ra_delta + ra_min
        dec_centers = np.random.random(length)*dec_delta + dec_min
        # Multiply by the number of filters
        filterName = np.random.choice(list(self.filters),
                                      self.ccd_visits_per_chunk)
        # Generate IDs for each of them
        ccdVisitId = chunk_id * 10**9 + np.arange(self.ccd_visits_per_chunk)
        return (ccdVisitId, ra_centers, dec_centers, filterName)


class CcdVisitGeneratorEF(ColumnGenerator):

    def __init__(self, chunker, ccd_visits_per_chunk, filters="ugrizy"):
        # TODO: Is this CcdVisits or Visits?
        self.chunker = chunker
        self.filters = filters
        self.ccd_visits_per_chunk = ccd_visits_per_chunk

    def _find_hpix8_in_cell(self, chunk_id):
        chunk_box = self.chunker.getChunkBounds(chunk_id)
        grid_size = 9
        lon_arr = np.linspace(chunk_box.getLon().getA().asDegrees(),
                              chunk_box.getLon().getB().asDegrees(),
                              grid_size)
        lat_arr = np.linspace(chunk_box.getLat().getA().asDegrees(),
                              chunk_box.getLat().getB().asDegrees(),
                              grid_size)
        xx, yy = np.meshgrid(lon_arr[1::-2], lat_arr[1::-2])
        nside = healpy.order2nside(8)
        trial_healpix = healpy.ang2pix(nside, lon_arr, lat_arr, nest=True, lonlat=True)

        return list(set(trial_healpix))

    def _generateBlock(self, chunk_id, simpleBox, length):
        """ Generate 'length' number of ccd visit data entries for 'simpleBox' """
        print("&&&_generateBlock ", chunk_id, " box=", simpleBox, " length=", length)
        ra_min = simpleBox.raA
        ra_delta = simpleBox.raB - simpleBox.raA
        dec_min = simpleBox.decA
        dec_delta = simpleBox.decB - simpleBox.decA
        print("&&&_generateBlock ra_delta=", ra_delta, " ra_min=", ra_min)
        ra_centers = np.random.random(length)*ra_delta + ra_min
        dec_centers = np.random.random(length)*dec_delta + dec_min
        # Multiply by the number of filters
        filterName = np.random.choice(list(self.filters),
                                      self.ccd_visits_per_chunk)
        # Generate IDs for each of them
        ccdVisitId = chunk_id * 10**9 + np.arange(self.ccd_visits_per_chunk)
        return (ccdVisitId, ra_centers, dec_centers, filterName)

    def __call__(self, chunk_id, length, edge_width, edgeOnly):
        """
        Returns
        -------
        CcdVisitId : array
            Unique IDs for each CcdVisit.
        hpix8 : array
            The healpix level 8 pixel number for the CcdVisits.
        filterName : array
            The name of the filter for each CcdVisit.
        edge_width : width of the edge in degrees 
        """

        # sphgeom Box from Chunker::getChunkBoundingBox
        chunk_box = self.chunker.getChunkBounds(chunk_id)
        # Need to correct for RA that crosses 0.
        raA = chunk_box.getLon().getA().asDegrees()
        raB = chunk_box.getLon().getB().asDegrees()
        ra_delta = raB - raA
        if ra_delta < 0:
            raA = raA - 360.0
            ra_delta = raB -raA
        decA = chunk_box.getLat().getA().asDegrees()
        decB = chunk_box.getLat().getB().asDegrees()

        boxes = dict()
        entireBox = SimpleBox(raA, raB, decA, decB)
        boxes["entire"] = entireBox

        if edge_width > 0.0:
            # Correct the edge_width for declination so there is at least 
            # edge_width at both the top an bottom of the east and west blocks.
            edge_raA = edge_width / math.cos(decA + edge_width)
            edge_raB = edge_width / math.cos(decB - edge_width)
            edge_widthRA = max(edge_raA, edge_raB)
                    
            boxes["north"] = SimpleBox(raA, raB, decB - edge_width, decB)
            boxes["east"] = SimpleBox(raA, raA + edge_widthRA, decA + edge_width, decB - edge_width)
            boxes["west"] = SimpleBox(raB - edge_widthRA, raB, decA + edge_width, decB - edge_width)
            boxes["south"] = SimpleBox(raA, raB, decA, decA + edge_width)

        print('chunk_id=', chunk_id, ' &&&boxes->', boxes)

        # If the area of the entire box is only slightly larger than the sub-boxes, 
        # don't bother with separate edge calculation
        edgeArea = 0.0
        entireArea = 0.0
        for key, value in boxes.items():
            if key == "entire":
                entireArea = value.area()
            else:
                edgeArea += value.area()
        
        ratioEdgeToEntire = edgeArea/entireArea
        blocks = dict()
        # &&& replace 10 with minLength and 0.90 with maxRatioEdgeToEntire
        if (not edge_width > 0.0) or ratioEdgeToEntire > 0.90 or length < 10:
            # Just generate the entire block
            blocks["entire"] = self._generateBlock(chunk_id, boxes["entire"], length)
        else:
            lengths = dict()
            subLength = length
            for key, value in boxes.items():
                if key != "entire":
                    print("&&&entirearea=",entireArea," key=",key," area=", value.area(), )
                    lengths[key] = int((value.area()/entireArea) * length)
                    if lengths[key] < 1:
                        lengths[key] = 1
                    print("&&&AA sub=", subLength, " lengths=", lengths)
                    subLength -= lengths[key]
                    print("&&&BB sub=", subLength, " lengths=", lengths)
                    blocks[key] = self._generateBlock(chunk_id, boxes[key], lengths[key])
            blockNS = mergeBlocks(blocks["north"], blocks["south"])
            blockEW = mergeBlocks(blocks["east"], blocks["west"])
            blocks["entire"] = mergeBlocks(blockNS, blockEW)    
            if not edgeOnly:
                boxMiddle = SimpleBox(boxes["east"].raB, boxes["west"].raA, boxes["north"].decB, boxes["south"].decA)
                blockMiddle = self._generateBlock(chunk_id, boxMiddle, subLength)
                blocks["entire"] = mergeBlocks(blocks["entire"], blockMiddle)
        return blocks["entire"]

class RaDecGenerator(ColumnGenerator):

    def __init__(self, chunker):
        self.chunker = chunker

    def __call__(self, chunk_id, length, prereq_tables=None, **kwargs):
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

        chunk_box = self.chunker.getChunkBounds(chunk_id)
        ra_delta = (chunk_box.getLon().getB().asDegrees() -
                    chunk_box.getLon().getA().asDegrees())
        dec_delta = (chunk_box.getLat().getB().asDegrees() -
                     chunk_box.getLat().getA().asDegrees())
        ra_min = chunk_box.getLon().getA().asDegrees()
        dec_min = chunk_box.getLat().getA().asDegrees()
        ra_centers = np.random.random(length)*ra_delta + ra_min
        dec_centers = np.random.random(length)*dec_delta + dec_min

        return (ra_centers, dec_centers)

class RaDecGeneratorEF(ColumnGenerator):

    def __init__(self, chunker, ignoreEdgeOnly=False):
        self.chunker = chunker
        self.ignoreEdgeOnly = ignoreEdgeOnly
        self.columnVal = 1
        # avoid having the same ra and dec in different tables.
        if self.ignoreEdgeOnly:
            self.columnVal = 2


    def _generateBlock(self, chunk_id, simpleBox, length):
        """ Generate 'length' number of ccd visit data entries for 'simpleBox' """
        print("&&&_generateBlock ", chunk_id, " box=", simpleBox, " length=", length)
        ra_min = simpleBox.raA
        ra_delta = simpleBox.raB - simpleBox.raA
        dec_min = simpleBox.decA
        dec_delta = simpleBox.decB - simpleBox.decA
        print("&&&_generateBlock ra_delta=", ra_delta, " ra_min=", ra_min)
        ra_centers = np.random.random(length)*ra_delta + ra_min
        dec_centers = np.random.random(length)*dec_delta + dec_min
        return (ra_centers, dec_centers)

    def __call__(self, chunk_id, length, seed, edge_width, edgeOnly, prereq_tables=None, **kwargs):
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
        np.random.seed(calcSeedFrom(chunk_id, seed, self.columnVal))

        # Some tables, such as ccdVisit, cannot be generated edgeOnly.
        if self.ignoreEdgeOnly:
            edgeOnly = False

        # sphgeom Box from Chunker::getChunkBoundingBox
        chunk_box = self.chunker.getChunkBounds(chunk_id)
        print("&&&!!chunk_id=", chunk_id, " chunk_box=", chunk_box)
        # Need to correct for RA that crosses 0.
        raA = chunk_box.getLon().getA().asDegrees()
        raB = chunk_box.getLon().getB().asDegrees()
        ra_delta = raB - raA
        if ra_delta < 0:
            raA = raA - 360.0
            ra_delta = raB -raA
        decA = chunk_box.getLat().getA().asDegrees()
        decB = chunk_box.getLat().getB().asDegrees()

        boxes = dict()
        entireBox = SimpleBox(raA, raB, decA, decB)
        boxes["entire"] = entireBox

        if edge_width > 0.0:
            # Correct the edge_width for declination so there is at least 
            # edge_width at both the top an bottom of the east and west blocks.
            edge_raA = edge_width / math.cos(decA + edge_width)
            edge_raB = edge_width / math.cos(decB - edge_width)
            edge_widthRA = max(edge_raA, edge_raB)
                    
            boxes["north"] = SimpleBox(raA, raB, decB - edge_width, decB)
            boxes["east"] = SimpleBox(raA, raA + edge_widthRA, decA + edge_width, decB - edge_width)
            boxes["west"] = SimpleBox(raB - edge_widthRA, raB, decA + edge_width, decB - edge_width)
            boxes["south"] = SimpleBox(raA, raB, decA, decA + edge_width)

        print('chunk_id=', chunk_id, ' &&&boxes->', boxes)

        # If the area of the entire box is only slightly larger than the sub-boxes, 
        # don't bother with separate edge calculation
        edgeArea = 0.0
        entireArea = 0.0
        for key, value in boxes.items():
            if key == "entire":
                entireArea = value.area()
            else:
                edgeArea += value.area()
        
        ratioEdgeToEntire = edgeArea/entireArea
        blocks = dict()
        # &&& replace 10 with minLength and 0.90 with maxRatioEdgeToEntire
        if (not edge_width > 0.0) or ratioEdgeToEntire > 0.90 or length < 10:
            # Just generate the entire block
            blocks["entire"] = self._generateBlock(chunk_id, boxes["entire"], length)
        else:
            lengths = dict()
            subLength = length
            for key, value in boxes.items():
                if key != "entire":
                    print("&&&entirearea=",entireArea," key=",key," area=", value.area(), )
                    lengths[key] = int((value.area()/entireArea) * length)
                    if lengths[key] < 1:
                        lengths[key] = 1
                    print("&&&AA sub=", subLength, " lengths=", lengths)
                    subLength -= lengths[key]
                    print("&&&BB sub=", subLength, " lengths=", lengths)
                    blocks[key] = self._generateBlock(chunk_id, boxes[key], lengths[key])
            blockNS = mergeBlocks(blocks["north"], blocks["south"])
            blockEW = mergeBlocks(blocks["east"], blocks["west"])
            blocks["entire"] = mergeBlocks(blockNS, blockEW)    
            if not edgeOnly:
                boxMiddle = SimpleBox(boxes["east"].raB, boxes["west"].raA, boxes["north"].decB, boxes["south"].decA)
                blockMiddle = self._generateBlock(chunk_id, boxMiddle, subLength)
                blocks["entire"] = mergeBlocks(blocks["entire"], blockMiddle)
        # blocks["entire"] = (ra_centers, dec_centers)
        return blocks["entire"]



class ObjIdGenerator(ColumnGenerator):

    def __call__(self, cell_id, length, **kwargs):
        """
        Returns
        -------
        object_id : array
            Array containing unique IDs for each object
        """

        #return (cell_id * 100000) + np.arange(length)
        vals = (cell_id * 100000) + np.arange(length)
        print("&&&objectId[0]=", vals[0])
        print("&&& type(vals)=", type(vals), "type(vals[0])=", type(vals[0]), "type(vals[100])=", type(vals[100]))
        return vals


class ObjIdGeneratorEF(ColumnGenerator):

    def __call__(self, chunk_id, length, seed, **kwargs):
        """
        Returns
        -------
        object_id : array
            Array containing unique IDs for each object
        """

        return (chunk_id * 100000) + np.arange(length) # &&& more than 100k objects in a chunk will cause issues
        

class VisitIdGenerator(ColumnGenerator):

    def __call__(self, cell_id, length, **kwargs):
        """
        Returns
        -------
        visit_id : array
            Array containing unique IDs for each visit
        """

        return 10000000000 + (cell_id * 100000) + np.arange(length)


class VisitIdGeneratorEF(ColumnGenerator):

    def __call__(self, chunk_id, length, seed, **kwargs):
        """
        Returns
        -------
        visit_id : array
            Array containing unique IDs for each visit
        """

        return 10000000000 + (chunk_id * 100000) + np.arange(length)


class MagnitudeGenerator(ColumnGenerator):
    """
    Currently generates a flat magnitude distribution. Should properly
    be some power law.
    """

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


class MagnitudeGeneratorEF(ColumnGenerator):
    """
    Currently generates a flat magnitude distribution. Should properly
    be some power law.
    If there is more than one call to this in a single table, there will
    be correlation between rows as the same random numbers will be used.
    """

    def __init__(self, n_mags=1, min_mag=0, max_mag=27.5):
        self.n_mags = n_mags
        self.min_mag = min_mag
        self.max_mag = max_mag
        self.columnVal = 7  # arbitrary, but different from other columns

    def __call__(self, chunk_id, length, seed, **kwargs):

        np.random.seed(calcSeedFrom(chunk_id, seed, self.columnVal))

        # &&& must make row by row not column by column.
        #mags = []
        #delta_mag = self.max_mag - self.min_mag
        #for n in range(self.n_mags):
        #    mag = np.random.rand(length)*delta_mag + self.min_mag
        #    mags.append(mag)
        #return mags
        magRows = list()
        delta_mag = self.max_mag - self.min_mag
        for j in range(length):
            mag = np.random.rand(self.n_mags)*delta_mag + self.min_mag
            magRows.append(mag)
        magCols = convertBlockToRows(magRows)
        print("&&&magRows=", magRows)
        print("&&&magCols=", magCols)
        return magCols


class FilterGenerator(ColumnGenerator):

    def __init__(self, filters="ugrizy"):
        self.filters = filters

    def __call__(self, cell_id, length, **kwargs):
        return np.random.choice(list(self.filters), length)


class FilterGeneratorEF(ColumnGenerator):

    def __init__(self, filters="ugrizy"):
        self.filters = filters
        self.columnVal = 6

    def __call__(self, chunk_id, length, seed, **kwargs):
        np.random.seed(calcSeedFrom(chunk_id, seed, self.columnVal))
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

        objectId = np.zeros(n_matching_visits, dtype=int) + int(object_record['objectId'])
        psFlux = np.random.randn(n_matching_visits)
        psFluxSigma = np.zeros(n_matching_visits) + 0.1
        ccdVisitId = np.zeros(n_matching_visits, dtype=int)

        index_position = 0
        for filter_name in self.filters:
            sel, = np.where((visit_table['filterName'] == filter_name) &
                            (dists_to_visit_center < self.visit_radius))
            matching_filter_visitIds = visit_table['ccdVisitId'][sel]

            if(len(matching_filter_visitIds) == 0):
                continue

            n_filter_visits = len(matching_filter_visitIds)
            output_indices = slice(index_position, index_position + n_filter_visits)
            ccdVisitId[output_indices] = matching_filter_visitIds
            psFlux[output_indices] += object_record['mag_{:s}'.format(filter_name)]
            index_position += n_filter_visits

        return (objectId, ccdVisitId, psFlux, psFluxSigma)


class ForcedSourceGeneratorEF(ColumnGenerator):

    def __init__(self, filters="ugrizy", visit_radius=0.30):
        self.filters = filters
        self.visit_radius = visit_radius
        self.columnVal = 3

    def __call__(self, chunk_id, length, seed, prereq_row=None, prereq_tables=None):
        assert prereq_row is not None, "ForcedSourceGenerator requires rows from Object."
        assert prereq_tables is not None, "ForcedSourceGenerator requires the Visit table."

        np.random.seed(calcSeedFrom(chunk_id, seed, self.columnVal))

        visit_table = prereq_tables['CcdVisit']
        object_record = prereq_row

        dists_to_visit_center = np.sqrt((visit_table['ra'] - object_record['ra'])**2 +
                                        (visit_table['decl'] - object_record['decl'])**2)
        n_matching_visits = np.sum(dists_to_visit_center < self.visit_radius)

        objectId = np.zeros(n_matching_visits, dtype=int) + int(object_record['objectId'])
        psFlux = np.random.randn(n_matching_visits)
        psFluxSigma = np.zeros(n_matching_visits) + 0.1
        ccdVisitId = np.zeros(n_matching_visits, dtype=int)

        index_position = 0
        for filter_name in self.filters:
            sel, = np.where((visit_table['filterName'] == filter_name) &
                            (dists_to_visit_center < self.visit_radius))
            matching_filter_visitIds = visit_table['ccdVisitId'][sel]

            if(len(matching_filter_visitIds) == 0):
                continue

            n_filter_visits = len(matching_filter_visitIds)
            output_indices = slice(index_position, index_position + n_filter_visits)
            ccdVisitId[output_indices] = matching_filter_visitIds
            psFlux[output_indices] += object_record['mag_{:s}'.format(filter_name)]
            index_position += n_filter_visits

        return (objectId, ccdVisitId, psFlux, psFluxSigma)


def tst_convertBlockToRows(logMsgs=True):
    vals1 = np.array([101, 102, 103])
    vals2 = np.array([309, 308, 307])
    vals3 = np.array([951, 952, 953])
    blockA = (vals1, vals2, vals3)
    rowsExpected = list()
    rowsExpected.append(list([101, 309, 951]))
    rowsExpected.append(list([102, 308, 952]))
    rowsExpected.append(list([103, 307, 953]))
    rowsA = convertBlockToRows(blockA)
    if logMsgs:
        print("rowsA=", rowsA)
        print("expected=", rowsExpected)
    if rowsA != rowsExpected:
        print("FAILED rows did not match a=", rowsA, " expected=", rowsExpected)
        return False
    return True

def tst_mergeBlocks(logMsgs=True):
    vals1 = np.array([101, 102, 103, 104, 105])
    vals2 = np.array([309, 308, 307, 306, 305])
    vals3 = np.array([951, 952, 953, 954, 955])
    blockA = (vals1, vals2, vals3)
    vals4 = np.array([201, 202, 203, 204, 205, 207])
    vals5 = np.array([609, 608, 607, 606, 605, 604])
    vals6 = np.array([751, 752, 753, 754, 755, 756])
    blockB = (vals4, vals5, vals6)
    blockC = mergeBlocks(blockA, blockB)
    if logMsgs:
        print("blockA=",blockA)
        print("blockB=",blockB)
        print("blockC=",blockC)
    expected1 = np.array([101, 102, 103, 104, 105, 201, 202, 203, 204, 205, 207])
    expected2 = np.array([309, 308, 307, 306, 305, 609, 608, 607, 606, 605, 604])
    expected3 = np.array([951, 952, 953, 954, 955, 751, 752, 753, 754, 755, 756])
    blockExpected = (expected1, expected2, expected3)
    success = None
    if not equalBlocks(blockC, blockExpected):
        print("FAILED blocks do not match expected=", blockExpected, " C=", blockC)
        success = False
    bad3 = np.array([951, 952, 953, 954, 955, 751, 752, 753, 754, 375, 756])
    blockBad = (expected1, expected2, bad3)
    if equalBlocks(blockC, blockBad):
        print("FAILED to detect differences in blocks bad=", blockBad, " C=", blockC)
        success = False
    if not containsBlock(blockA, blockC):
        print("FAILED blockA should have been found in blockC A=", blockA, " C=", blockC)
        success = False
    if containsBlock(blockBad, blockExpected):
        print("FAILED blockBad should not have been found in blockExpected bad=", blockBad, 
              "ex=", blockExpected)
        success = False
    # check for duplicate row
    blockBad2 = (np.array([101, 101]), np.array([309, 309]), np.array([951, 951]))
    if containsBlock(blockBad2, blockC):
        print("FAILED blockBad2 should not have been found in blockC bad=", blockBad, 
              "C=", blockC)
        success = False
    
    if success is None:
        success = True
    return success

def tst_CcdVisitGeneratorEF(logMsgs=True):
    """ Hmm, it looks like CcdVisitGenerator is not being used and
    it doesn't produce a valid table as the column lengths are not equal.
    """
    success = None
    # setup chunking information
    num_stripes = 200
    num_substripes = 5
    localChunker = Chunker(0, num_stripes, num_substripes)

    np.random.seed(100)
    chunk_id = 50
    length = 10000
    # a bit more than an arcminute for edge_width.
    edge_width = 0.018
    visitsPerChunk = 25
    colGen = CcdVisitGeneratorEF(localChunker, visitsPerChunk)
    completeBlock = colGen(chunk_id, length, edge_width, edgeOnly=False)
    print("completeBlock=", completeBlock)
    # Check the lengths of all arrays in the block are the same
    # 
    sz = len(completeBlock[0])
    for arr in completeBlock:
        if logMsgs: print("&&& sz=", sz, " len(arr)=", len(arr))
        if sz != len(arr):
            print("FAILED array length mismatch", sz, " ", len(arr), arr)
            success = False
    
    if success is None:
        success = True
    return success

def tst_RaDecGeneratorEF(logMsgs=True, everyNth=75):
    success = None
    # setup chunking information
    num_stripes = 200
    num_substripes = 5
    localChunker = Chunker(0, num_stripes, num_substripes)

    allChunks = localChunker.getAllChunks()
    if logMsgs: print("&&& allChunks=", len(allChunks))
    manyChunks = allChunks[0::everyNth]
    if manyChunks[-1] != allChunks[-1]:
        manyChunks.append(allChunks[-1])
    length = 10000
    # a bit more than an arcminute for edge_width.
    edge_width = 0.018
    colGen = RaDecGeneratorEF(localChunker)
    seed = 1
    blocksA = dict()
    j = 0
    for chunk_id in manyChunks:
        blocksA[chunk_id] = colGen(chunk_id, length, seed, edge_width, edgeOnly=False)
        if logMsgs: print("blocksA[", chunk_id, "]=", blocksA[chunk_id])
    
    blocksB = dict()
    j = 0
    for chunk_id in manyChunks:
        blocksB[chunk_id] = colGen(chunk_id, length, seed, edge_width, edgeOnly=False)
        if logMsgs: print("blocksB[", chunk_id, "]=", blocksB[chunk_id])

    blocksC = dict()
    j = 0
    for chunk_id in manyChunks:
        blocksC[chunk_id] = colGen(chunk_id, length, seed, edge_width, edgeOnly=True)
        if logMsgs: print("blocksC[", chunk_id, "]=", blocksC[chunk_id])

    blocksD = dict()
    j = 0
    for chunk_id in manyChunks:
        blocksD[chunk_id] = colGen(chunk_id, length, seed, edge_width, edgeOnly=True)
        if logMsgs: print("blocksD[", chunk_id, "]=", blocksD[chunk_id])

    for chunk_id in manyChunks:
        blockA = blocksA[chunk_id]
        blockB = blocksB[chunk_id]
        if not equalBlocks(blockA, blockB):
            print("FAILED blocks not equal chunk_id=", chunk_id)
            success = False
        blockC = blocksC[chunk_id]
        blockD = blocksD[chunk_id]
        if not equalBlocks(blockC, blockD):
            print("FAILED edgeOnly blocks not equal chunk_id=", chunk_id)
            success = False
        if not containsBlock(blockC, blockA):
            print("FAILED edgeOnly blocks not found in larger block chunk_id=", chunk_id) 

    if success is None:
        success = True
    return success

