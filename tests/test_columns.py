
import unittest
import numpy as np
import healpy

import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker


class ColumnGeneratorTests(unittest.TestCase):

    @unittest.skip("Disabled while CcdVisit under repair")
    def testCcdVisitGenerator(self):
        filters = "ugrizy"
        num_ccd_visits = 10
        cell_id = 5000
        ccdVisitGenerator = columns.CcdVisitGenerator(num_ccd_visits, filters=filters)

        results = ccdVisitGenerator(cell_id, 0)
        self.assertEqual(len(results), 3)

        ccdVisitId, hpix8, filterName = results
        self.assertEqual(len(ccdVisitId), num_ccd_visits)
        self.assertEqual(len(ccdVisitId), len(set(ccdVisitId)))

    def testCcdVisitHpix8(self):
        filters = "ugrizy"
        num_ccd_visits = 10
        cell_id = 1
        chunker = Chunker(0, 0, 0)
        ccdVisitGenerator = columns.CcdVisitGenerator(chunker, num_ccd_visits, filters=filters)
        hpix8_values = ccdVisitGenerator._find_hpix8_in_cell(cell_id)
        self.assertTrue(len(hpix8_values) > 0)

        nside = healpy.order2nside(8)
        chunks = [chunker.locate(healpy.pix2ang(nside, pixel, nest=True, lonlat=True))
                  for pixel in hpix8_values]
        hpix_centers_in_chunk = np.array(chunks) == cell_id
        # Some of the hpix centers will be outside of the chunk area, and that seems ok.
        # The test is to confirm that we get enough of them with centers inside the
        # chunk to confirm that the code is working.
        self.assertGreaterEqual(np.sum(hpix_centers_in_chunk)/float(len(hpix8_values)), 0.5)

    def testObjIdGenerator(self):
        obs_generator = columns.ObjIdGenerator()

        cell_id = 5000
        output_ids = obs_generator(cell_id, 20)
        self.assertEqual(len(output_ids), 20)

        # Test for uniqueness
        self.assertEqual(len(set(output_ids)), 20)

        output_ids2 = obs_generator(cell_id + 1, 20)
        # Test for uniqueness
        self.assertEqual(len(set(output_ids) | set(output_ids2)), 40)

    def testMagnitudeGenerator(self):

        min_mag = 21.0
        max_mag = 22.0
        mag_generator = columns.MagnitudeGenerator(min_mag=min_mag,
                                                   max_mag=max_mag,
                                                   n_mags=1)
        magnitudes = mag_generator(5000, 40)
        self.assertTrue(np.min(magnitudes) >= min_mag)
        self.assertTrue(np.max(magnitudes) <= max_mag)

    def testFilterGenerator(self):
        filters = "ugrizy"
        filter_generator = columns.FilterGenerator(filters)
        filter_values = filter_generator(5000, 40)
        self.assertTrue(set(filter_values).issubset(filters))
