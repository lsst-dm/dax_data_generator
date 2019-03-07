
import unittest
import numpy as np

import lsst.dax.data_generator.columns as columns


class ColumnGeneratorTests(unittest.TestCase):

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
