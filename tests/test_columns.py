
import unittest
import numpy as np

import lsst.dax.data_generator as dataGen


class ColumnGeneratorTests(unittest.TestCase):

    def testObjIdGenerator(self):
        obs_generator = dataGen.columns.ObjIdGenerator()

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
        mag_generator = dataGen.columns.MagnitudeGenerator(min_mag=min_mag,
                                                           max_mag=max_mag,
                                                           n_mags=1)
        magnitudes = mag_generator(5000, 40)
        self.assertTrue(np.min(magnitudes) >= min_mag)
        self.assertTrue(np.max(magnitudes) <= max_mag)

    def testFilterGenerator(self):
        filters = "ugrizy"
        filter_generator = dataGen.columns.FilterGenerator(filters)
        filter_values = filter_generator(5000, 40)
        self.assertTrue(set(filter_values).issubset(filters))
