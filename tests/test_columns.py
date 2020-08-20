
import unittest
import numpy as np
import healpy
import pandas as pd

import lsst.dax.data_generator.columns as columns
import lsst.dax.data_generator.testing as testing
from lsst.dax.data_generator import Chunker

num_stripes = 50
num_substripes = 5


class ColumnGeneratorTests(unittest.TestCase):

    @unittest.skip("hpix8 is deprecated")
    def testCcdVisitHpix8(self):
        filters = "ugrizy"
        num_ccd_visits = 10
        cell_id = 2500
        chunker = Chunker(0, num_stripes, num_substripes)
        ccdVisitGenerator = columns.CcdVisitGenerator(chunker, num_ccd_visits, filters=filters)
        hpix8_values = ccdVisitGenerator._find_hpix8_in_cell(cell_id)
        print(hpix8_values)
        self.assertTrue(len(hpix8_values) > 0)

        nside = healpy.order2nside(8)
        chunks = [chunker.locate(healpy.pix2ang(nside, pixel, nest=True, lonlat=True))
                  for pixel in hpix8_values]
        hpix_centers_in_chunk = np.array(chunks) == cell_id
        # Some of the hpix centers will be outside of the chunk area, and that seems ok.
        # The test is to confirm that we get enough of them with centers inside the
        # chunk to confirm that the code is working.
        print(chunks)
        self.assertGreaterEqual(np.sum(hpix_centers_in_chunk)/float(len(hpix8_values)), 0.5)

    def testObjIdGenerator(self):
        obs_generator = columns.ObjIdGenerator()

        cell_id = 5000
        seed = 1
        output_ids = obs_generator(cell_id, 20, seed)
        self.assertEqual(len(output_ids), 20)

        # Test for uniqueness
        self.assertEqual(len(set(output_ids)), 20)

        output_ids2 = obs_generator(cell_id + 1, 20, seed)
        # Test for uniqueness
        self.assertEqual(len(set(output_ids) | set(output_ids2)), 40)

    def testMagnitudeGenerator(self):

        min_mag = 21.0
        max_mag = 22.0
        mag_generator = columns.MagnitudeGenerator(min_mag=min_mag,
                                                   max_mag=max_mag,
                                                   n_mags=1)
        magnitudes = mag_generator(5000, 40, seed=1)
        self.assertTrue(np.min(magnitudes) >= min_mag)
        self.assertTrue(np.max(magnitudes) <= max_mag)

    def testFilterGenerator(self):
        filters = "ugrizy"
        filter_generator = columns.FilterGenerator(filters)
        filter_values = filter_generator(5000, 40, seed=1)
        self.assertTrue(set(filter_values).issubset(filters))

    def testFSGenerator(self):
        object_ids = np.array([1, 2, 3,
                              4,
                              5, 6, 7, 8])
        object_ra_decs = [(1.1, 1.1), (1.1, 1.2), (1.1, 1.3),
                          (2.0, 2.1),
                          (3.0, 3.1), (3.1, 3.0), (3.05, 3.05), (3.1, 3.1)]

        visit_ids = np.array([101, 102, 103, 104])
        visit_ra_decs = [(1.0, 1.0), (2.0, 2.0), (3.0, 3.0), (3.02, 3.02)]
        visit_df = pd.DataFrame({'ccdVisitId': visit_ids,
                                 'ra': [x[0] for x in visit_ra_decs],
                                 'decl': [x[1] for x in visit_ra_decs],
                                 'filterName': ['g']*len(visit_ids)
                                 })
        prereq_tables = {'CcdVisit': visit_df}

        object_df = pd.DataFrame({'objectId': object_ids,
                                  'ra': [x[0] for x in object_ra_decs],
                                  'decl': [x[1] for x in object_ra_decs],
                                  'mag_g': [20]*len(object_ids)
                                  })

        cell_id = 1
        length = 0
        fs_generator = columns.ForcedSourceGenerator(visit_radius=0.30)

        # Expected result in the form of a list for each object,
        # containing (objectId, visitId) tuples.
        expected = [[(1, 101)],
                    [(2, 101)],
                    [],
                    [(4, 102)],
                    [(5, 103), (5, 104)],
                    [(6, 103), (6, 104)],
                    [(7, 103), (7, 104)],
                    [(8, 103), (8, 104)]]

        for object_row_id, expected_res in enumerate(expected):
            fs_output = fs_generator(cell_id, length, seed=1,
                                     prereq_row=object_df.iloc[object_row_id],
                                     prereq_tables=prereq_tables)

            output_obj_ids, output_ccdvisits, _, _ = fs_output
            self.assertEqual(len(output_obj_ids), len(expected_res))
            for res_row_obj, res_row_visit in expected_res:
                self.assertIn(res_row_obj, output_obj_ids)
                self.assertIn(res_row_visit, output_ccdvisits)

    def testRaDecGenerator(self):
        self.assertTrue(testing.tst_convertBlockToRows(False))
        self.assertTrue(testing.tst_mergeBlocks(False))
        self.assertTrue(testing.tst_RaDecGenerator(False, 1000))
