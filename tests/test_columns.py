
import unittest
import numpy as np
import healpy
import pandas as pd
from astropy.coordinates import SkyCoord

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
        box = columns.SimpleBox(0.5, 0.5, 2.5, 2.5)

        cell_id = 5000
        seed = 1
        output_ids = obs_generator(box, 20, seed, unique_box_id=0)
        self.assertEqual(len(output_ids), 20)

        # Test for uniqueness
        self.assertEqual(len(set(output_ids)), 20)

        output_ids2 = obs_generator(box, 20, seed, unique_box_id=1)
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


    @unittest.skip("FS is out of date")
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

        object_df = pd.DataFrame({'objectId': object_ids,
                                  'ra': [x[0] for x in object_ra_decs],
                                  'decl': [x[1] for x in object_ra_decs],
                                  'mag_g': [20]*len(object_ids)
                                  })

        prereq_tables = {'CcdVisit': visit_df, 'Object': object_df}

        cell_id = 1
        length = 0
        seed = 1
        fs_generator = columns.ForcedSourceGenerator(visit_radius=1.00)

        # (array([1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8]), array([101, 102, 101, 102, 101,
        # 102, 101, 102, 101, 102, 101, 102, 101, 102, 101, 102]), a

        # Expected result in the form of a list for each object,
        # containing (objectId, visitId) tuples.
        expected = [[(1, 101), (1, 102)],
                    [(2, 101), (2, 102)],
                    [(3, 101), (3, 102)],
                    [(4, 101), (4, 102)]]
                    # [(5, 103), (5, 104)],
                    # [(6, 103), (6, 104)],
                    # [(7, 103), (7, 104)],
                    # [(8, 103), (8, 104)]]

        box = columns.SimpleBox(0.5, 0.5, 2.5, 2.5)
        chunk_center = SkyCoord(1.5, 1.5, frame="icrs", unit="deg")

        for object_row_id, expected_res in enumerate(expected):
            fs_output = fs_generator(box, length, seed,
                                     prereq_tables=prereq_tables,
                                     chunk_center=chunk_center)

            output_obj_ids, output_ccdvisits, _, _ = fs_output
            self.assertEqual(len(output_obj_ids), len(expected_res))
            for res_row_obj, res_row_visit in expected_res:
                self.assertIn(res_row_obj, output_obj_ids)
                self.assertIn(res_row_visit, output_ccdvisits)

    def testConvertBlockToRows(self):
        self.assertTrue(testing.tst_convertBlockToRows(False))

    def testMergeBlocks(self):
        self.assertTrue(testing.tst_mergeBlocks(False))
