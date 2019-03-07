
import unittest
from lsst.dax.data_generator import DataGenerator
import lsst.dax.data_generator.columns as columns


class TestDataGenerator(unittest.TestCase):

    def testObjectTable(self):
        generator_spec = {
            "Object": {
                "columns": {"objectId": columns.ObjIdGenerator(),
                            "ra,dec": columns.RaDecGenerator(),
                            "mag_u,mag_g,mag_r": columns.MagnitudeGenerator(n_mags=3)
                            }
            }
        }
        cell_id = 5000

        generator = DataGenerator(generator_spec)

        data = generator.make_chunk(cell_id, num_rows=50)
        self.assertIn('Object', data.keys())
        self.assertEqual(len(data['Object']), 50)
