
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

    def testResolveTableOrder(self):
        generator_spec = {
            "ForcedSource": {
                "prereq_row": "Object",
                "prereq_tables": ["CcdVisit"],
                "columns": {}
            },
            "CcdVisit": {
                "columns": {}
            }
        }
        table_order = DataGenerator._resolve_table_order(generator_spec)
        self.assertTrue(table_order.index("CcdVisit") < table_order.index("ForcedSource"))

    def testForcedSource(self):

        generator_spec = {
            "Object": {
                "columns": {"objectId": columns.ObjIdGenerator(),
                            "ra,dec": columns.RaDecGenerator(),
                            "mag_u,mag_g,mag_r": columns.MagnitudeGenerator(n_mags=3)
                            }
            },
            "CcdVisit": {
                "columns": {"ccdVisitId": columns.ObjIdGenerator(),
                            "filterName": columns.FilterGenerator(filters="ugr"),
                            }
            },
            "ForcedSource": {
                "prereq_row": "Object",
                "prereq_tables": ["CcdVisit"],
                "columns": {
                    "objectId,ccdVisitId,psFlux,psFlux_Sigma": columns.ForcedSourceGenerator(),
                },
            }
        }

        chunk_id = 5000
        num_filters = 3
        generator = DataGenerator(generator_spec)
        chunk_tables = generator.make_chunk(chunk_id, num_rows=40)
        self.assertIn("ForcedSource", chunk_tables.keys())
        self.assertIn("Object", chunk_tables.keys())
        self.assertIn("CcdVisit", chunk_tables.keys())

        self.assertEqual(len(chunk_tables['ForcedSource']), len(chunk_tables['CcdVisit']*num_filters))
        self.assertEqual(set(chunk_tables['ForcedSource']['ccdVisitId']),
                         set(chunk_tables['CcdVisit']['ccdVisitId']))

