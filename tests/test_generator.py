
import numpy as np
import unittest
from lsst.dax.data_generator import DataGenerator
import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker

num_stripes = 200
num_substripes = 5
chunker = Chunker(0, num_stripes, num_substripes)


class TestDataGenerator(unittest.TestCase):

    def testObjectTable(self):
        generator_spec = {
            "Object": {
                "columns": {"objectId": columns.ObjIdGenerator(),
                            "ra,decl": columns.RaDecGenerator(chunker),
                            "mag_u,mag_g,mag_r": columns.MagnitudeGenerator(n_mags=3)
                            }
            }
        }
        chunk_id = 5000

        generator = DataGenerator(generator_spec)

        #&&& data = generator.make_chunk(cell_id, num_rows=50)
        seed = 1
        edge_width = 0.018 # degrees
        data = generator.make_chunk(chunk_id, num_rows=50, seed=seed,
                                  edgeWidth=edge_width, edgeOnly=False)
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

    def testCcdVisit(self):
        generator_spec = {
            "CcdVisit": {
                "columns": {"ccdVisitId": columns.VisitIdGenerator(),
                            "filterName": columns.FilterGenerator(filters="ugr"),
                            "ra,decl": columns.RaDecGenerator(chunker)
                            }
            },
        }
        chunk_id = 5000
        seed = 1
        edge_width = 0.018 # degrees
        generator = DataGenerator(generator_spec)
        #&&& chunk_table = generator.make_chunk(chunk_id, 50)
        chunk_table = generator.make_chunk(chunk_id, num_rows=50, seed=seed,
                                           edgeWidth=edge_width, edgeOnly=False)
        self.assertIn("CcdVisit", chunk_table.keys())
        self.assertEqual(len(chunk_table["CcdVisit"]), 50)

    def testForcedSource(self):

        generator_spec = {
            "Object": {
                "columns": {"objectId": columns.ObjIdGenerator(),
                            "ra,decl": columns.RaDecGenerator(chunker),
                            "mag_u,mag_g,mag_r": columns.MagnitudeGenerator(n_mags=3)
                            }
            },
            "CcdVisit": {
                "columns": {"ccdVisitId": columns.VisitIdGenerator(),
                            "filterName": columns.FilterGenerator(filters="ugr"),
                            "ra,decl": columns.RaDecGenerator(chunker)
                            }
            },
            "ForcedSource": {
                "prereq_row": "Object",
                "prereq_tables": ["CcdVisit"],
                "columns": {
                    "objectId,ccdVisitId,psFlux,psFlux_Sigma":
                        columns.ForcedSourceGenerator(visit_radius=3.0, filters="ugr"),
                },
            }
        }

        chunk_id = 3525
        generator = DataGenerator(generator_spec)
        row_counts = {"CcdVisit": 80,
                      "Object": 100,
                      "ForcedSource": 0}
        seed = 1
        edge_width = 0.018 # degrees
        chunk_tables = generator.make_chunk(chunk_id, num_rows=50, seed=seed,
                                            edgeWidth=edge_width, edgeOnly=False)
        self.assertIn("ForcedSource", chunk_tables.keys())
        self.assertIn("Object", chunk_tables.keys())
        self.assertIn("CcdVisit", chunk_tables.keys())

        print(len(chunk_tables['ForcedSource']))
        print(len(chunk_tables['CcdVisit']))
        print(len(chunk_tables['Object']))
        self.assertEqual(set(chunk_tables['ForcedSource']['ccdVisitId']),
                         set(chunk_tables['CcdVisit']['ccdVisitId']))

