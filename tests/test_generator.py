
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
        generator = DataGenerator(generator_spec)
        chunk_table = generator.make_chunk(chunk_id, 50)
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
        chunk_tables = generator.make_chunk(chunk_id, row_counts)
        self.assertIn("ForcedSource", chunk_tables.keys())
        self.assertIn("Object", chunk_tables.keys())
        self.assertIn("CcdVisit", chunk_tables.keys())

        print(len(chunk_tables['ForcedSource']))
        print(len(chunk_tables['CcdVisit']))
        print(len(chunk_tables['Object']))
        # print("Object: ", min(chunk_tables['Object']['decl']), max(chunk_tables['Object']['decl']))
        # print("Visit: ", min(chunk_tables['CcdVisit']['decl']), max(chunk_tables['CcdVisit']['decl']))
        # print(chunk_tables['CcdVisit']['ra'][:20], chunk_tables['CcdVisit']['decl'][:20])
        # print(chunk_tables['Object']['ra'][:20], chunk_tables['Object']['decl'][:20])
        delta_set = (set(chunk_tables['CcdVisit']['ccdVisitId']) -
                     set(chunk_tables['ForcedSource']['ccdVisitId']))
        test_id = delta_set.pop()
        target_visit = chunk_tables['CcdVisit'][chunk_tables['CcdVisit']['ccdVisitId'] == test_id]
        print(target_visit)
        center_ra = target_visit.iloc[0]['ra']
        center_dec = target_visit.iloc[0]['decl']
        dists = np.sqrt((chunk_tables['Object']['ra'] - center_ra)**2 +
                        (chunk_tables['Object']['decl'] - center_dec))
        #  print(dists)
        # print(chunk_tables['Object']['ra'] - float(center_ra))
        print(center_ra)
        self.assertEqual(set(chunk_tables['ForcedSource']['ccdVisitId']),
                         set(chunk_tables['CcdVisit']['ccdVisitId']))

