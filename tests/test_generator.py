
import numpy as np
import unittest
from lsst.dax.data_generator import DataGenerator
import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker, UniformSpatialModel

num_stripes = 200
num_substripes = 5
chunker = Chunker(0, num_stripes, num_substripes)

# From lsst.sphgeom
RAD_PER_DEG = 0.0174532925199432957692369076849

class TestDataGenerator(unittest.TestCase):

    def testObjectTable(self):
        generator_spec = {
            "Object": {
                "columns": {"objectId": columns.ObjIdGenerator(),
                            "ra,decl": columns.RaDecGenerator(),
                            "mag_u,mag_g,mag_r": columns.MagnitudeGenerator(n_mags=3)
                            },
                "density": UniformSpatialModel(50),
            }
        }
        chunk_id = 3525
        seed = 1

        generator = DataGenerator(generator_spec, chunker, seed=seed)

        edge_width = 0.017 # degrees
        data = generator.make_chunk(chunk_id, edge_width=edge_width, edge_only=False)
        chunk_area = chunker.getChunkBounds(chunk_id).getArea() / RAD_PER_DEG**2
        self.assertIn('Object', data.keys())
        self.assertAlmostEqual(len(data["Object"]), int(50 * chunk_area), delta=1)

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
                            "ra,decl": columns.RaDecGenerator()
                            },
                "density": UniformSpatialModel(50),
            },
        }
        chunk_id = 5000
        seed = 1
        edge_width = 0.017 # degrees
        generator = DataGenerator(generator_spec, chunker, seed=seed)
        chunk_area = chunker.getChunkBounds(chunk_id).getArea() / RAD_PER_DEG**2
        chunk_table = generator.make_chunk(chunk_id, edge_width=edge_width, edge_only=False)
        self.assertIn("CcdVisit", chunk_table.keys())
        self.assertAlmostEqual(len(chunk_table["CcdVisit"]), int(50 * chunk_area), delta=1)

    def testForcedSource(self):

        generator_spec = {
            "Object": {
                "columns": {"objectId": columns.ObjIdGenerator(),
                            "ra,decl": columns.RaDecGenerator(),
                            "mag_u,mag_g,mag_r": columns.MagnitudeGenerator(n_mags=3)
                            },
                "density": UniformSpatialModel(100),
            },
            "CcdVisit": {
                "columns": {"ccdVisitId": columns.VisitIdGenerator(),
                            "filterName": columns.FilterGenerator(filters="ugr"),
                            "ra,decl": columns.RaDecGenerator()
                            },
                "density": UniformSpatialModel(80),
            },
            "ForcedSource": {
                "prereq_tables": ["CcdVisit", "Object"],
                "columns": {
                    "objectId,ccdVisitId,psFlux,psFlux_Sigma":
                        columns.ForcedSourceGenerator(visit_radius=3.0, filters="ugr"),
                },
            }
        }

        chunk_id = 3525
        seed = 1
        generator = DataGenerator(generator_spec, chunker, seed=seed)
        edge_width = 0.017 # degrees
        chunk_tables = generator.make_chunk(chunk_id, edge_width=edge_width, edge_only=False)
        self.assertIn("ForcedSource", chunk_tables.keys())
        self.assertIn("Object", chunk_tables.keys())
        self.assertIn("CcdVisit", chunk_tables.keys())

        print(len(chunk_tables['ForcedSource']))
        print(len(chunk_tables['CcdVisit']))
        print(len(chunk_tables['Object']))
        self.assertEqual(set(chunk_tables['ForcedSource']['ccdVisitId']),
                         set(chunk_tables['CcdVisit']['ccdVisitId']))

