
import os
import unittest
import tempfile
from lsst.dax.data_generator import DataGenerator
import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker, UniformSpatialModel

num_stripes = 200
num_substripes = 5
chunker = Chunker(0, num_stripes, num_substripes)

# From lsst.sphgeom
RAD_PER_DEG = 0.0174532925199432957692369076849

chunk3525_visits = """
12820000000,g,180.78827718185477,-81.9102231995917
12820300000,z,186.8356297367516,-82.7948759909024
12820400000,i,183.1161985598496,-82.3019432815631
12820400001,z,180.640479686363,-82.62327795054735
12820400002,i,180.99272432414028,-82.06439195113585
12820400003,i,183.33806442213995,-82.06904964148708
12820400004,u,182.7827246419214,-82.65558368941461
12820400005,r,183.77229514198172,-82.44869779607919
12820400006,g,185.05622117678053,-81.95908962188572
12820400007,i,186.68065257828533,-82.11268712631474
"""


class TestDataGenerator(unittest.TestCase):

    def testObjectTable(self):
        generator_spec = {
            "Object": {
                "columns": {"objectId": columns.ObjIdGenerator(),
                            "ra,decl": columns.RaDecGenerator(),
                            "uPsFlux,gPsFlux,rPsFlux,iPsFlux,zPsFlux,yPsFlux": columns.MagnitudeGenerator(
                                n_mags=6)
                            },
                "density": UniformSpatialModel(500),
            }
        }
        chunk_id = 3525
        seed = 1

        generator = DataGenerator(generator_spec, chunker, seed=seed)

        edge_width = 0.017  # degrees
        data = generator.make_chunk(chunk_id, edge_width=edge_width, edge_only=False)
        chunk_area = chunker.getChunkBounds(chunk_id).getArea() / RAD_PER_DEG**2
        self.assertIn('Object', data.keys())
        self.assertAlmostEqual(len(data["Object"]), int(500 * chunk_area), delta=3)

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

        with tempfile.TemporaryDirectory() as data_dir:

            with open(os.path.join(data_dir, "visit_table.csv"), "w") as f:
                print(chunk3525_visits, file=f)

            generator_spec = {
                "Object": {
                    "columns": {"objectId": columns.ObjIdGenerator(),
                                "psRa,psDecl": columns.RaDecGenerator(),
                                "uPsFlux,gPsFlux,rPsFlux,iPsFlux,zPsFlux,yPsFlux": columns.MagnitudeGenerator(
                                    n_mags=6)
                                },
                    "density": UniformSpatialModel(100),
                },
                "CcdVisit": {
                    "from_file": os.path.join(data_dir, "visit_table.csv"),
                    "columns": "ccdVisitId,filterName,ra,decl"
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
            edge_width = 0.017  # degrees
            chunk_tables = generator.make_chunk(chunk_id, edge_width=edge_width, edge_only=False,
                                                return_pregenerated=True)
        self.assertIn("ForcedSource", chunk_tables.keys())
        self.assertIn("Object", chunk_tables.keys())

        print(len(chunk_tables['ForcedSource']))
        print(len(chunk_tables['Object']))
        self.assertEqual(set(chunk_tables['ForcedSource']['ccdVisitId']),
                         set(chunk_tables['CcdVisit']['ccdVisitId']))

