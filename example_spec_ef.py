import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker

num_stripes = 200
num_substripes = 5
chunker = Chunker(0, num_stripes, num_substripes)

spec = {
    "Object": {
        "columns": {"objectId": columns.ObjIdGeneratorEF(),
                    "ra,decl": columns.RaDecGeneratorEF(chunker),
                    "mag_u,mag_g,mag_r,mag_i,mag_z": columns.MagnitudeGeneratorEF(n_mags=5)
                    }
    },
    "CcdVisit": {
        "columns": {"ccdVisitId": columns.VisitIdGeneratorEF(),
                    "filterName": columns.FilterGeneratorEF(filters="ugriz"),
                    "ra,decl": columns.RaDecGeneratorEF(chunker)
                    }
    },
    "ForcedSource": {
        "prereq_row": "Object",
        "prereq_tables": ["CcdVisit"],
        "columns": {
            "objectId,ccdVisitId,psFlux,psFlux_Sigma":
                columns.ForcedSourceGenerator(visit_radius=1.4, filters="ugriz"),
        },
    }
}
