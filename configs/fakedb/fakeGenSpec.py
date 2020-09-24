import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker

num_stripes = 200
num_substripes = 5
chunker = Chunker(0, num_stripes, num_substripes)
edge_width = 0.017  # degrees, must be >= overlap

spec = {
    "Object": {
        "columns": {"objectId": columns.ObjIdGenerator(),
                    "ra,decl": columns.RaDecGenerator(chunker),
                    "mag_u,mag_g,mag_r,mag_i,mag_z": columns.MagnitudeGenerator(n_mags=5)
                    }
    },
    "CcdVisit": {
        "columns": {"ccdVisitId": columns.VisitIdGenerator(),
                    "filterName": columns.FilterGenerator(filters="ugriz"),
                    "ra,decl": columns.RaDecGenerator(chunker, ignore_edge_only=True)
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

directors = {
    "Object": {
        "ForcedSource": "objectId"
    }
}
