import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker, UniformSpatialModel

num_stripes = 200
num_substripes = 5
chunker = Chunker(0, num_stripes, num_substripes)
edge_width = 0.017  # degrees, must be >= overlap

spec = {
    "Object": {
        "columns": {"objectId": columns.ObjIdGenerator(),
                    "psRa,psDecl": columns.RaDecGenerator(),
                    "uPsFlux,gPsFlux,rPsFlux,iPsFlux,zPsFlux,yPsFlux": columns.MagnitudeGenerator(n_mags=6)
                    },
        "density": UniformSpatialModel(1000),
    },
    "CcdVisit": {
        "from_file": "visit_table.csv",
        "columns": "ccdVisitId,filterName,ra,decl"
    },
    "ForcedSource": {
        "prereq_tables": ["CcdVisit", "Object"],
        "columns": {
            "objectId,ccdVisitId,psFlux,psFlux_Sigma,flags":
                columns.ForcedSourceGenerator(visit_radius=1.4, filters="ugriz"),
        },
    }
}

directors = {
    "Object": {
        "ForcedSource": "objectId"
    }
}


