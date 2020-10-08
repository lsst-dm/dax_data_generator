import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker, UniformSpatialModel

num_stripes = 200
num_substripes = 5
chunker = Chunker(0, num_stripes, num_substripes)
edge_width = 0.017  # degrees, must be >= overlap


spec = {
    "Object": {
        "columns": {
            "objectId": columns.ObjIdGenerator(),
            "ra,decl": columns.RaDecGenerator(),
            "uPsFlux,gPsFlux,rPsFlux,iPsFlux,zPsFlux,yPsFlux":
            columns.MagnitudeGenerator(n_mags=6),
            "uPsFluxErr,gPsFluxErr,rPsFluxErr,iPsFluxErr,zPsFluxErr,yPsFluxErr":
            columns.UniformGenerator(min_val=0.05, max_val=0.5, n_columns=6, column_seed=6),
            "psLnL,psChi2,psN": columns.UniformGenerator(n_columns=6, column_seed=7),
            "uBbdRa,uBdRaErr,uBdDecl,uBdDeclErr,uBdE1,uBdE1Err,uBdE2,uBdE2Err,uBdFluxB,uBdFluxBErr,uBdFluxD,uBdFluxDErr,uBdReB,uBdReBErr,uBdReD,uBdReDErr,uBdLnL,uBdChi2,uBdN,gBbdRa,gBdRaErr,gBdDecl,gBdDeclErr,gBdE1,gBdE1Err,gBdE2,gBdE2Err,gBdFluxB,gBdFluxBErr,gBdFluxD,gBdFluxDErr,gBdReB,gBdReBErr,gBdReD,gBdReDErr,gBdLnL,gBdChi2,gBdN,rBbdRa,rBdRaErr,rBdDecl,rBdDeclErr,rBdE1,rBdE1Err,rBdE2,rBdE2Err,rBdFluxB,rBdFluxBErr,rBdFluxD,rBdFluxDErr,rBdReB,rBdReBErr,rBdReD,rBdReDErr,rBdLnL,rBdChi2,rBdN,iBbdRa,iBdRaErr,iBdDecl,iBdDeclErr,iBdE1,iBdE1Err,iBdE2,iBdE2Err,iBdFluxB,iBdFluxBErr,iBdFluxD,iBdFluxDErr,iBdReB,iBdReBErr,iBdReD,iBdReDErr,iBdLnL,iBdChi2,iBdN,zBbdRa,zBdRaErr,zBdDecl,zBdDeclErr,zBdE1,zBdE1Err,zBdE2,zBdE2Err,zBdFluxB,zBdFluxBErr,zBdFluxD,zBdFluxDErr,zBdReB,zBdReBErr,zBdReD,zBdReDErr,zBdLnL,zBdChi2,zBdN,yBbdRa,yBdRaErr,yBdDecl,yBdDeclErr,yBdE1,yBdE1Err,yBdE2,yBdE2Err,yBdFluxB,yBdFluxBErr,yBdFluxD,yBdFluxDErr,yBdReB,yBdReBErr,yBdReD,yBdReDErr,yBdLnL,yBdChi2,yBdN":
            columns.UniformGenerator(n_columns=114, column_seed=8),
            "ugStd,ugStdErr,grStd,grStdErr,riStd,riStdErr,izStd,izStdErr,zyStd,zyStdErr":
            columns.UniformGenerator(min_val=-2, max_val=2, n_columns=10, column_seed=9),
            "uRa,uRaErr,uDecl,uDeclErr,gRa,gRaErr,gDecl,gDeclErr,rRa,rRaErr,rDecl,rDeclErr,iRa,iRaErr,iDecl,iDeclErr,zRa,zRaErr,zDecl,zDeclErr,yRa,yRaErr,yDecl,yDeclErr":
            columns.UniformGenerator(n_columns=24, column_seed=10),
            "uE1,uE1Err,uE2,uE2Err,uE1_E2_Cov,gE1,gE1Err,gE2,gE2Err,gE1_E2_Cov,rE1,rE1Err,rE2,rE2Err,rE1_E2_Cov,iE1,iE1Err,iE2,iE2Err,iE1_E2_Cov,zE1,zE1Err,zE2,zE2Err,zE1_E2_Cov,yE1,yE1Err,yE2,yE2Err,yE1_E2_Cov":
            columns.UniformGenerator(min_val=-1, max_val=1, n_columns=30, column_seed=11),
            "uMSum,uMSumErr,gMSum,gMSumErr,rMSum,rMSumErr,iMSum,iMSumErr,zMSum,zMSumErr,yMSum,yMSumErr,uM4,gM4,rM4,iM4,zM4,yM4":
            columns.UniformGenerator(min_val=-1, max_val=1, n_columns=18, column_seed=12),
            "uPetroRad,uPetroRadErr,gPetroRad,gPetroRadErr,rPetroRad,rPetroRadErr,iPetroRad,iPetroRadErr,zPetroRad,zPetroRadErr,yPetroRad,yPetroRadErr,petroFilter,uPetroFlux,uPetroFluxErr,gPetroFlux,gPetroFluxErr,rPetroFlux,rPetroFluxErr,iPetroFlux,iPetroFluxErr,zPetroFlux,zPetroFluxErr,yPetroFlux,yPetroFluxErr,uPetroRad50,uPetroRad50Err,gPetroRad50,gPetroRad50Err,rPetroRad50,rPetroRad50Err,iPetroRad50,iPetroRad50Err,zPetroRad50,zPetroRad50Err,yPetroRad50,yPetroRad50Err,uPetroRad90,uPetroRad90Err,gPetroRad90,gPetroRad90Err,rPetroRad90,rPetroRad90Err,iPetroRad90,iPetroRad90Err,zPetroRad90,zPetroRad90Err,yPetroRad90,yPetroRad90Err":
            columns.UniformGenerator(max_val=10, n_columns=49, column_seed=13),
            "uKronRad,uKronRadErr,gKronRad,gKronRadErr,rKronRad,rKronRadErr,iKronRad,iKronRadErr,zKronRad,zKronRadErr,yKronRad,yKronRadErr,kronFilter,uKronFlux,uKronFluxErr,gKronFlux,gKronFluxErr,rKronFlux,rKronFluxErr,iKronFlux,iKronFluxErr,zKronFlux,zKronFluxErr,yKronFlux,yKronFluxErr,uKronRad50,uKronRad50Err,gKronRad50,gKronRad50Err,rKronRad50,rKronRad50Err,iKronRad50,iKronRad50Err,zKronRad50,zKronRad50Err,yKronRad50,yKronRad50Err,uKronRad90,uKronRad90Err,gKronRad90,gKronRad90Err,rKronRad90,rKronRad90Err,iKronRad90,iKronRad90Err,zKronRad90,zKronRad90Err,yKronRad90,yKronRad90Err":
            columns.UniformGenerator(max_val=10, n_columns=49, column_seed=14),
            "uApN,gApN,rApN,iApN,zApN,yApN,extendedness,FLAGS1,FLAGS2":
            columns.UniformGenerator(max_val=1, n_columns=9, column_seed=15),
        },

        "density": UniformSpatialModel(50000),
    },
    "ForcedSource": {
        "prereq_tables": ["CcdVisit", "Object"],
        "columns": {
            "objectId,ccdVisitId,psFlux,psFlux_Sigma":
                columns.ForcedSourceGenerator(visit_radius=1.7, filters="ugriz"),
        },
    }
}

spec["CcdVisit"] = {
    "from_file": "visit_table.csv",
    "columns": "ccdVisitId,filterName,ra,decl"
}
