import lsst.dax.data_generator.columns as columns
from lsst.dax.data_generator import Chunker, UniformSpatialModel

# Verify files in ingestCfgs all have correct database name, stripes, and substripes.
num_stripes = 340
num_substripes = 3
chunker = Chunker(0, num_stripes, num_substripes)
edge_width = 0.0167  # degrees, must be >= overlap

spec = {
    "Object": {
        "columns": {
            "objectId": columns.ObjIdGenerator(),
            "parentObjectId,prv_inputId": columns.PoissonGenerator(mean_val=10, n_columns=2, column_seed=1),
            "psRa,psRaErr,psDecl,psDeclErr": columns.RaDecGenerator(include_err=True),
            "psMuRa,psMuRaErr,psMuDecl,psMuDeclErr,psParallax,psParallaxErr": columns.UniformGenerator(n_columns=6, min_val=-5, max_val=5, column_seed=2),
            "uPsFlux": columns.MagnitudeGenerator(n_mags=1, column_seed=102),
            "uPsFluxErr": columns.UniformGenerator(min_val=0.05, max_val=0.5, n_columns=1, column_seed=103),
            "gPsFlux": columns.MagnitudeGenerator(n_mags=1, column_seed=104),
            "gPsFluxErr": columns.UniformGenerator(min_val=0.05, max_val=0.5, n_columns=1, column_seed=105),
            "rPsFlux": columns.MagnitudeGenerator(n_mags=1, column_seed=106),
            "rPsFluxErr": columns.UniformGenerator(min_val=0.05, max_val=0.5, n_columns=1, column_seed=107),
            "iPsFlux": columns.MagnitudeGenerator(n_mags=1, column_seed=107),
            "iPsFluxErr": columns.UniformGenerator(min_val=0.05, max_val=0.5, n_columns=1, column_seed=109),
            "zPsFlux": columns.MagnitudeGenerator(n_mags=1, column_seed=108),
            "zPsFluxErr": columns.UniformGenerator(min_val=0.05, max_val=0.5, n_columns=1, column_seed=111),
            "yPsFlux": columns.MagnitudeGenerator(n_mags=1, column_seed=109),
            "yPsFluxErr": columns.UniformGenerator(min_val=0.05, max_val=0.5, n_columns=1, column_seed=113),
            "psLnL,psChi2": columns.UniformGenerator(n_columns=2, column_seed=114),
            "psN": columns.PoissonGenerator(mean_val=30, column_seed=8),
            "uBbdRa,uBdRaErr,uBdDecl,uBdDeclErr,uBdE1,uBdE1Err,uBdE2,uBdE2Err,uBdFluxB,uBdFluxBErr,uBdFluxD,uBdFluxDErr,uBdReB,uBdReBErr,uBdReD,uBdReDErr,uBdLnL,uBdChi2": columns.UniformGenerator(n_columns=18, column_seed=9),
            "uBdN": columns.PoissonGenerator(mean_val=10, column_seed=10),
            "gBbdRa,gBdRaErr,gBdDecl,gBdDeclErr,gBdE1,gBdE1Err,gBdE2,gBdE2Err,gBdFluxB,gBdFluxBErr,gBdFluxD,gBdFluxDErr,gBdReB,gBdReBErr,gBdReD,gBdReDErr,gBdLnL,gBdChi2":columns.UniformGenerator(n_columns=18, column_seed=11),
            "gBdN": columns.PoissonGenerator(mean_val=10, column_seed=12),
            "rBbdRa,rBdRaErr,rBdDecl,rBdDeclErr,rBdE1,rBdE1Err,rBdE2,rBdE2Err,rBdFluxB,rBdFluxBErr,rBdFluxD,rBdFluxDErr,rBdReB,rBdReBErr,rBdReD,rBdReDErr,rBdLnL,rBdChi2":columns.UniformGenerator(n_columns=18, column_seed=13),
            "rBdN": columns.PoissonGenerator(mean_val=10, column_seed=14),
            "iBbdRa,iBdRaErr,iBdDecl,iBdDeclErr,iBdE1,iBdE1Err,iBdE2,iBdE2Err,iBdFluxB,iBdFluxBErr,iBdFluxD,iBdFluxDErr,iBdReB,iBdReBErr,iBdReD,iBdReDErr,iBdLnL,iBdChi2":columns.UniformGenerator(n_columns=18, column_seed=15),
            "iBdN": columns.PoissonGenerator(mean_val=10, column_seed=16),
            "zBbdRa,zBdRaErr,zBdDecl,zBdDeclErr,zBdE1,zBdE1Err,zBdE2,zBdE2Err,zBdFluxB,zBdFluxBErr,zBdFluxD,zBdFluxDErr,zBdReB,zBdReBErr,zBdReD,zBdReDErr,zBdLnL,zBdChi2":columns.UniformGenerator(n_columns=18, column_seed=17),
            "zBdN": columns.PoissonGenerator(mean_val=10, column_seed=18),
            "yBbdRa,yBdRaErr,yBdDecl,yBdDeclErr,yBdE1,yBdE1Err,yBdE2,yBdE2Err,yBdFluxB,yBdFluxBErr,yBdFluxD,yBdFluxDErr,yBdReB,yBdReBErr,yBdReD,yBdReDErr,yBdLnL,yBdChi2":columns.UniformGenerator(n_columns=18, column_seed=19),
            "yBdN": columns.PoissonGenerator(mean_val=10, column_seed=20),
            "ugStd,ugStdErr,grStd,grStdErr,riStd,riStdErr,izStd,izStdErr,zyStd,zyStdErr":
            columns.UniformGenerator(min_val=-2, max_val=2, n_columns=10, column_seed=21),
            "uRa,uRaErr,uDecl,uDeclErr,gRa,gRaErr,gDecl,gDeclErr,rRa,rRaErr,rDecl,rDeclErr,iRa,iRaErr,iDecl,iDeclErr,zRa,zRaErr,zDecl,zDeclErr,yRa,yRaErr,yDecl,yDeclErr":
            columns.UniformGenerator(n_columns=24, column_seed=22),
            "uE1,uE1Err,uE2,uE2Err,uE1_E2_Cov,gE1,gE1Err,gE2,gE2Err,gE1_E2_Cov,rE1,rE1Err,rE2,rE2Err,rE1_E2_Cov,iE1,iE1Err,iE2,iE2Err,iE1_E2_Cov,zE1,zE1Err,zE2,zE2Err,zE1_E2_Cov,yE1,yE1Err,yE2,yE2Err,yE1_E2_Cov":
            columns.UniformGenerator(min_val=-1, max_val=1, n_columns=30, column_seed=23),
            "uMSum,uMSumErr,gMSum,gMSumErr,rMSum,rMSumErr,iMSum,iMSumErr,zMSum,zMSumErr,yMSum,yMSumErr,uM4,gM4,rM4,iM4,zM4,yM4":
            columns.UniformGenerator(min_val=-1, max_val=1, n_columns=18, column_seed=24),
            "uPetroRad,uPetroRadErr,gPetroRad,gPetroRadErr,rPetroRad,rPetroRadErr,iPetroRad,iPetroRadErr,zPetroRad,zPetroRadErr,yPetroRad,yPetroRadErr":
            columns.UniformGenerator(max_val=10, n_columns=12, column_seed=25),
            "petroFilter": columns.FilterGenerator(column_seed=251),
            "uPetroFlux,uPetroFluxErr,gPetroFlux,gPetroFluxErr,rPetroFlux,rPetroFluxErr,iPetroFlux,iPetroFluxErr,zPetroFlux,zPetroFluxErr,yPetroFlux,yPetroFluxErr,uPetroRad50,uPetroRad50Err,gPetroRad50,gPetroRad50Err,rPetroRad50,rPetroRad50Err,iPetroRad50,iPetroRad50Err,zPetroRad50,zPetroRad50Err,yPetroRad50,yPetroRad50Err,uPetroRad90,uPetroRad90Err,gPetroRad90,gPetroRad90Err,rPetroRad90,rPetroRad90Err,iPetroRad90,iPetroRad90Err,zPetroRad90,zPetroRad90Err,yPetroRad90,yPetroRad90Err":
            columns.UniformGenerator(max_val=10, n_columns=36, column_seed=252),
            "uKronRad,uKronRadErr,gKronRad,gKronRadErr,rKronRad,rKronRadErr,iKronRad,iKronRadErr,zKronRad,zKronRadErr,yKronRad,yKronRadErr":
            columns.UniformGenerator(max_val=10, n_columns=12, column_seed=26),
            "kronFilter": columns.FilterGenerator(column_seed=261),
            "uKronFlux,uKronFluxErr,gKronFlux,gKronFluxErr,rKronFlux,rKronFluxErr,iKronFlux,iKronFluxErr,zKronFlux,zKronFluxErr,yKronFlux,yKronFluxErr,uKronRad50,uKronRad50Err,gKronRad50,gKronRad50Err,rKronRad50,rKronRad50Err,iKronRad50,iKronRad50Err,zKronRad50,zKronRad50Err,yKronRad50,yKronRad50Err,uKronRad90,uKronRad90Err,gKronRad90,gKronRad90Err,rKronRad90,rKronRad90Err,iKronRad90,iKronRad90Err,zKronRad90,zKronRad90Err,yKronRad90,yKronRad90Err":
            columns.UniformGenerator(max_val=10, n_columns=36, column_seed=262),
            "uApN,gApN,rApN,iApN,zApN,yApN":columns.PoissonGenerator(mean_val=10, n_columns=6, column_seed=27),
            "extendedness": columns.UniformGenerator(max_val=10, n_columns=1, column_seed=28),
            "FLAGS1,FLAGS2": columns.PoissonGenerator(mean_val=1000, n_columns=2, column_seed=29),
        },

        "density": UniformSpatialModel(5000),
    },
    "ForcedSource": {
        "prereq_tables": ["CcdVisit", "Object"],
        "columns": {
            "objectId,ccdVisitId,psFlux,psFlux_Err,flags":
                columns.ForcedSourceGenerator(visit_radius=1.7, filters="ugriz"),
        },
    },
    "Source": {
        "prereq_tables": ["CcdVisit", "Object"],
        "columns": {
            "objectId,ccdVisitId,psFlux,psFlux_Err,flags":
                columns.SourceGenerator(visit_radius=1.7, filters="ugriz"),
        },
    },
    "CcdVisit": {
        "from_file": "visit_table.csv",
        "columns": "ccdVisitId,filterName,ra,decl"
    }
}

directors = {
    "Object": {
        "ForcedSource": "objectId"
    }
}

