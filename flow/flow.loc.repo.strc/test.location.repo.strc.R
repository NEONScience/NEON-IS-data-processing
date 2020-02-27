library(testthat)
source("location_repo_strc.R")
# test_that(
#    "location repo strc all files exists",
#    {
#      DirIn <- "tests/test_input/pfs/prt"
#      DirOut <- "tests/test_output/pfs/out"
#      DataFilePath <- file.path(DirOut,"2019/01/01/CFGLOC108440/data/prt_19963_2019-01-01.avro" )
#      FlagsFilePath <- file.path(DirOut,"2019/01/01/CFGLOC108440/flags/prt_19963_2019-01-01_flagsCal.avro" )
#      LocationFilePath <- file.path(DirOut,"2019/01/01/CFGLOC108440/location/prt_19963_locations.json" )
#      UncertaintyCoefFilePath <- file.path(DirOut,"2019/01/01/CFGLOC108440/uncertainty_coef/prt_19963_2019-01-01_uncertaintyCoef.json" )
#      UnCertaintyDataFilePath <- file.path(DirOut,"2019/01/01/CFGLOC108440/uncertainty_data/prt_19963_2019-01-01_uncertaintyData.avro" )
#      restructure_location_repo(DirIn, DirOut, Comb=TRUE)
#      expect_true(file.exists(DataFilePath))
#      expect_true(file.exists(FlagsFilePath))
#      expect_true(file.exists(LocationFilePath))
#      expect_true(file.exists(UncertaintyCoefFilePath))
#      expect_true(file.exists(UnCertaintyDataFilePath))
#    }
# )

# test_that(
#   "location repo strc with no location files",
#   {
#     DirEXTENSION <- "2019/01/02"
#     DirIn <- file.path("tests/test_input/pfs/prt", DirEXTENSION)
#     DirOut <- "tests/test_output/pfs/out"
#     restructure_location_repo(DirIn, DirOut, Comb=TRUE)
#     expect_false(file.exists(DirOut))
#     
# 
#   }
# )


# test_that(
#   "location repo strc where there are more than one locations",
#   {
#     DirEXTENSION <- "2019/01/03"
#     DirIn <- file.path("tests/test_input/pfs/prt", DirEXTENSION)
#     DirOut <- "tests/test_output/pfs/out"
#     DataFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/data/prt_19963_2019-01-01.avro" )
#     FlagsFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/flags/prt_19963_2019-01-01_flagsCal.avro" )
#     LocationFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/location/prt_19963_locations.json" )
#     UncertaintyCoefFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/uncertainty_coef/prt_19963_2019-01-01_uncertaintyCoef.json" )
#     UnCertaintyDataFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/uncertainty_data/prt_19963_2019-01-01_uncertaintyData.avro" )
#     
#     DataFilePath1 <- file.path(DirOut,"2019/01/03/CFGLOC108441/data/prt_19963_2019-01-01.avro" )
#     FlagsFilePath1 <- file.path(DirOut,"2019/01/03/CFGLOC108441/flags/prt_19963_2019-01-01_flagsCal.avro" )
#     LocationFilePath1 <- file.path(DirOut,"2019/01/03/CFGLOC108441/location/prt_19963_locations.json" )
#     UncertaintyCoefFilePath1 <- file.path(DirOut,"2019/01/03/CFGLOC108441/uncertainty_coef/prt_19963_2019-01-01_uncertaintyCoef.json" )
#     UnCertaintyDataFilePath1 <- file.path(DirOut,"2019/01/03/CFGLOC108441/uncertainty_data/prt_19963_2019-01-01_uncertaintyData.avro" )
#     restructure_location_repo(DirIn, DirOut, Comb=TRUE)
#     expect_true(file.exists(DataFilePath))
#     expect_true(file.exists(FlagsFilePath))
#     expect_true(file.exists(LocationFilePath))
#     expect_true(file.exists(UncertaintyCoefFilePath))
#     expect_true(file.exists(UnCertaintyDataFilePath))
#     
#     expect_true(file.exists(DataFilePath1))
#     expect_true(file.exists(FlagsFilePath1))
#     expect_true(file.exists(LocationFilePath1))
#     expect_true(file.exists(UncertaintyCoefFilePath1))
#     expect_true(file.exists(UnCertaintyDataFilePath1))
#   }
# )

test_that(
  "location repo strc when Comb variable is FALSE",
  {
    DirEXTENSION <- "2019/01/03"
    DirIn <- file.path("tests/test_input/pfs/prt", DirEXTENSION)
    DirOut <- "tests/test_output/pfs/out"
    # DataFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/data/prt_19963_2019-01-01.avro" )
    # FlagsFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/flags/prt_19963_2019-01-01_flagsCal.avro" )
    # LocationFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/location/prt_19963_locations.json" )
    # UncertaintyCoefFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/uncertainty_coef/prt_19963_2019-01-01_uncertaintyCoef.json" )
    # UnCertaintyDataFilePath <- file.path(DirOut,"2019/01/03/CFGLOC108440/uncertainty_data/prt_19963_2019-01-01_uncertaintyData.avro" )
    restructure_location_repo(DirIn, DirOut, Comb=FALSE)
    # expect_true(file.exists(DataFilePath))
    # expect_true(file.exists(FlagsFilePath))
    # expect_true(file.exists(LocationFilePath))
    # expect_true(file.exists(UncertaintyCoefFilePath))
    # expect_true(file.exists(UnCertaintyDataFilePath))
  }
)


