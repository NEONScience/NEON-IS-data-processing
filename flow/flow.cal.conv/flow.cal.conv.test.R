library(testthat)

test_that(
  "Calibration Conversion files first test",
  {
      DirEXTENSION <- "prt/2019/01/01"
    DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_input/prt_calibration_filter/pfs/", DirEXTENSION)
    DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_output/pfs/prt"
    FileSchmData <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_input/avro_schemas/dp0p/prt_calibrated.avsc"
    FileSchmQf <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_input/avro_schemas/dp0p/flags_validCal.avsc"
    Term <- "resistance"
    NumDayExpiMax <- "NA"
    filter_calibration_conversion(DirIn,DirOut, FileSchmData, FileSchmQf, Term, NumDayExpiMax)
    expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
  }
)
