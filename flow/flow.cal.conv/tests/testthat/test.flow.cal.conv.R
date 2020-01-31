library(testthat)

test_that(
  "Calibration Conversion files first test",
  {
      DirEXTENSION <- "prt/2019/01/01"
    DirIn <- file.path("Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_input/prt_calibration_filter/pfs", DirEXTENSION)
    DirOut <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_output/pfs/prt"
    FileSchmData <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_input/avro_schemas/dp0p/prt_calibrated.avsc"
    FileSchmQf <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_input/avro_schemas/dp0p/flags_validCal.avsc"
    Term <- "resistance"
    NumDayExpiMax <- "NA"
    TermUcrtFdas="resistance(R)"
    FileUcrtFdas <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.conv/tests/test_input/uncertainty_fdas/fdas_calibration_uncertainty_general.json"
    filter_calibration_conversion(DirIn,DirOut, FileSchmData, FileSchmQf, Term, NumDayExpiMax, TermUcrtFdas, FileUcrtFdas=FileUcrtFdas)
    expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
  }
)
