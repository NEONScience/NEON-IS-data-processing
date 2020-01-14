library(testthat)

test_that(
  "Calibration file exists in the output folder",
  {
    DirEXTENSION <- "prt/2019/01/01"
    DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
    DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
    DirSubCopy <- "data"
    filter_calibration_files(DirIn,DirOut, DirSubCopy)
    expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
  }
)

test_that(
  "All Calibration files expired, expected to see the latest expired in the output folder",
  {
    DirEXTENSION <- "2019/01/03"
    DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs/prt", DirEXTENSION)
    DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
    DirCal <- file.path(DirOut, DirEXTENSION, "19963/calibration/resistance/30000000014473_WO12766_78769.xml")
    DirSubCopy <- "data"
    filter_calibration_files(DirIn,DirOut, DirSubCopy)
    expect_that(file.exists(DirCal),is_true())
  }
)

test_that(
  "No Datums Found Message when input is found",
  {
    DirEXTENSION <- "prt/2019/01/02"
    DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
    DirOut <- "C:/Users/vchundru/git//NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
    DirSubCopy <- "data"
    expect_that(filter_calibration_files(DirIn,DirOut, DirSubCopy), gives_warning(base::paste0('No datums found for processing in parent directory ',DirIn)))
  }
)

test_that(
  "No Valid Calibration for the Data Date",
  {
    DirEXTENSION <- "prt/2019/01/02"
    DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
    DirOut <- "C:/Users/vchundru/git//NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
    DirSubCopy <- "data"
    expect_that(filter_calibration_files(DirIn,DirOut, DirSubCopy), gives_warning(base::paste0('No datums found for processing in parent directory ',DirIn)))
  }
)