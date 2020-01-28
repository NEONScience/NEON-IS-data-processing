# # library(testthat)
# # 
# test_that(
#   "Calibration file exists in the output folder",
#   {
#     DirEXTENSION <- "prt/2019/01/01"
#     DirIn <- file.path("/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
#     DirOut <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
#     DirSubCopy <- "data"
#     filter_calibration_files(DirIn,DirOut, DirSubCopy)
#     expect_that(file.exists(file.path(DirOut, DirEXTENSION)),is_true())
#   }
# )
# 
# test_that(
#   "All Calibration files expired, expected to see the latest expired in the output folder",
#   {
#     DirEXTENSION <- "2019/01/03"
#     DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs/prt", DirEXTENSION)
#     DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
#     DirCal <- file.path(DirOut, DirEXTENSION, "19963/calibration/resistance/30000000014473_WO12766_78769.xml")
#     DirSubCopy <- "data"
#     filter_calibration_files(DirIn,DirOut, DirSubCopy)
#     expect_that(file.exists(DirCal),is_true())
#   }
# )
# 
# test_that(S
#   "No Datums Found Message when input is found",
#   {
#     DirEXTENSION <- "prt/2019/01/02"
#     DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs", DirEXTENSION)
#     DirOut <- "C:/Users/vchundru/git//NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
#     DirSubCopy <- "data"
#     expect_that(filter_calibration_files(DirIn,DirOut, DirSubCopy), gives_warning(base::paste0('No datums found for processing in parent directory ',DirIn)))
#   }
# )

# #When no calibration for the Data date, we get the calibration before the start date and latest that expired
# test_that(
#   "No Valid Calibration for the Data Date, get the latest one before the data date.",
#   {
#     DirEXTENSION <- "2019/01/04/"
#     DirIn <- file.path(" ", DirEXTENSION)
#     DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
#     DirSubCopy <- "data"
#     filePath <- paste0(DirOut, DirEXTENSION,"19963/calibration/resistance/30000000014473_WO12766_78769.xml" )
#     print(file.path(filePath))
#     filter_calibration_files(DirIn,DirOut, DirSubCopy)
#     expect_true(file.exists(filePath))
#   }
# )

#When no calibration for the Data date, and there are no calibration before the data date, we don't get the calibrations
#even if there are calibration after the data date.
test_that(
  "No Valid Calibration for the Data Date, if there is no calibration file found before data date",
  {
    DirEXTENSION <- "2019/01/01/"
    DirIn <- file.path("/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs/prt", DirEXTENSION)
    DirOut <- "/Users/vchundru/git/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
    DirSubCopy <- "data"
    filePath <- paste0(DirOut, DirEXTENSION,"19963/calibration/resistance/30000000014473_WO12766_123059.xml" )
    print(file.path(filePath))
    filter_calibration_files(DirIn,DirOut, DirSubCopy)
    expect_false(file.exists(filePath))
  }
)