library(testthat)
source("calibration_conversion.R")
test_that(
   "Calibration filter first tests",
   {
     DirIn <- "tests/test_input"
     DirOut <- "tests/test_output/pfs/out"
     FileSchmData <- file.path(DirIn,"avro_schemas/dp0p/prt_calibrated.avsc" )
     FileSchmQf <- file.path(DirIn,"avro_schemas/dp0p/flags_calibration.avsc" )
     TermConv <- "resistance"
     TermQf <- "resistance"
     TermUcrt <- "resistance(R)"
     FuncConv  <- "def.cal.conv.poly"
     FuncUcrt <- "def.ucrt.meas"
     FileUcrtFdas <-  file.path(DirIn,"uncertainty_fdas/fdas_calibration_uncertainty_general.json" )
     NumDayExpiMax <- NA
    calibration_filter(DirIn=DirIn, DirOut=DirOut, FileSchmData=FileSchmData, FileSchmQf=FileSchmQf, TermConv=TermConv, FuncConv=FuncConv,TermQf=TermQf, TermUcrt=TermUcrt, FileUcrtFdas=FileUcrtFdas, FuncUcrt=FuncUcrt, NumDayExpiMax = NumDayExpiMax)
    
   }
)


#path to FDAS uncertainty is empty