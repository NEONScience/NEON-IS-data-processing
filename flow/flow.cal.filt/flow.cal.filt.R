# DirEXTENSION <- "2019/01/01/"
# DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs/prt", DirEXTENSION)
#      DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
#      DirSubCopy <- "data"
#require("NEONprocIS.base")
log <- NEONprocIS.base:::def.log.init()



DirEXTENSION <- "prt/2019/01/01"
    DirIn <- file.path("./test_input/pfs", DirEXTENSION)
    DirOut <- "./test_output/prt"
    DirSubCopy <- "data"
DirBgn <- DirIn # Input directory. 
print(base::paste0('Input directory Flow C: ',DirBgn)) 
log$debug(base::paste0('Input directory Flow C: ',DirBgn))

# Retrieve base output path
DirOut <- DirOut
print(base::paste0('Output directory Flow C: ',DirOut))
log$debug(base::paste0('Output directory Flow C: ',DirOut))

DirSubCopy <- DirSubCopy
print(base::paste0('DirSubCopy directory Flow C: ',DirSubCopy))
log$debug(base::paste0('DirSubCopy directory Flow C: ',DirSubCopy))

source("./calibration_filter.R")
filter_calibration_files(DirBgn, DirOut, DirSubCopy)


