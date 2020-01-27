arg <- base::commandArgs(trailingOnly=TRUE)

# DirEXTENSION <- "2019/01/01/"
# DirIn <- file.path("C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_input/pfs/prt", DirEXTENSION)
#      DirOut <- "C:/Users/vchundru/git/NEONScience/NEON-IS-data-processing/flow/flow.cal.filt/tests/test_output/prt"
#      DirSubCopy <- "data"
log <- NEONprocIS.base::def.log.init()

# Options
base::options(digits.secs = 3)

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut"),NameParaOptn="DirSubCopy",log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,c('calibration'))) # Make sure we don't symbolically link the calibration folder
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

source("calibration_filter.R")
filter_calibration_files(DirIn,DirOut, DirSubCopy)
