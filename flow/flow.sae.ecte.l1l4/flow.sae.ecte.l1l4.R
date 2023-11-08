#######################################################
#' @title Call ECTE L1L4 workflow from eddy4r image.
# This is an experiment to see how Pachyderm handles large image.
# input would be l0p h5 file and location file at site level.
# the location file is faked now, its format is not real, the json parser also need to change later
# input directory would be something like #/pfs/#/year/mm/dd/site
# assume input directory has nested location file and 9 l0p h5 files:
# 2020/01/05/ABBY/data/NEON.D16.ABBY.IP0.00200.001.ecte.2020-01-01.l0p.h5.gz
#                     /NEON.D16.ABBY.IP0.00200.001.ecte.2020-01-02.l0p.h5.gz
#                      ...   ...
#                     /NEON.D16.ABBY.IP0.00200.001.ecte.2020-01-05.l0p.h5.gz
#                      ...   ...
#                     /NEON.D16.ABBY.IP0.00200.001.ecte.2020-01-09.l0p.h5.gz
#                /location/ABBY.json
#######################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.sae.ecte.l1l4.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

Para <- NEONprocIS.base::def.arg.pars(arg=arg,
                                      NameParaReqd=c("DirIn","DirOut","DirErr"),
                                      NameParaOptn=c("LOG")
                                      )

# Start logging if not already
log <- NEONprocIS.base::def.log.init(Lvl=Para$LOG)

# Use environment variable to specify how many cores to run on
numCoreUse <- base::as.numeric(Sys.getenv('PARALLELIZATION_INTERNAL'))
numCoreAvail <- parallel::detectCores()
if (base::is.na(numCoreUse)){
  numCoreUse <- 1
}
if(numCoreUse > numCoreAvail){
  numCoreUse <- numCoreAvail
}
log$debug(paste0(numCoreUse, ' of ',numCoreAvail, ' available cores will be used for internal parallelization.'))

# read in json file and get metadata for planar fit 
# set up environment variables, call workflow in eddy4r
# set env in pipelines? see csat3_calibration_assignment, I didn't see how it(LOG_LEVEL) was used in flow.cal.asgn.R

# Retrieve datum input and output path.
DirBgn <- Para$DirIn

# Find all the input paths. We will process each one.
# assumption is files are already joined to the expected path which is year/month/day/SITE
# nested files are under subdirectory data (h5 file) and location (json file) 
nameDirSub <- list('data')

DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn, nameDirSub=nameDirSub, log=log)
log$debug(DirIn)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.sae.ecte.l1l4(DirIn=idxDirIn,
                    DirOutBase=Para$DirOut,
                    log=log
      ),
      error = function(err) {
        call.stack <- base::sys.calls() # is like a traceback within "withCallingHandlers"
        
        # Re-route the failed datum
        NEONprocIS.base::def.err.datm(
          err=err,
          call.stack=call.stack,
          DirDatm=idxDirIn,
          DirErrBase=Para$DirErr,
          RmvDatmOut=TRUE,
          DirOutBase=Para$DirOut,
          log=log
        )
      }
    ),
    # This simply to avoid returning the error
    error=function(err) {}
  )
}