#######################################################
#' @title Call ECTE L1L4 workflow from eddy4r image.
# This is an experiment to see how Pachyderm handles large image.
# input would be l0p h5 file and location file at site level.
# the location file is faked now, its format is not real, the json parser also need to change later
# the current structure already group location file and h5 file
# a join should be done before this
# consider to join following two repos:
# turbulent_l0p/2020/01/01/ABBY/abby.l0p.h5.gz
# location_site/2020/01/01/ABBY/abby.json
#######################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.sae.ecte.l1l4.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

Para <- NEONprocIS.base::def.arg.pars(arg=arg,
                                      NameParaReqd=c("DirIn","DirOut","DirTmp", "DirErr"),
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
DirTmp <- Para$DirTmp # directory to hold ECTE l0p files from all (9) days

# Find all the input paths. We will process each one.
# assumption is files are already joined to the expected path which is year/month/day/SITE
# data (h5 file) and metadata (json file) are at the same level under SITE
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn, nameDirSub=NULL, log=log)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(paste0('Processing path to datum: ', idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.sae.ecte.l1l4(DirIn=idxDirIn,
                    DirOutBase=Para$DirOut,
                    DirTmp=DirTmp,
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