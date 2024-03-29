##############################################################################################
#' @title Threshold selection module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Threshold selection module for NEON IS data processing. The choice of 
#' constraint/threshold to use is determined by moving up the following hierarchy 
#' from finer to coarser constraints until one applies. Thus, the finest applicable level of constraint 
#' is chosen. Threshold selection order is as follows (1 being the finest possible contraint): 
#' 6. Realm, annual
#' 5. Realm, seasonal
#' 4. Site-specific, annual
#' 3. Site-specific, seasonal
#' 2. Sensor-spefific, annual
#' 1. Sensor-specific, seasonal
#' 
#' This script is run at the command line with 4 or more arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the path to input data directory.
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder. This 
#' date will be used in the threshold selection process.
#' 
#' Below this path is a directory named for the location identifier of the data included 
#' within it. (e.g. #/pfs/BASE_REPO/#/yyyy/mm/dd/#/CGFLOC12345/). The location identifier will be matched 
#' against the location information supplied in the location file(s) (see below). Nested exactly one level
#' deep within the location identifier folder is the folder:
#'         location/ 
#' The location folder holds at least 1 json file with location data/properties specific to the location 
#' identifier in the directory path. If there is more than one file in this directory, only the first will 
#' be used, since the properties of the named location (i.e. site) should be the same across files. 
#'    
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "DirErr=value", where the value is the output path to place the path structure of errored datums that will 
#' replace the #/pfs/BASE_REPO portion of DirIn.
#' 
#' 4. "FileThsh=value", where value is the full path to the thresholds file from which to select thresholds. 
#' 
#' 5-N. "TermCtxtX=value", where X is a number beginning at 1 and value is a term-context group corresponding 
#' to QA/QC thresholds for the type of data at the named location defined in the location directory. Each 
#' term-context group is a single argument, where the term is listed first followed by any applicable context 
#' strings, separated by pipes. There may be multiple assignments of TermCtxtX, specified by incrementing the 
#' number X by 1 with each additional argument. For example, a 3-argument set of term-context groups could be: 
#' "TermCtxt1=resistance|soil|deep" "TermCtxt2=windspeed|air" "TermCtxt3=temperature". In the first case, 
#' thresholds will be selected for the term "resistance" and matching both context strings "soil" and "deep". 
#' The second argument indicates selection of thresholds for term "windspeed" and matching context string 
#' "air". The third argument indicates selection of thresholds for term "temperature" without context. 
#' Thresholds will be selected for all 3 and placed in the same file. TermCtxt1 must be an input, and there is
#' a limit of X=100 for additional TermCtxtX arguments. Be sure there is no overlap between 
#' term-context groups, as you will have to reapply the logic to select appropriate thresholds for each at a 
#' later time.
#' 
#' N+1. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the location folder in the input path that are to be copied with a 
#' symbolic link to the output path.
#' 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return A directory structure created in DirOut, where the terminal directory of DirOut 
#' replaces BASE_REPO but otherwise retains the child directory structure of the input path. The filtered 
#' threshold file will be placed in an additional subdirectory called 'thresholds' at the same level as the 
#' location directory. Be sure to specify "DirSubCopy=location" to pass the location directory through
#' to the output (along with any other desired subdirectories to copy over. Otherwise, they will be dropped. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.thsh.slct.R "DirIn=/pfs/tempSoil_locations/prt/2020/01/01" "DirOut=/pfs/out" "DirErr=/pfs/out/errored_datums" FileThsh=/pfs/tempSoil_threshold_filter/thresholds.json" "TermCtxt1=temp|soil" "DirSubCopy=location"

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-11)
#     original creation
#   Cove Sturtevant (2019-10-01)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
#   Cove Sturtevant (2021-12-16)
#     Move main functionality to wrapper function and add error routing
##############################################################################################
library(foreach)
library(doParallel)

# Source the wrapper function. Assume it is in the working directory
source("./wrap.thsh.slct.R")

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Start logging
log <- NEONprocIS.base::def.log.init()

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

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,
                                      NameParaReqd=c("DirIn",
                                                     "DirOut",
                                                     "DirErr",
                                                     "FileThsh",
                                                     "TermCtxt1"
                                                     ),
                                      NameParaOptn=c(base::paste0("TermCtxt",2:100),
                                                     "DirSubCopy"),
                                      log=log
                                      )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0('Error directory: ', Para$DirErr))

# Load the thresholds
log$debug(base::paste0('Threshold file to select from: ',Para$FileThsh))
thshRaw <- base::try(rjson::fromJSON(file=Para$FileThsh,simplify=TRUE),silent=FALSE)
if(base::class(thshRaw) == 'try-error'){
  # Generate error and stop execution
  log$fatal(base::paste0('Threshold file ', Para$FileThsh, ' is unreadable or contains no data. Aborting...')) 
  stop()
}
thshRaw <- thshRaw$thresholds
thshPosx <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(listThsh=thshRaw,log=log) # Turns dates to posixct
log$debug(base::paste0('Successfully loaded ',length(thshPosx), ' thresholds from ', Para$FileThsh)) 

# Retrieve the term-context groups we are going to select thresholds for. 
# These are input as subsequent arguments with term and context strings separated by pipes. 
nameParaTermCtxt <- base::names(Para)[names(Para) %in% base::paste0("TermCtxt",1:100)]
numTermCtxt <- base::length(nameParaTermCtxt)
ParaThsh <- base::list()
for(idx in base::seq_len(numTermCtxt)){
  splt <- Para[[nameParaTermCtxt[idx]]]
  ParaThsh[[idx]] <- base::list(Term=splt[1],Ctxt=NULL) # The term
  numSplt <- base::length(splt)
  if(numSplt > 1){
    ParaThsh[[idx]]$Ctxt <- splt[2:numSplt]
  }
}
log$debug(base::paste0('Term-context groupings to select thresholds for: ',base::paste0(ParaThsh,collapse=",")))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(Para$DirSubCopy)
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,'location')))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths. We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=Para$DirIn,nameDirSub=nameDirSub,log=log)


# Process each file path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Run the wrapper function for each datum, with error routing
  tryCatch(
    withCallingHandlers(
      wrap.thsh.slct(DirIn=idxDirIn,
                     DirOutBase=Para$DirOut,
                     thshRaw=thshRaw,
                     thshPosx=thshPosx,
                     ParaThsh=ParaThsh,
                     DirSubCopy=DirSubCopy,
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
  
  
  return()
  
} # End loop around datum paths
