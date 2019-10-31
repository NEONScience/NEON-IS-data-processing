##############################################################################################
#' @title Threshold filtering module (by term and/or context) for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Threshold filtering module for NEON IS data processing. Applies a filter for 
#' a specific term and/or context. 
#' 
#' This script is run at the command line with 4 arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the path to the input data directory. 
#' The input path is structured as follows: #/pfs/BASE_REPO/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs'. All files in this 
#' path will be filtered. DO NOT ENTER THE FULL PATH TO A SPECIFIC FILE. ENTER THE PARENT DIRECTORY.
#'   
#' For example:
#' Input path = /scratch/pfs/proc_group/soilprt/27134/2019/01/01 
#'    
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "Term=value" (optional), where value is the term to filter by. Use pipes (|) to separate 
#' multiple term strings (e.g. "PRTResistance|windspeed". See documentation for def.thsh.json.filt for logic 
#' applied when selecting/combining multiple terms with or without context.
#' 
#' 4. "Ctxt=value" (optional), where the value is context to filter by. Use pipes (|) to separate 
#' multiple context strings (e.g. "soil|water". See documentation for def.thsh.json.filt for logic applied 
#' when selecting/combining multiple contexts with or without term.
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Filtered threshold json files in DirOut, where the the terminal directory of DirOut
#' replaces BASE_REPO but otherwise retains the child directory structure of the input path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.thsh.filt.R "DirIn=/pfs/prt_calibration/prt/2019/01/01" "DirOut=/pfs/out" "Term=resistance|PRTResistance" "Ctxt=soil"

#' @seealso \code{\link[NEONprocIS.qaqc]{def.thsh.json.filt}}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-05-20)
#     original creation
#   Cove Sturtevant (2019-09-30)
#     re-structured inputs to be more human readable
#     added argument for output directory
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut"),NameParaOptn=c("Term","Ctxt"),log=log)

# Retrieve datum path. 
DirIn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirIn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Base output directory: ',DirOut))

# Retrieve Term & Context values
Term <- Para$Term
Ctxt <- Para$Ctxt

log$info(base::paste0('Filtering thresholds for Term = ',base::paste0(Term,collapse=','), ', Context = ', base::paste0(Ctxt,collapse=',')))

log$info(base::paste0('Processing path to datum: ',DirIn))
  
# Get directory listing of input directory
file <- base::dir(DirIn)
  
# Gather info about the input directory (including date, if present) and create the output directory. 
InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
DirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
base::dir.create(DirOut,recursive=TRUE)
  
if(base::length(file) > 1){
  log$warn(base::paste0('There are is more than one threshold file in path: ',DirIn,'... Filtering them all!'))
}

# Filter each available file
for(idxFile in file){
  
  # Construct file names
  fileIn <- base::paste0(DirIn,'/',idxFile)
  log$info(base::paste0('Processing file: ',fileIn))
  fileOut <- base::paste0(DirOut,'/',idxFile)  
  
  # Filter the thresholds
  NEONprocIS.qaqc::def.thsh.json.filt(NameFile=fileIn,NameFileOut=fileOut,Term=Term,Ctxt=Ctxt)
  log$info(base::paste0('Filtered thresholds written in: ',fileOut))
  
}
