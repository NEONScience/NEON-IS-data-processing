##############################################################################################
#' @title Restructure repository from a sensor focus to a location focus. 

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Restructure the repository to a location focus (from a sensor focus). 
#' The name of the location and its properties will be read from a location file in the 
#' input repository. The output repository will nest the original sensor-based contents in a folder 
#' named for the location. If there are multiple location names found in the repository, the original 
#' contents will be copied to each one. Optionally, the output repository may be filtered for 
#' locations matching a context property specified in the input arguments, and the repo contents (not 
#' file contents) from separate sensors at the same location may be combined.
#' 
#' This script is run at the command line with 4 arguments. Each argument must be a string in the 
#' format "Para=value", where "Para" is the intended parameter name and "value" is the value of the 
#' parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter 
#' will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the input path, structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, 
#' where # indicates any number of parent and child directories of any name, so long as they are not 
#' 'pfs', 'location',or recognizable as the 'yyyy/mm/dd' structure which indicates the 4-digit year, 
#' 2-digit month, and 2-digit day of the data contained in the folder. The data day is identified from 
#' the input path. 
#' 
#' Nested within the input path path are the nested folders:
#'         source-id/location  
#' The source-id is the unique identifier of the sensor. Note that the input directory MUST be at least 
#' one parent above the source-id directory.
#' The location folder holds a single json file holding the location data. Any other subfolders 
#' (at the same level of the location folder) are carried through untouched.
#' 
#' For example, for a source-id of 27134 and additional (optional) data directory:
#' Input path = /scratch/pfs/proc_group/prt/2019/01/01/27134 with nested folders:
#'    /location 
#'    /data
#'    
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' 3. "Ctxt=value" (optional), where the value is a string specifying one or more context properties that each 
#' location must match (all of them) in order to be included in the output directory. Separate multiple contexts 
#' with a pipe (|). For example "soil|aspirated-triple".
#' 
#' 4. "Comb=value" (optional), where value is either TRUE or FALSE whether to merge the contents of the nested 
#' sensor-based directories that reside at the same location ID. Defaults to FALSE. If TRUE, the location name 
#' will replace the source id in the repo structure, and the contents of subfolders will be combined across 
#' source-ids. If FALSE, the location name will be inserted in the directory path above the level of source-id.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return A restructured repository in DirOut, where DirOut replaces the input directory structure up to 
#' #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure of the input path. 
#' with the exception of inserting the location name in the path according to choices made in Comb. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.loc.strc.repo.R "DirIn=/pfs/tempSoil_context_group/prt/2018/01/01" "DirOut=/pfs/out"  "Ctxt=soil" "Comb=TRUE"
#' 
#' Using environment variable for input directory
#' Sys.setenv(DIR_IN='/pfs/tempSoil_context_group/prt/2018/01/01')
#' Rscript flow.loc.strc.repo.R "DirIn=$DIR_IN" "DirOut=/pfs/out"  "Ctxt=soil" "Comb=TRUE"

#' @seealso None
#' 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-10)
#     original creation
#   Cove Sturtevant (2019-09-12)
#     added reading of input path from environment variables
#     simplified fatal errors to not stifle the R error message
#   Cove Sturtevant (2019-09-26)
#     re-structured inputs to be more human readable
#     added argument for output directory 
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut"),NameParaOptn=c("Ctxt","Comb"),
                                      TypePara=base::list(Comb="logical"),log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Context(s)
Ctxt <- Para$Ctxt
log$debug(base::paste0('Context(s) to filter locations by: ',base::paste0(Ctxt,collapse=",")))

# Merge contents (TRUE/FALSE)
Comb <- Para$Comb
if(base::is.null(Comb)){
  Comb <- FALSE
}
source("./location_repo_strc.R")
restructure_location_repo (DirBgn, DirOut, Ctxt, Comb)


