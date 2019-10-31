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
log$debug(base::paste0('Merge/combine the directory contents of source-ids at the same location-id is set to: ', Comb))
if(base::is.na(Comb)){
  log$fatal(base::paste0('Input argument Comb must be TRUE or FALSE')) 
  stop()
}

# What are the expected subdirectories of each input path
nameDirSub <- base::list('location')

# Find all the input paths. We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub)

if(base::length(DirIn) == 0){
  log$warn(base::paste0('No datums found for processing in parent directory ',DirBgn))
} else {
  log$info(base::paste0('Preparing to process ',base::length(DirIn),' datums.'))
}

# Process each datum
for(idxDirIn in DirIn){
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory and formulate the parent output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  idSrc <- utils::tail(InfoDirIn$dirSplt,1)
  idxDirOutPrnt <- base::paste0(c(DirOut,InfoDirIn$dirSplt[(InfoDirIn$idxRepo+1):(base::length(InfoDirIn$dirSplt)-1)]),collapse='/')
  
  # Get a list of location files
  idxDirInLoc <- base::paste0(idxDirIn,'/location')
  fileLoc <- base::dir(idxDirInLoc)
  
  # If there is no location file, skip
  numFileLoc <- base::length(fileLoc)
  if(numFileLoc == 0){
    log$warn(base::paste0('No location data in ',idxDirInLoc,'. Skipping...'))
    next()
  }
  
  # If there is more than one location file, use the first
  if(numFileLoc > 1){
    log$warn(base::paste0('There is more than one location file in ',idxDirInLoc,'. Using the first... (',fileLoc[1],')'))
    fileLoc <- fileLoc[1]
  }
  
  # Load in the location json
  loc <- rjson::fromJSON(file=base::paste0(idxDirInLoc,'/',fileLoc),simplify=FALSE)
  
  # How many named locations do we have?
  numLoc <- base::length(loc$features)
  if(numLoc == 0){
    log$warn(base::paste0('No named locations listed in ',base::paste0(idxDirInLoc,'/',fileLoc),'. Skipping...'))
    next()
  }
  
  # Go through each named location, restructuring the repo and copying the sensor data into it
  for(idxLoc in base::seq_len(numLoc)){
    # Get the named location name and any context
    nameLoc <- loc$features[[idxLoc]]$properties$name
    ctxtLoc <- base::unlist(loc$features[[idxLoc]]$properties$context)
    
    # Check for context match
    if(base::sum(Ctxt %in% ctxtLoc)!=base::length(Ctxt)){
      log$warn(base::paste0('Context properties "', base::paste0(ctxtLoc,collapse=','), '" of named location ',
                            nameLoc, ' found in location file ', base::paste0(idxDirInLoc,'/',fileLoc), 
                            ' do not contain a match for required context(s) "',base::paste0(Ctxt,collapse=','),
                            '". Excluding this named location from output repo...'))
      next()
    }
    
    # Create output repo with location name
    idxDirOut <- base::paste0(idxDirOutPrnt,'/',nameLoc)
    base::suppressWarnings(base::dir.create(idxDirOut,recursive=TRUE))
    
    # Copy all folders to our output directory with a symbolic link. We won't be modifying them. 
    if(Comb == TRUE){
      
      # We are going to merge the folder contents across source-ids and get rid of source-id in the repo structure 
      fileCopy <- base::list.files(idxDirIn,recursive=TRUE) # Files to copy over
      
      # Get the parent directories so we can create them in the main output directory
      idxDirSub <- base::unique(base::unlist(base::lapply(base::strsplit(fileCopy,'/'),FUN=function(vec){
        base::paste0(utils::head(vec,n=-1),collapse='/')
      })))
      idxDirOutSub <- base::paste0(idxDirOut,'/',idxDirSub)
      rptDir <- base::suppressWarnings(base::lapply(idxDirOutSub,base::dir.create,recursive=TRUE)) # Create subdirectories
      
      
      # Symbolically link each file
      for(idxFileCopy in fileCopy){
        cmdCopy <- base::paste0('ln -s ',base::paste0(idxDirIn,'/',idxFileCopy),' ',base::paste0(idxDirOut,'/',idxFileCopy))
        rptCopy <- base::system(cmdCopy)
      }
      
      log$info(base::paste0('Restructured path to datum ',idxDirIn,' to ',idxDirOut, 
                            '. Merged directory contents with any other source-ids at that named location.'))
    
    } else {
      base::suppressWarnings(NEONprocIS.base::def.copy.dir.symb(idxDirIn,idxDirOut))
      log$info(base::paste0('Restructured path to datum ',idxDirIn,' to ',base::paste0(idxDirOut,'/',idSrc)))
    }
    
  } # End loop around named locations
} # End loop around datum paths

