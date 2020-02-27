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
restructure_location_repo <- function(DirBgn,
                                      DirOut,
                                      Ctxt = NULL,
                                      Comb) {
  log <- NEONprocIS.base:::def.log.init()
  
  if(base::is.na(Comb)){
    log$fatal(base::paste0('Input argument Comb must be TRUE or FALSE')) 
    stop()
  }
  
  # What are the expected subdirectories of each input path
  nameDirSub <- base::list('location')
  
  # Find all the input paths. We will process each one.
  DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)
  
  
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
        NEONprocIS.base::def.dir.copy.symb(idxDirIn,idxDirOut,log=log)
      }
      
    } # End loop around named locations
  } # End loop around datum paths
}
  