##############################################################################################
#' @title Restructure repository from a sensor focus to a location focus. 

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Restructure the repository to a location focus from a sensor focus. 
#' The name of the location and its properties will be read from a location file in the 
#' input repository. The output repository will nest the original sensor-based contents in a folder 
#' named for the location. If there are multiple location names found in the location file, the original 
#' contents will be copied to each one. Optionally, the repo contents 
#' (not file contents) from separate sensors at the same location may be combined into a single
#' folder for the location (this option drops the nested sensor folders).
#'     
#' @param DirIn Character value. The input path to the data from a single sensor ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id, 
#' where # indicates any number of parent and child directories of any name, so long as they are not 
#' 'pfs', 'location',or recognizable as the 'yyyy/mm/dd' structure which indicates the 4-digit year, 
#' 2-digit month, and 2-digit day of the data contained in the folder. The data day is identified from 
#' the input path. The source-id is the unique identifier of the sensor. \cr
#' 
#' Nested within the input path path are the nested folders:\cr
#'         location/  \cr
#' The location folder holds a single json file holding the location data. Any other subfolders 
#' (at the same level of the location folder) are copied to the output untouched.\cr
#' 
#' For example, for a source-id of 27134 and additional (optional) data directory:
#' Input path = /scratch/pfs/proc_group/prt/2019/01/01/27134/ with nested folders:\cr
#'    location/\cr
#'    data/\cr
#'    
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param Comb Boolean. If TRUE, the location name will replace the source id in the repo structure. If FALSE, 
#' the location name will be inserted in the directory path above the level of source-id. Defaults to FALSE. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A restructured repository in DirOutBase, where DirOutBase replaces the input directory structure up to 
#' #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure of the input path 
#' with the exception of inserting the location name in the path according to choices made in Comb. For example, 
#' if Comb is FALSE, DirOutBase is /pfs/out, and following the example in the DirIn argument, the output repo would contain: 
#' /pfs/out/prt/2019/1/01/LOCATION_ID/27134/
#'                                         location/
#'                                         data/
#' where LOCATION_ID is a location ID found in the location file. 
#' If Comb=TRUE, the output repo would contain:
#' /pfs/out/prt/2019/1/01/LOCATION_ID/
#'                                   location/
#'                                   data/
#'  

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.loc.strc.repo(DirIn="/pfs/tempSoil_context_group/prt/2018/01/01",DirOutBase="/pfs/out",Comb=TRUE)

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
#   Cove Sturtevant (2020-07-16)
#     replaced reading of location file with NEONprocIS.base::def.loc.meta
#   Cove Sturtevant (2020-12-18)
#     converted to a wrapper function
##############################################################################################
wrap.loc.repo.strc <- function(DirIn,
                               DirOutBase,
                               Comb=FALSE,
                               log=NULL
                               ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Gather info about the input directory and formulate the parent output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  idSrc <- utils::tail(InfoDirIn$dirSplt,1)
  dirOutPrnt <- base::paste0(c(DirOutBase,InfoDirIn$dirSplt[(InfoDirIn$idxRepo+1):(base::length(InfoDirIn$dirSplt)-1)]),collapse='/')
  
  # Get a list of location files
  dirInLoc <- base::paste0(DirIn,'/location')
  fileLoc <- base::dir(dirInLoc)
  
  # If there is no location file, skip
  numFileLoc <- base::length(fileLoc)
  if(numFileLoc == 0){
    log$warn(base::paste0('No location data in ',dirInLoc,'. Skipping...'))
    return()
  }
  
  # If there is more than one location file, use the first
  if(numFileLoc > 1){
    log$warn(base::paste0('There is more than one location file in ',dirInLoc,'. Using the first... (',fileLoc[1],')'))
    fileLoc <- fileLoc[1]
  }
  
  # Load in the location json
  loc <- NEONprocIS.base::def.loc.meta(NameFile=base::paste0(dirInLoc,'/',fileLoc))
  
  # How many named locations do we have?
  numLoc <- base::nrow(loc)
  # if(numLoc == 0){
  #   log$warn(base::paste0('No named locations listed in ',base::paste0(dirInLoc,'/',fileLoc),'. Skipping...'))
  #   return()
  # }
  
  # Go through each named location, restructuring the repo and copying the sensor data into it
  for(idxLoc in base::seq_len(numLoc)){
    # Get the named location name
    nameLoc <- loc$name[idxLoc]
    
    # Create output repo with location name
    dirOut <- base::paste0(dirOutPrnt,'/',nameLoc)
    NEONprocIS.base::def.dir.crea(DirSub=dirOut,log=log)

    # Copy all folders to our output directory with a symbolic link. We won't be modifying them. 
    if(Comb == TRUE){
      
      # We are going to merge the folder contents across source-ids and get rid of source-id in the repo structure 
      fileCopy <- base::list.files(DirIn,recursive=TRUE) # Files to copy over
      
      # Get the parent directories so we can create them in the main output directory
      dirSub <- base::unique(base::unlist(base::lapply(base::strsplit(fileCopy,'/'),FUN=function(vec){
        base::paste0(utils::head(vec,n=-1),collapse='/')
      })))
      NEONprocIS.base::def.dir.crea(DirBgn=dirOut,DirSub=dirSub,log=log)

      # Symbolically link each file
      for(idxFileCopy in fileCopy){
        cmdCopy <- base::paste0('ln -s ',base::paste0(DirIn,'/',idxFileCopy),' ',base::paste0(dirOut,'/',idxFileCopy))
        rptCopy <- base::system(cmdCopy)
      }
      
      log$info(base::paste0('Restructured path to datum ',DirIn,' to ',dirOut, 
                            '. Merged directory contents with any other source-ids at that named location.'))
    
    } else {
      NEONprocIS.base::def.dir.copy.symb(DirIn,dirOut,log=log)
    }
    
  } # End loop around named locations

}
