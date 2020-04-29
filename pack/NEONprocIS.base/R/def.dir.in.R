##############################################################################################
#' @title Determine input directories based on expected subdirectories

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Determine the input directories (one or more) considered to be individual 
#' datums based on matching the names of child directories. 

#' @param DirBgn String. Directory path considered to be zero or more parents above the input directories.
#' @param nameDirSub String vector. The exact names of expected direct child directories (does not need to 
#' be a complete list of the subdirectories, but all must be found in the input directory for it to be 
#' returned in the output).

#' @return A character vector of input directories (datums)  

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none
#' def.dir.in(DirBgn='/scratch/pfs/proc_group/prt/27134/2019/01/01',nameDirSub=c('data','calibration'))


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-04-30)
#     original creation
#   Cove Sturtevant (2019-10-01)
#     add catch for duplicate nameDirSub
#   Cove Sturtevant (2010-01-30)
#     add log output
##############################################################################################
def.dir.in <- function(DirBgn,nameDirSub,log=NULL){
  #browser()

  # Initialize log if not input
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Get rid of duplicates
  nameDirSub <- base::unique(nameDirSub)
  
  # Get a recursive directory listing
  dirAll <- base::list.dirs(path=DirBgn,full.names = TRUE, recursive = TRUE)
  
  # Separate the directories
  dirAllSplt <- base::strsplit(dirAll,'/',fixed=TRUE)
  dirAllBgn <- base::unlist(base::lapply(dirAllSplt,FUN=function(idxDir){
    base::paste0(utils::head(x=idxDir,n=-1),collapse='/')}))
  dirAllEnd <- base::unlist(base::lapply(dirAllSplt,utils::tail,n=1))
  
  # If there are no subdirectories expected in the datum directory, then each terminal directory is a datum
  if(base::length(nameDirSub) == 0){
    # Find the ones that dont nest in anything else
    setMtch <- base::unlist(base::lapply(dirAll,FUN=function(idxDir){
      base::sum(base::grepl(pattern=idxDir,x=dirAll,fixed=FALSE))==1
    }))
    DirIn <- base::unique(dirAll[setMtch])
    
  } else {
    
    # Filter the list for directories that end with one of our subdirectories
    setMtch <- dirAllEnd %in% nameDirSub
    dirAll <- dirAll[setMtch]
    dirAllBgn <- dirAllBgn[setMtch]
    dirAllEnd <- dirAllEnd[setMtch]
    
    # For each remaining directory, there must be a number of matching parent directories equaling 
    # the length of nameDirSub. If there is, then the parent directory is a datum
    numDirMtch <- base::length(nameDirSub)
    setMtch <- base::unlist(base::lapply(dirAllBgn,FUN=function(idxDir){
      base::sum(dirAllBgn==idxDir)==numDirMtch
    }))
    DirIn <- base::unique(dirAllBgn[setMtch])
    
  }
  
  # Log status
  if(base::length(DirIn) == 0){
    log$warn(base::paste0('No datums found for processing in parent directory ',DirBgn))
  } else {
    log$info(base::paste0(base::length(DirIn),' datums found for processing.'))
  }
  
  return(DirIn)
}
