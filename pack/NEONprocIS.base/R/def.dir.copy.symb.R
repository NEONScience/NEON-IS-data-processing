##############################################################################################
#' @title Copy a directory with a symbolic link

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Copy a source directory to a destination directory with a symbolic link 

#' @param DirSrc String vector. Source directories. All files in these source directories will be copied to the 
#' destination directories. 
#' @param DirDest String value or vector. Destination director(ies). If not of length 1, must be same length 
#' as DirDest, each index corresponding to the same index of DirDest. NOTE: DirDest should be the parent of the 
#' distination directories. For example, to create a link from source /parent/child/ to /newparent/child, 
#' DirSrc='/parent/child/' and DirDest='/newparent/'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return No output from this function other than performing the intended action.  

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' def.dir.copy.symb(DirSrc='/scratch/pfs/proc_group/prt/27134/2019/01/01',DirDest='pfs/out/prt/27134/2019/01/01')


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-02)
#     original creation
#   Cove Sturtevant (2020-02-04)
#     added logging
##############################################################################################
def.dir.copy.symb <- function(DirSrc,DirDest,log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Do some error checking 
  numDirSrc <- base::length(DirSrc)
  numDirDest <- base::length(DirDest)
  if(numDirDest != 1 && numDirSrc != numDirDest){
    log$error('Lengths of DirSrc and DirDest must be equal if length of DirDest is not equal to 1.')
    base::stop()
  }

  # Check that the source directories exist. Remove those that don't
  exstDirSrc <- base::unlist(base::lapply(DirSrc,base::dir.exists))
  if(!base::all(exstDirSrc)){
    log$warn(base::paste0("The following source directories do not exist and will not be symbolically linked: ",
                          base::paste0(DirSrc[!exstDirSrc],collapse=',')))
    DirSrc <- DirSrc[exstDirSrc]
    
    if(numDirDest > 1){
      DirDest <- DirDest[exstDirSrc]
    }
  }
  
  # Set up the command to copy
  cmdCopy <- base::paste0('ln -s ',base::paste0(DirSrc),' ',base::paste0(DirDest))
  
  # perform symbolic link
  rptDir <- base::suppressWarnings(base::lapply(DirDest,base::dir.create,recursive=TRUE)) # Create the destination directories
  rptCopy <- base::lapply(cmdCopy,base::system) # Symbolically link the directories
  log$info(base::paste0('Unmodified ',base::paste0(DirSrc,collapse=','), ' copied to ',DirDest))

  
}
