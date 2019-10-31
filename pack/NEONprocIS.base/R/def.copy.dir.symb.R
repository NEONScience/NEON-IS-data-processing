##############################################################################################
#' @title Copy all files in a directory with a symbolic link

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Copy all files in a source directory to a destination directory with a symbolic link 

#' @param DirSrc String vector. Source directories. All files in these source directories will be copied to the 
#' destination directories. 
#' @param DirDest String value or vector. Destination director(ies). If not of length 1, must be same length 
#' as DirDest, each index corresponding to the same index of DirDest. NOTE: DirDest should be the parent of the 
#' distination directories. For example, to create a link from source /parent/child/ to /newparent/child, 
#' DirSrc='/parent/child/' and DirDest='/newparent/'

#' @return No output from this function other than performing the intended action.  

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none
#' def.copy.dir.symb(DirSrc='/scratch/pfs/proc_group/prt/27134/2019/01/01',DirDest='pfs/out/prt/27134/2019/01/01')


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-02)
#     original creation
##############################################################################################
def.copy.dir.symb <- function(DirSrc,DirDest){
  
  # Do some error checking 
  numDirSrc <- base::length(DirSrc)
  numDirDest <- base::length(DirDest)
  if(numDirDest != 1 && numDirSrc != numDirDest){
    base::stop('Lengths of DirSrc and DirDest must be equal if length of DirDest is not equal to 1.')
  }

  
  rptDir <- base::lapply(DirDest,base::dir.create,recursive=TRUE) # Create the destination directories
  cmdCopy <- base::paste0('ln -s ',base::paste0(DirSrc),' ',base::paste0(DirDest))
  rptCopy <- base::lapply(cmdCopy,base::system) # Symbolically link the directories
  
}
