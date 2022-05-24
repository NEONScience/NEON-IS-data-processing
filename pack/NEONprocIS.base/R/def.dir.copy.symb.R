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
#' @param LnkSubObj Logical (default FALSE). TRUE: Instead of symbolically linking the entire directory DirSrc 
#' to DirDest, create the 'child' directory in DirDest and link each individual object.
#' For example, if DirSrc='/parent/child/' and DirDest='/newparent/', the 'child' directory will be created in 
#' DirDest ('/newparent/child') and each individual object in '/parent/child/' will be symbolically linked in 
#' '/newparent/child'. Note that the output directory structure will be the same regardless of the choice here, 
#' but TRUE is useful for situations in which you want to write additional objects in '/newparent/child/' 
#' either prior to executing this function or afterward. TRUE will allow you to do this, whereas 
#' FALSE will not.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return No output from this function other than performing the intended action.  

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' def.dir.copy.symb(DirSrc='/scratch/pfs/proc_group/prt/27134/2019/01/01',DirDest='pfs/out/prt/27134/2019/01')


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-07-02)
#     original creation
#   Cove Sturtevant (2020-02-04)
#     added logging
#   Cove Sturtevant (2021-06-07)
#     add support for relative paths
#   Cove Sturtevant (2022-02-10)
#     add option to link individual objects with a directory rather the directory itself
##############################################################################################
def.dir.copy.symb <- function(DirSrc,
                              DirDest,
                              LnkSubObj=FALSE,
                              log=NULL){
  
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
  
  # Check for relative paths
  rltvDirSrc <- base::substr(x=DirSrc,start=1,stop=1) != '/'
  rltvDirDest <- base::substr(x=DirDest,start=1,stop=1) != '/'
  
  # Create the parent destination directories
  rptDir <- base::suppressWarnings(base::lapply(DirDest,base::dir.create,recursive=TRUE)) 
  
  if(LnkSubObj==TRUE){
    # Goal: link the individual objects in each DirSrc to dirDest
    
    # Create the terminal directories of DirSrc within DirDest
    nameDirEnd <- base::unlist(
                base::lapply(
                  base::strsplit(DirSrc,split='/'),
                  utils::tail,
                  n=1
                  )
    )
    dirDest <- base::paste0(DirDest,'/',nameDirEnd)
    rptDir <- base::suppressWarnings(base::lapply(dirDest,base::dir.create,recursive=TRUE)) 
    
    # Set up the command to link the individual objects in each DirSrc to dirDest
    cmdCopy <- base::paste0('ln -s ',base::paste0(DirSrc),'/* ',base::paste0(dirDest))

    # Modify the copy command for the relative paths
    setRltv <- rltvDirSrc | rltvDirDest
    cmdCopy[setRltv] <- base::paste0('ln -s -r ',base::paste0(DirSrc[setRltv]),'/* ',base::paste0(dirDest[setRltv]))
    
    # perform symbolic link
    rptCopy <- base::lapply(cmdCopy,base::system) # Symbolically link the directories
    
    # Report success/failure
    sccs <- rptCopy==0
    if(base::any(sccs)){
      log$info(base::paste0('Unmodified objects in ',DirSrc[sccs], ' symbolically linked to ',dirDest[sccs]))
    }
    if(base::any(!sccs)){
      log$warn(base::paste0('At least some of the objects in ',DirSrc[!sccs], ' could not be symbolically linked to ',dirDest[!sccs],
                            '. Permissions may be inadequate or objects with the same name might already exist in the destination directory.'))
    }
  
  } else {
    # Goal: Link the whole directories of DirSrc over to DirDest 
    
    if(base::length(DirDest) == 1){
      dirDest <- base::rep(DirDest,times=base::length(DirSrc))
    } else {
      dirDest <- DirDest
    }

    # Set up the command to copy
    cmdCopy <- base::paste0('ln -s ',base::paste0(DirSrc),' ',base::paste0(dirDest))
    
    # Modify the copy command for the relative paths
    setRltv <- rltvDirSrc | rltvDirDest
    cmdCopy[setRltv] <- base::paste0('ln -s -r ',base::paste0(DirSrc[setRltv]),' ',base::paste0(dirDest[setRltv]))
    
    # perform symbolic link
    rptCopy <- base::lapply(cmdCopy,base::system) # Symbolically link the directories
  
    # Report success/failure
    sccs <- rptCopy==0
    if(base::any(sccs)){
      log$info(base::paste0('Unmodified ',DirSrc[sccs], ' symbolically linked into ',dirDest[sccs]))
    }
    if(base::any(!sccs)){
      log$warn(base::paste0(DirSrc[!sccs], ' could not be symbolically linked into ',dirDest[!sccs],
                            '. Permissions may be inadequate or objects with the same name might already exist in the destination directory.'))
    }
  }
  
}
