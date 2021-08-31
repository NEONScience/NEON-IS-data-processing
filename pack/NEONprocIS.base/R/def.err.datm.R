##############################################################################################
#' @title Route datum errors to specified location

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Route datum paths that errored to a specified error directory. 
#' The input path to the erroring datum will be created in the error directory.
#' Optionally, remove any partial output from the errored datum.

#' @param DirDatm Character value. The input path to the datum, structured as follows: 
#' #/pfs/BASE_REPO/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs'. Note that the path should terminate at 
#' the directory containing all the data and metadata (nested in further subdirectories 
#' as needed) considered to be one complete entity for processing. 
#' For example:
#' DirDatm = /scratch/pfs/proc_group/prt/2019/01/01/27134
#' @param DirErrBase Character value. The path that will replace the #/pfs/BASE_REPO portion of DirDatm
#' @param RmvDataOut Logical. TRUE to remove any partial output for the datum from the output repo. NOTE:
#' Removing partial output only works if the output datum path matches the typical structure where 
#' DirOutBase replaces the #/pfs/BASE_REPO portion of DirDatm, but otherwise is the same as DirDatm.
#' If this is not the case, set RmvDataOut to FALSE (which is the default).
#' @param DirOutBase Character value. The path that will replace the #/pfs/BASE_REPO portion of 
#' DirIn when writing successful output for the datum. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return The action of creating the path structure to the datum within DirErrBase, having replaced 
#' the #/pfs/BASE_REPO portion of DirDatm with DirErrBase. If RmvDataOut is set to true, any partial
#' output for DirDatm will be removed. The output directory is the same as DirDatm but with the 
#' #/pfs/BASE_REPO portion of DirDatm replaced with DirOutBase.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # Not run:
#' DirDatm <- '/scratch/pfs/proc_group/prt/2019/01/01/27134'
#' DirErrBase <- '/scratch/pfs/proc_group_output/errored_datums'
#' RmvDatmOut <- TRUE
#' DirOutBase <- '/scratch/pfs/proc_group_output'
#' tryCatch(stop('error!'),error=function(err) def.err.datm(DirDatm=DirDatm,DirErrBase=DirErrBase,RmvDatmOut=RmvDatmOut,DirOutBase=DirOutBase))


#' @seealso tryCatch

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-08-10)
#     original creation
##############################################################################################
def.err.datm <- function(
                         DirDatm,
                         DirErrBase,
                         RmvDatmOut=FALSE,
                         DirOutBase=NULL,
                         log=NULL){

  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Make sure the output directory is a character
  if(base::length(DirErrBase) != 1 && !base::is.character(DirErrBase)){
    log$error(base::paste0('DirErrBase (',DirErrBase,') must be a character string.'))
    stop()
  }
  
  # Tell the user about the error routing 
  log$info(
    base::paste0(
      'Re-routing failed datum path ',
      DirDatm,
      ' to ',
      DirErrBase
      )
    )

  # Parse the datum path
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirDatm)
  
  # Create the error path
  NEONprocIS.base::def.dir.crea(DirBgn=DirErrBase, 
                                DirSub=InfoDirIn$dirRepo,
                                log=log)
  
  # Remove any partial output for the datum
  if(RmvDatmOut==TRUE && base::is.null(DirOutBase)){
    # Something isn't right
    log$error(
      base::paste0(
        'Cannot remove partial output for errored datum: ',
        DirDatm,
        ' because input argument DirOut is not specified.'
      )
    )   
  } else if (RmvDatmOut==TRUE){
    # Delete partial output
    dirOutDatm <- base::paste0(DirOutBase, InfoDirIn$dirRepo)
    rpt <- base::unlink(dirOutDatm,recursive=TRUE)
    
    # Report outcome
    switch(base::as.character(rpt),
           '0'=log$info(
             base::paste0(
               'Removed partial output for errored datum: ',
               dirOutDatm
               )
             ),
           '1'=log$info(
             base::paste0(
               'Could not remove partial output for errored datum: ',
               dirOutDatm,
               '. Likely it did not exist.'
               )
             )
    )
  }
  
  return()
}
