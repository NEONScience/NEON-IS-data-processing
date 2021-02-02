##############################################################################################
#' @title Create output directories

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Create one or more output directories given a starting path and 1 or more relative paths

#' @param DirBgn String value. Starting path. Defaults to NULL, in which case DirSub contains the full absolute or relative path.
#' @param DirSub String value or vector. Path to subdirectories to create relative to starting
#' directory
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A logical vector, where TRUE indicates the directory at the index was created. FALSE if it was not created for any reason.

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none
#' NEONprocIS.base::def.dir.crea(DirBgn='/scratch/pfs/proc_group/prt',DirSub=c('relative/path/to/new/dir','relative/path2/to/new/dir2')

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
#   Cove Sturtevant (2020-09-06)
#     fix error when input ~ as the starting path
#     return TRUE/FALSE for whether directory creation was successful, with better logging
##############################################################################################
def.dir.crea <- function(DirBgn=NULL, DirSub, log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  if (base::is.null(DirBgn) || base::nchar(DirBgn) == 0){
    dirCrea <- base::paste0(DirSub)
  } else {
    dirCrea <- base::paste0(DirBgn, '/', DirSub)
  }
  
  rpt <- base::lapply(dirCrea, base::dir.create, recursive = TRUE)
  rpt <- base::unlist(rpt)
  
  if (base::any(!rpt)){
    log$warn(base::paste0('Did not create directories ', base::paste0(dirCrea[!rpt],collapse = ','),'. This might be because they already exist.'))
  } 
  if(base::any(rpt)){
    log$debug(base::paste0('Successfully created directories ', base::paste0(dirCrea[rpt],collapse = ',')))
  }
  
}
