##############################################################################################
#' @title Create output directories

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Create one or more output directories given a starting path and 1 or more relative paths

#' @param DirBgn String value. Starting path
#' @param DirSub String value or vector. Path to subdirectories to create relative to starting
#' directory
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return No output from this function other than performing the intended action.

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none
#' NEONprocIS.base::def.dir.crea(DirBgn='/scratch/pfs/proc_group/prt',DirSub=c('relative/path/to/new/dir','relative/path2/to/new/dir2')

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
##############################################################################################
def.dir.crea <- function(DirBgn, DirSub, log = log) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  dirCrea <- base::paste0(DirBgn, '/', DirSub)
  rpt <- base::lapply(dirCrea, base::dir.create, recursive = TRUE)
  log$debug(base::paste0('Created directories ', base::paste0(dirCrea[base::unlist(rpt)],
                                                              collapse = ',')))
  
}
