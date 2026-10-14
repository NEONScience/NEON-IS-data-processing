##############################################################################################
#' @title Determine input directories based on partial names of expected subdirectories

#' @author 
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description 
#' Definition function. Determine the input directories (one or more) considered to be individual 
#' datums based on matching the names of child directories. 

#' @param DirBgn String. Directory path considered to be zero or more parents above the input directories.
#' @param nameDirSubPartial String vector. The partial names of expected direct child directories (does not need to 
#' be a complete list of the subdirectories, but all must be found in the input directory for it to be 
#' returned in the output).

#' @return A character vector of input directories (datums)  

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Nora Catolico (2026-10-14)
#     original creation based on def.dir.in.R
##############################################################################################
def.dir.in.partial<-function (DirBgn, nameDirSubPartial, log = NULL) 
{
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  nameDirSubPartial <- base::unique(nameDirSubPartial)
  dirAll <- base::list.dirs(path = DirBgn, full.names = TRUE, 
                            recursive = TRUE)
  dirAllSplt <- base::strsplit(dirAll, "/", fixed = TRUE)
  dirAllBgn <- base::unlist(base::lapply(dirAllSplt, FUN = function(idxDir) {
    base::paste0(utils::head(x = idxDir, n = -1), collapse = "/")
  }))
  dirAllEnd <- base::unlist(base::lapply(dirAllSplt, utils::tail, 
                                         n = 1))
  if (base::length(nameDirSubPartial) == 0) {
    setMtch <- base::unlist(base::lapply(dirAllSplt, FUN = function(idxDirSplt) {
      base::sum(base::unlist(base::lapply(dirAllSplt, 
                                          FUN = function(idxDirAllSplt) {
                                            base::all(idxDirSplt %in% idxDirAllSplt)
                                          }))) <= 1
    }))
    DirIn <- base::unique(dirAll[setMtch])
  }
  else {
    dirParent <- base::unique(dirAllBgn)
    setMtch <- base::vapply(dirParent, FUN = function(idxDirParent) {
      base::all(base::vapply(nameDirSubPartial, FUN = function(idxNamePartial) {
        base::any(dirAllBgn == idxDirParent & grepl(idxNamePartial, dirAllEnd))
      }, FUN.VALUE = base::logical(1)))
    }, FUN.VALUE = base::logical(1))
    DirIn <- dirParent[setMtch]
  }
  if (base::length(DirIn) == 0) {
    log$warn(base::paste0("No datums found for processing in parent directory ", 
                          DirBgn))
  }
  else {
    log$info(base::paste0(base::length(DirIn), " datums found for processing. 
                          ",DirIn))
  }
  return(DirIn)
}