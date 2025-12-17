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
    setMtch <- grepl(nameDirSubPartial,dirAllEnd)
    dirAll <- dirAll[setMtch]
  }
  if (base::length(dirAll) == 0) {
    log$warn(base::paste0("No datums found for processing in parent directory ", 
                          DirBgn))
  }
  else {
    log$info(base::paste0(base::length(dirAll), " datums found for processing. 
                          ",dirAll))
  }
  return(dirAll)
}