##############################################################################################
#' @title Split Pachyderm directory into component folders and interpret date embedded in directory structure

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Parse the directory structure of a Pachyderm repository located in a Docker container into  
#' its components and read the date optionally embedded within the file path. 

#' @param dir String. Directory path (often found as an environment variable named as the input repository), structured as 
#' follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number (including zero) of 
#' parent and child directories of any name, so long as they are not 'pfs' or recognizable as the '/yyyy/mm/dd' structure which 
#' indicates the 4-digit year, 2-digit month, and 2-digit day.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A named list:\cr
#' \itemize{
#'    \item dirSplt = Character vector of the directory parents and children split into separate character strings (parsed by /)\cr
#'    \item repo = Character. The repository name (the child directory of /pfs)\cr
#'    \item idxRepo = Numeric. The index within \code{dirSplt} indicating the position of \code{repo}\cr
#'    \item dirRepo = Character. The directory structure within \code{repo}.\cr
#'    \item time = POSIXct. The /yyyy/mm/dd date (GMT) embedded within the directory structure (if present). NULL if cannot be interpreted.  
#' }

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none
#' def.dir.splt.pach.time('/scratch/pfs/proc_group/prt/27134/2019/01/01')


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-03-18)
#     original creation
#   Cove Sturtevant (2019-05-20)
#     fixed bug causing bad interpretation of repo structure when the terminal directory is the repo
#   Cove Sturtevant (2023-03-08)
#     add error catching when the repo structure does not match expectations
##############################################################################################
def.dir.splt.pach.time <- function(dir, 
                                   log=NULL
 ){
  
 
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Split the directory into parents and children, and parse basic components
  dirSplt <- base::strsplit(dir,'/',fixed=TRUE)[[1]]
  idxRepo <- base::which(dirSplt=='pfs')+1
  if(base::length(idxRepo) == 0){
    log$error('pfs directory not found in input path structure. Check input repo.')
    stop()
  } 
  repo <- dirSplt[idxRepo]
  if(base::is.na(repo)){
    log$error('Cannot determine repo name. Repository structure must conform to .../pfs/repoName/repoContents.... Check input repo.')
    stop()
  }
  dirRepo <- base::paste0(c('',dirSplt[base::seq.int(from=idxRepo+1,length.out=base::length(dirSplt)-idxRepo)]),collapse='/')
  
  # Interpret (if possible) the date embedded within the directory structure
  idxTimeBgn <- base::regexpr(pattern=,'/[0-9]{4}/[0-9]{2}/[0-9]{2}',text=dir)
  if(idxTimeBgn != -1){
    time <- base::as.POSIXct(base::substr(x=dir,start=idxTimeBgn[1]+1,stop=idxTimeBgn[1]+10),format='%Y/%m/%d',tz='GMT')
  } else {
    time <- NULL
  }
  
  
  # Output  
  rpt <- base::list(dirSplt=dirSplt,repo=repo,idxRepo=idxRepo,dirRepo=dirRepo,time=time)
  return(rpt)
}
