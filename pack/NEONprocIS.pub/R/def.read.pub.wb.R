##############################################################################################
#' @title Read Publication Workbook

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read in one or more tab-delimited publication workbooks. 

#' @param NameFile Character array. Name(s) (including relative or absolute path) of publication workbook file(s).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A data frame with all input publication workbooks combined.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' pubWb <- NEONprocIS.base::def.read.pub.wb(NameFile=c('/scratch/mypubWb1.txt','/scratch/mypubWb2.txt'))

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-02-20)
#     original creation
##############################################################################################
def.read.pub.wb <- function(NameFile,
                            log=NULL
){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  tryCatch({
    PubWbList <- base::lapply(FilePubWb,utils::read.delim)
    pubWb <- base::do.call(base::rbind,PubWbList)
    
    log$debug(base::paste0('Successfully read in publication workbook: ',NameFile, ' to data frame.'))
  },
    error=function(err) {
      log$error(base::paste0('One or more of the publication workbooks could not be loaded. Check input file(s).'))
      stop(err)
    }
  )
  
  return(pubWb)
}
