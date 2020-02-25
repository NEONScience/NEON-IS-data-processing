##############################################################################################
#' @title Read NEON calibration XML file

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read in a NEON calibration XML file.

#' @param NameFile String. Name (including relative or absolute path) of json file.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

 
#' @return A data frame of FDAS uncertainty coefficients:\cr
#' \code{Name} Character. Name of the coefficient.\cr
#' \code{Value} Character. Value of the coefficient.\cr
#' \code{.attrs} Character. Relevant attribute (i.e. units)\cr

#' @references Currently none
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
##############################################################################################
def.read.ucrt.coef.fdas <- function(NameFile,log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Open FDAS uncertainty file
  ucrtCoefFdas  <- base::try(rjson::fromJSON(file=NameFile,simplify=TRUE),silent=FALSE)
  if(base::class(ucrtCoefFdas) == 'try-error'){
    # Generate error and stop execution
    log$error(base::paste0('File: ', NameFile, ' is unreadable.')) 
    stop()
  }
  
  # Convert to data frame
  ucrtCoefFdas <- base::lapply(ucrtCoefFdas,base::as.data.frame,stringsAsFactors=FALSE)
  ucrtCoefFdas <- base::do.call(base::rbind,ucrtCoefFdas)
  
  
  return(ucrtCoefFdas)
  
}
