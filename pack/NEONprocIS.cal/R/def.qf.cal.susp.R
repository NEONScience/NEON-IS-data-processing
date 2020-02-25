##############################################################################################
#' @title Quality flag for suspect calibration

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Check the CVALR1 coefficient in the calibration file. If it is present,
#' set the suspect calibration flag for all data values to the value of the coefficient 
#' (1=bad,0=good). If the CVALR1 coefficient is not present, set the suspect calibration flag 
#' to 0. If no calibration information is available, set the flag to -1.

#' @param data Numeric vector of raw measurements
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml. 
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients. 
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient.\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case flag value will be -1. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A numeric vector the same length as input argument "data" of suspect calibration flag 
#' values (1=bad,0=good,-1=could not evaluate).

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' data <- c(1,2,3,4,5)
#' infoCal <- list(cal=data.frame(Name=c('CVALR1','CVALA0'),Value=c(1,0),stringsAsFactors=FALSE))
#' qfSusp <- NEONprocIS.cal::def.qf.cal.susp(data,infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-12)
#     original creation
##############################################################################################
def.qf.cal.susp <- function(data=base::numeric(0),
                              infoCal = NULL,
                              log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Check to see if data is a numeric array
  chk <- NEONprocIS.base::def.validate.vector(data,TestEmpty = FALSE,log=log)
  if (!chk){
    stop()
  }
  
  # Initialize the flag
  qfSusp <- data # 
  qfSusp[] <- -1 # Initialize to "cannot evaluate"
  
  # If infoCal is NULL, return flag. No cal to evaluate.
  if(base::is.null(infoCal)){
    log$debug('No calibration information supplied, returning -1 for suspect calibration flag.')
    return(base::as.integer(qfSusp))
  }

  # Validate calibration information
  if (!NEONprocIS.cal::def.validate.info.cal (infoCal,NameList='cal',log=log)) {
    stop()
  }
  
  # Check for calibration coefficient CVALR1. 
  if('CVALR1' %in% infoCal$cal$Name){
  # If it is present, set the flag to it's value. 
    qfSusp[] <- base::as.numeric(infoCal$cal$Value[infoCal$cal$Name == 'CVALR1'][1])
    
  } else {
  # If it is not present, set the flag to 0.
    qfSusp[] <- 0
    
  }
  
  return(base::as.integer(qfSusp))

}
