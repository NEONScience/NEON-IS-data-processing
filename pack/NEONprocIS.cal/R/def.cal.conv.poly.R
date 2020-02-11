##############################################################################################
#' @title Convert raw to calibrated data using NEON polynomial calibration coefficients

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration polynomial function to convert raw data to calibrated data.

#' @param data Numeric vector of raw measurements
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml. 
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients. 
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALA[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A  Numeric vector of calibrated data\cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' data=c(1,2,3)
#' cal <- data.frame(Name=c('CVALA1','CVALA0'),Value=c(10,1),stringsAsFactors=FALSE)
#' def.cal.conv(data=data,cal=cal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-02-26)
#     original creation
#   Cove Sturtevant (2019-10-28)
#     Added computation of uncertainty information
#   Mija Choi (2020-01-07)
#     Added parameter validations and logging
#   Cove Sturtevant (2020-01-31)
#     Removed uncertainty quantification (moved to separate function)
#     Split out creation of the polynomial model object into a function
##############################################################################################
def.cal.conv.poly <- function(data,
                              infoCal,
                              log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  chk <- base::logical(0)
  
  # Check to see if data is not empty 
  chkNew <- NEONprocIS.base::def.validate.vector (data,log=log)
  if (!chk0){
    chk <- c(chk,chkNew)
  }
  
  # Validate calibration information
  chkNew <- NEONprocIS.cal::def.validate.info.cal(infoCal=infoCal,NameList='cal',log=log)
  if (!chk0){
    chk <- c(chk,chkNew)
  }
  
  if(!all(chk)){
   on.exit() 
  }
  else {
    # Construct the polynomial calibration function
    func <- NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal,log=log)
    
    # Convert data using the calibration function
    dataConv <- stats::predict(object = func, newdata = data)
    
    return(dataConv)
  }
}
