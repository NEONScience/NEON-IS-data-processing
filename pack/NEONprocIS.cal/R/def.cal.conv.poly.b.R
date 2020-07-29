##############################################################################################
#' @title Convert raw to calibrated data using NEON CVALB polynomial calibration coefficients

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration polynomial function contained in coefficients 
#' CVALB0, CVALB1, CVALB2, etc. to convert raw data to calibrated data.

#' @param data Numeric vector of raw measurements
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml.
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients.
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALB[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case converted data will be retured as NA.
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
#' infoCal <- data.frame(Name=c('CVALB1','CVALB0'),Value=c(10,1),stringsAsFactors=FALSE)
#' def.cal.conv.poly.b(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.m}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-07-28)
#     original creation, from def.cal.conv.poly
##############################################################################################
def.cal.conv.poly.b <- function(data = base::numeric(0),
                              infoCal = NULL,
                              log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  chk <- base::logical(0)
  
  # Check to see if data is a numeric array
  chkNew <-
    NEONprocIS.base::def.validate.vector(data, TestEmpty = FALSE, log = log)
  if (!chkNew) {
    chk <- c(chk, chkNew)
  }
  
  # If infoCal is NULL, return NA data
  if ((is.null(infoCal)) || any (is.na(unlist((infoCal))))) {
    log$warn('No calibration information supplied, returning NA values for converted data.')
    dataConv <- NA * data
    return(dataConv)
  } else {
    # Check to see if infoCal is a list
    chkList <-
      NEONprocIS.base::def.validate.list(infoCal, log = log)
    if (!chkList) {
      chk <- c(chk, chkList)
    }
    
  }
  
  if (!all(chk)) {
    stop()
  }
  
  # Construct the polynomial calibration function
  func <-
    NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALB', log = log)
  
  # Convert data using the calibration function
  dataConv <- stats::predict(object = func, newdata = data)
  
  return(dataConv)
  
}
