##############################################################################################
#' @title Convert raw to calibrated data using NEON CVALM polynomial calibration coefficients

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration polynomial function contained in coefficients 
#' CVALM0, CVALM1, CVALM2, etc. to convert raw data to calibrated data.

#' @param data Numeric data frame of raw measurements. 
#' @param infoCal A list of calibration information as returned from NEONprocIS.cal::def.read.cal.xml.
#' One list element must be \code{cal}, which is a data frame of polynomial calibration coefficients.
#' This data frame must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALM[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' Defaults to NULL, in which case converted data will be retured as NA.
#' @param varConv A character string of the target variable (column) in the data frame \code{data} for 
#' which the calibration will be applied (all other columns will be ignored). Note that for other
#' uncertainty functions this variable may not need to be in the input data frame. Defaults to the first
#' column in \code{data}.
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A  Numeric vector of calibrated data\cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' data=data.frame(data=c(1,2,3))
#' infoCal <- data.frame(Name=c('CVALB1','CVALB0'),Value=c(10,1),stringsAsFactors=FALSE)
#' def.cal.conv.poly.b(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-07-28)
#     original creation, from def.cal.conv.poly
#   Cove Sturtevant (2020-08-31)
#     adjusted inputs to conform to new generic format for all cal funcs
#     This includes inputting the entire data frame, the 
#     variable to be calibrated, and the (unused) argument calSlct
##############################################################################################
def.cal.conv.poly.m <- function(data = data.frame(data=base::numeric(0)),
                                infoCal = NULL,
                                varConv = base::names(data)[1],
                                calSlct=NULL,
                                log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Ensure input is data frame
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=varConv,TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Check to see if data to be calibrated is a numeric array
  chk <-
    NEONprocIS.base::def.validate.vector(data[[varConv]], TestEmpty = FALSE, TestNumc = TRUE, log = log)
  if (!chk) {
    stop()
  }
  
  # If infoCal is NULL, return NA data
  if ((is.null(infoCal)) || any (is.na(unlist((infoCal))))) {
    log$warn('No calibration information supplied, returning NA values for converted data.')
    dataConv <- NA * data[[varConv]]
    return(dataConv)
  } else {
    # Check to see if infoCal is a list
    chkList <-
      NEONprocIS.base::def.validate.list(infoCal, log = log)
    if (!chkList) {
      chk <- c(chk, chkList)
    }
    
  }

  # Construct the polynomial calibration function
  func <-
    NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALM', log = log)
  
  # Convert data using the calibration function
  dataConv <- stats::predict(object = func, newdata = data[[varConv]])
  
  return(dataConv)
  
}
