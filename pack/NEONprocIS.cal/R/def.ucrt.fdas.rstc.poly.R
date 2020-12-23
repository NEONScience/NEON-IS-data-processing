##############################################################################################
#' @title Compute uncertainty attributed to NEON FDAS resistance measurements (and CVALA polynomial calibration conversion)

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Computes L0' uncertainty imposed by a NEON field data acquisition system (FDAS)
#' when making resistance measurements and using a polynomial calibration conversion function with CVALA 
#' coefficients.

#' @param data Data frame of raw measurements. Note that the column/variable indicated in varUcrt must be numeric.
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$cal and info$ucrt,
#' which are data frames of calibration coefficients and uncertainty coeffcients, respectively.
#' Columns of these data frames are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param varUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which FDAS uncertainty data will be computed (all other columns will be ignored). Note that for other
#' uncertainty functions this variable may not need to be in the input data frame, so long as the function
#' knows that. Defaults to the first column in \code{data}.
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.ucrt.dp0p for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrt$raw} - raw reading value (i.e. same as input data)\cr
#' \code{ucrt$dervCal} - 1st derivative of calibration function evaluated at raw reading value (e.g.
#' partial derivative of a temperature measurement with respect to the resistance reading)\cr
#' \code{ucrt$ucrtFdas} - standard uncertainty of individual measurement introduced by the Field DAS \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.func.poly}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-01-31)
#     original creation
#   Cove Sturtevant (2020-05-12)
#     Bug fix - allow code to produce NAs if infoCal$cal is NULL but infoCal$ucrt is not NULL
#   Cove Sturtevant (2020-09-02)
#     adjusted inputs to conform to new generic format 
#     This includes inputting the entire data frame, the 
#     variable to be generate uncertainty info for, and the (unused) argument calSlct
##############################################################################################
def.ucrt.fdas.rstc.poly <- function(data = data.frame(data=base::numeric(0)),
                               varUcrt = base::names(data)[1],
                               calSlct=NULL,
                               infoCal = NULL,
                               log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Ensure input is data frame with the target variable in it
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=varUcrt,TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Check data input is numeric
  if (!NEONprocIS.base::def.validate.vector(data[[varUcrt]],TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
    stop()
  }
  
  # Initialize uncertainty output
  dataUcrt <- data[[varUcrt]] # Target variable to compute uncertainty for
  ucrt <-
    base::data.frame(raw = dataUcrt,
                     dervCal = NA * dataUcrt,
                     ucrtFdas = NA * dataUcrt)
  
  # If infoCal is NULL, return NA data
  if (base::is.null(infoCal$cal)) {
    log$debug('No calibration information supplied, returning NA values for FDAS uncertainty.')
    return(ucrt)
  }
  
  # Check format of infoCal
  if (!NEONprocIS.cal::def.validate.info.cal(infoCal,
                                             CoefUcrt = c('U_CVALR1', 'U_CVALR4'),
                                             log = log)) {
    stop()
  }
  
  # Compute derivative of calibration function
  func <- NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal)
  funcDerv <- stats::deriv(func)
  ucrt$dervCal <-
    stats::predict(object = funcDerv, newdata = dataUcrt) # Eval derivative at each measurement
  
  # Retrieve the uncertainty coefficients for resistance measurements
  # Combined, relative FDAS uncertainty of an individual measurement (U_CVALR1, unitless).
  coefUcrtFdas <-
    base::as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == "U_CVALR1"])
  
  # Check that we have only 1 coefficient to apply
  if(base::length(coefUcrtFdas) > 1){
    log$warn('Multiple "U_CVALR1" coefficients found in the uncertainty coefficients. Using the first encountered.')
    coefUcrtFdas <- coefUcrtFdas[1]
  }
  
  # Offset imposed by the FDAS, in units of resistance (U_CVALR4)
  coefUcrtFdasOfst <-
    base::as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == "U_CVALR4"])
  
  # Check that we have only 1 coefficient to apply
  if(base::length(coefUcrtFdasOfst) > 1){
    log$warn('Multiple "U_CVALR4" coefficients found in the uncertainty coefficients. Using the first encountered.')
    coefUcrtFdasOfst <- coefUcrtFdasOfst[1]
  }
  
  # Compute FDAS uncertainty
  ucrt$ucrtFdas <-
    (coefUcrtFdas * dataUcrt + coefUcrtFdasOfst) * base::abs(ucrt$dervCal)
  
  return(ucrt)
}
