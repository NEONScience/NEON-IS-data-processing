##############################################################################################
#' @title Compute uncertainty attributed to NEON FDAS voltage measurements

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Computes uncertainty imposed by a NEON field data acquisition system (FDAS)
#' when making voltage measurements.

#' @param data Numeric vector of raw voltage measurements
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$cal and info$ucrt,
#' which are data frames of calibration coefficients and uncertainty coeffcients, respectively.
#' Columns of these data frames are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrt$raw} - raw reading value (i.e. same as input data)\cr
#' \code{ucrt$dervCal} - 1st derivative of calibration function evaluated at raw reading value (e.g. partial derivative
#' of a radiation measurement with respect to the voltage reading)\cr
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
#   Cove Sturtevant (2020-02-04)
#     original creation
##############################################################################################
def.ucrt.fdas.volt <- function(data = base::numeric(0),
                               infoCal = NULL,
                               log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Check data input
  if (!NEONprocIS.base::def.validate.vector(data, TestEmpty = FALSE, log =
                                            log)) {
    stop()
  }
  
  # Initialize uncertainty output
  ucrt <-
    base::data.frame(raw = data,
                     dervCal = NA * data,
                     ucrtFdas = NA * data)
  
  # If infoCal is NULL, return NA data
  if (base::is.null(infoCal)) {
    log$debug('No calibration information supplied, returning NA values for FDAS uncertainty.')
    return(ucrt)
  }
  
  # Check format of infoCal
  if (!NEONprocIS.cal::def.validate.info.cal(infoCal,
                                             CoefUcrt = c('U_CVALV1', 'U_CVALV4'),
                                             log = log)) {
    stop()
  }
  
  
  # Compute derivative of calibration function
  func <- NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal)
  funcDerv <- stats::deriv(func)
  ucrt$dervCal <-
    stats::predict(object = funcDerv, newdata = data) # Eval derivative at each measurement
  
  # Retrieve the uncertainty coefficients for voltage measurements
  # Combined, relative FDAS uncertainty of an individual measurement (U_CVALV1, unitless).
  coefUcrtFdas <-
    base::as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == "U_CVALV1"])
  
  # Offset imposed by the FDAS, in units of voltage (U_CVALV4)
  coefUcrtFdasOfst <-
    base::as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == "U_CVALV4"])
  
  # Compute FDAS uncertainty
  ucrt$ucrtFdas <-
    (coefUcrtFdas * data + coefUcrtFdasOfst) * base::abs(ucrt$dervCal)
  
  return(ucrt)
}
