##############################################################################################
#' @title Compute individual measurement uncertainty using coefficients in NEON calibration file

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Accepts a L0 data vector and NEON uncertainty information as produced
#' by NEONprocIS.cal::def.read.cal.xml and returns a vector of individual measurement
#' uncertainties for each data value. Note that all uncertainty functions must take inputs of
#' data and infoCal (see inputs) to be used within the generic calibration module.

#' @param data Numeric vector of raw measurements
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with the following variables:\cr
#' \code{ucrtMeas} - combined measurement uncertainty for an individual reading. Includes the
#' repeatability and reproducibility of the sensor and the lab DAS and ii) uncertainty of the
#' calibration procedures and coefficients including uncertainty in the standard (truth).

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty

#' @examples
#' data <- c(1,6,7,0,10)
#' infoCal <- list(ucrt = data.frame(Name=c('U_CVALA1','U_CVALA3'),Value=c(0.1,5),stringsAsFactors=FALSE))
#' def.ucrt.meas(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt}
#' @seealso \link[NEONprocIS.base]{def.log.init}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-03)
#     original creation
##############################################################################################
def.ucrt.meas <- function(data, infoCal, log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Check inputs
  if (!NEONprocIS.base::def.validate.vector(data)) {
    stop()
  }
  if (!NEONprocIS.cal::def.validate.info.cal(infoCal,coefUcrt='U_CVALA1',log=log)){
    stop()
  }
  
  # Initialize output data frame
  ucrt <- base::data.frame(ucrtMeas = NA * data)
  
  # Uncertainty coefficient U_CVALA1 represents the combined measurement uncertainty for an
  # individual reading. It includes the repeatability and reproducibility of the sensor and the
  # lab DAS and ii) uncertainty of the calibration procedures and coefficients including
  # uncertainty in the standard (truth).
  ucrtCoef <- infoCal$ucrt[infoCal$ucrt$Name == 'U_CVALA1',]
  
  # Issue warning if more than one matching uncertainty coefficient was found
  if(base::nrow(ucrtCoef) > 1){
    log$warn("More than one matching uncertainty coefficient was found for U_CVALA1. Using the first.")
  }
  
  # The individual measurement uncertainty is just U_CVALA1 for each measurement
  ucrt$ucrtMeas[] <- base::as.numeric(ucrtCoef$Value[1])
  
  return(ucrt)
  
}
