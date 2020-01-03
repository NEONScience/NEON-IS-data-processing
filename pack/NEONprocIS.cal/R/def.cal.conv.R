##############################################################################################
#' @title Apply NEON calibration coefficients and compute measurement uncertainty

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration coefficients to a data stream and compute
#' uncertainty for individual measurements.

#' @param data Numeric vector of data to apply calibration to
#' @param cal Data frame of calibration coefficients. Must include columns:\cr
#' \code{Name} String. The name of the coefficient. Must fit regular expression CVALA[0-9]\cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param coefUcrtMeas Combined uncertainty of an individual measurement (U_CVALA1) in units of converted value
#' (e.g. temperature). Defaults to NULL, in which case uncertainty will not be calculated.
#' @param coefUcrtFdas Combined, relative FDAS uncertainty of an individual measurement (U_CVALR1 or U_CVALV1, unitless).
#' Defaults to NULL, in which case uncertainty will not be calculated.
#' @param coefUcrtFdasOfst offset imposed by the FDAS for e.g. resistance or voltage readings (U_CVALR4 or U_CVALV4).
#' Defaults to NULL, in which case uncertainty will not be calculated.

#' @return A list of: \cr
#' \code{data} Numeric vector of calibrated data\cr
#' \code{ucrt} A data frame with the following variables:\cr
#' \code{ucrt$ucrtMeas} - combined measurement uncertainty for an individual reading. Includes the
#' repeatability and reproducibility of the sensor and the lab DAS and ii) uncertainty of the
#' calibration procedures and coefficients including uncertainty in the standard (truth).
#' \code{ucrt$derivCal} - 1st derivative of calibration function evaluated at raw reading value (e.g. partial derivative
#' of a temperature measurement with respect to the resistance reading)\cr
#' \code{ucrt$ucrtFdas} - standard uncertainty of individual measurement introduced by the Field DAS \cr
#' \code{ucrt$ucrtComb} - combined, standard, measurement uncertainty for an individual measurement
#'

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-02-26)
#     original creation
#   Cove Sturtevant (2019-10-28)
#     Added computation of uncertainty information
##############################################################################################
def.cal.conv <- function(data,
                         cal,
                         coefUcrtMeas = NULL,
                         coefUcrtFdas = NULL,
                         coefUcrtFdasOfst = NULL) {
  # Reduce cal coefficients to ones we recognize
  
  
  if ((validateVector <- NEONprocIS.cal::def.validate.vector (data))
    & (validateDf <- NEONprocIS.cal::def.validate.df (cal)))
  
  {
    cal <- cal[grep('CVALA[0-9]', cal$Name), ]
    
    # Pull out the numeric polynomial level of each coefficient (a0,a1,a2,...)
    levlPoly <-
      base::as.numeric(base::unlist(base::lapply(
        base::strsplit(cal$Name, 'CVALA', fixed = TRUE),
        FUN = function(vect) {
          vect[2]
        }
      )))
    
    # Initialize vector of polynomial coefficients (a0,a1,a2,...)
    coefPoly <- base::rep(0, max(levlPoly) + 1)
    
    # Place the polynomial coefficients in the right place in our coeficient vector
    coefPoly[levlPoly + 1] <- base::as.numeric(cal$Value)
    
    # Create polynomial function from coefficients
    func <- polynom::polynomial(coef = coefPoly)
    
    # Convert data using polynomial function
    dataConv <- stats::predict(object = func, newdata = data)
    
    
    # Compute derivative of calibration function
    funcDerv <- stats::deriv(func)
    dervCal <-
      stats::predict(object = funcDerv, newdata = data) # Eval derivative at each measurement
    
    # Compute uncertainty
    ucrt <-
      base::data.frame(
        dervCal = dervCal,
        ucrtMeas = coefUcrtMeas,
        ucrtFdas = NA * data,
        ucrtComb = NA * data
      )
    if (base::length(coefUcrtFdas) == 1 &&
        base::length(coefUcrtFdasOfst) == 1) {
      ucrt$ucrtFdas <-
        (coefUcrtFdas * data + coefUcrtFdasOfst) * base::abs(dervCal)
    }
    if (base::length(coefUcrtMeas) == 1) {
      ucrt$ucrtComb <- base::sqrt(coefUcrtMeas ^ 2 + ucrt$ucrtFdas ^ 2)
    }
    
    return(base::list(data = dataConv, ucrt = ucrt))
    }
  else
    cat ("\n ####### Error at cal.conv:  Calibration will not run due to data error moving to the next test\n \n")
}