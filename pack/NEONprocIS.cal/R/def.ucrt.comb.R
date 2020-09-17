##############################################################################################
#' @title Combine measurement uncertainties by adding them in quadrature.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Reads in a data frame of uncertainties and adds each row in quadrature.
#' NA values are NOT ignored.

#' @param ucrt Numeric data frame of uncertainties to be combined.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with a single numeric column:\cr
#' \code{ucrtComb} - combined measurement uncertainty resulting by adding in quadrature all the
#' uncertainties provided in the input data frame \code{ucrt}.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords combined uncertainty

#' @examples
#' ucrt <- data.frame(ucrtA=c(1,2,1,1,2),ucrtB=c(5,6,7,8,9),stringsAsFactors=FALSE)
#' NEONprocIS.cal::def.ucrt.comb(data=data,ucrtCoef=ucrtCoef)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.cnst}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.mult}

#' @seealso \link[NEONprocIS.base]{def.log.init}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-03)
#     original creation
##############################################################################################
def.ucrt.comb <- function(ucrt, log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Error check
  if (!NEONprocIS.base::def.validate.dataframe(ucrt,
                                               TestNa = FALSE,
                                               TestNumc = TRUE,
                                               log = log)) {
    stop()
  }
  
  # Combine uncertainties
  ucrtComb <- base::sqrt(base::rowSums(x = ucrt^2, na.rm = FALSE))
  
  # Create data frame
  rpt <- base::data.frame(ucrtComb = ucrtComb, stringsAsFactors = FALSE)
  
  return(rpt)
  
}
