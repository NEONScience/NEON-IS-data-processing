##############################################################################################
#' @title Compute expanded measurement uncertainty (95 percent confidence)

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Compute expanded measurement uncertainty at 95 percent confidence from combined
#' measurement uncertainty

#' @param ucrt Numeric data vector of combined measurement uncertainty (1 sigma)
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A data frame with a single numeric column: \cr
#' \code{ucrtExpn} - expanded measurement uncertainty

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords combined uncertainty

#' @examples
#' ucrtComb <- c(1,2,1,1,2)
#' NEONprocIS.cal::def.ucrt.expn(ucrtComb=ucrtComb)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.cnst}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.mult}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.comb}
#' @seealso \link[NEONprocIS.base]{def.log.init}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-03)
#     original creation
##############################################################################################
def.ucrt.expn <- function(ucrtComb, log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Error check
  if (!NEONprocIS.base::def.validate.vector(ucrtComb,
                                            TestEmpty=FALSE,
                                            TestNumc=TRUE,
                                            log=log)) {
    stop()
  }
  
  # Compute expanded uncertainty
  ucrtExpn <- ucrtComb*2
  
  # Create data frame
  rpt <- base::data.frame(ucrtExpn = ucrtExpn, stringsAsFactors = FALSE)
  
  return(rpt)
  
}
