##############################################################################################
#' @title Uncertainty for dissolved oxygen (DO) concentration (mg/L) as part of the water
#' quality transition

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Alternative calibration uncertainty function. Create file (dataframe) with
#' uncertainty information based off of the L0 dissolved oxygen (DO) concentration data values
#' according to NEON.DOC.004931 - NEON Algorithm Theoretical Basis Document (ATBD): Water Quality.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @param data Dissolved oxygen (DO) concentration data [vector]
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return dataframe with L0 uncertatinty column(s) [dataframe]

#' @export

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' #Written to potentially plug in to def.cal.conv.R
#' ucrt <- def.ucrt.wq.do.conc(data = data, cal = NULL)

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
##############################################################################################
def.ucrt.wq.do.conc <- function(data, infoCal = NULL, log = NULL) {
  # Start logging, if needed
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  #Check that we have a valid vector
  if (!NEONprocIS.base::def.validate.vector(data, TestEmpty = FALSE, log =
                                            log)) {
    msg <-
      base::paste0('       |------ data is empty. Uncertainty will not run\n')
    log$error(msg)
    stop()
  }
  
  #The cal input is not needed for this function
  #It's just a placeholder input to allow the calibration module to be more generic
  
  #Create the output dataframe
  outputNames <- c("ucrtPercent", "ucrtMeas")
  outputDF <-
    base::as.data.frame(base::matrix(
      nrow = length(data),
      ncol = length(outputNames),
      data = NA
    ),
    stringsAsFactors = FALSE)
  names(outputDF) <- outputNames
  log$debug('Output dataframe for dissolvedOxygenConcUnc created.')
  
  #Create an output dataframe with U_CVALA1 based off of the following rules:
  ### U_CVALA1 = 0.01 if DO is > 0 & <= 20 mg/l according to the manual
  ### U_CVALA1 = 0.05 if DO is >20 mg/l & < 50 mg/l according to the manual
  outputDF$ucrtPercent[data <= 20] <-
    0.01
  log$debug('Low range DO uncertainty populated.')
  
  outputDF$ucrtPercent[data > 20] <-
    0.05
  log$debug('High range DO uncertainty populated.')
  
  #Determine uncertainty factor
  outputDF$ucrtMeas <- outputDF$ucrtPercent * data
  
  return(outputDF)
  
}
