##############################################################################################
#' @title Uncertainty for dew/frost point from the relative humidity sensor as part of the relative
#' humidity transition

#' @author
#' Edward Ayres \email{eayres@battelleecology.org}

#' @description Alternative calibration uncertainty function. Create file (dataframe) with
#' uncertainty information based off of the L0 temperature and relative humidity data values from 
#' the relative humidity sensor according to NEON.DOC.000851 - NEON Algorithm Theoretical Basis   
#' Document (ATBD): Humidity and Temperature Sensor.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @param data Temperature, relative humidity, and dew/frost point data from the relative humidity 
#' sensor [vector]
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return dataframe with L0 uncertatinty column(s) [dataframe]

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' #Written to potentially plug in to def.cal.conv.R
#' ucrt <- def.ucrt.dew.frst.pnt(data = data, cal = NULL)

#' @seealso None currently

# changelog and author contributions / copyrights
#   Edward Ayres (2020-07-23)
#     original creation
##############################################################################################
def.ucrt.dew.frst.pnt <- function(data, infoCal = NULL, log = NULL) {
  # Start logging, if needed
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  #Check that we have more than 0 rows of data
  if (!NEONprocIS.base::def.validate.vector(data, TestEmpty = FALSE, log =
                                            log)) {
    msg <-
      base::paste0('       |------ data is empty. Uncertainty will not run\n')
    log$error(msg)
    stop()
  }
  
  #The cal input is not needed for this function
  #It's just a placeholder input to allow the calibration module to be more generic
  
  # Specify constants based on ATBD
  absZero <- 273.15
  b0 <- -0.58002206*10^4
  b1 <- 1.3914993
  b2 <- -0.048640239
  b3 <- 0.41764768 * 10^-4
  b4 <- -0.14452093 * 10^-7
  b5 <- 6.5459673
  c0 <- 0.4931358
  c1 <- -0.46094296 * 10^-2
  c2 <- 0.13746454 * 10^-4
  c3 <- -0.12743214 * 10^-7
  a0 <- -0.56745359 x 10^4
  a1 <- 6.3925247
  a2 <- -0.96778430 x 10^-2
  a3 <- 0.62215701 x 10^-6
  a4 <- 0.20747825 x 10^-8
  a5 <- -0.94840240 x 10^-12
  a6 <- 4.1635019
  
  #Create the output dataframe
  outputNames <- c("ucrtMeas")
  outputDF <-
    base::as.data.frame(base::matrix(
      nrow = length(data),
      ncol = length(outputNames),
      data = NA
    ),
    stringsAsFactors = FALSE)
  names(outputDF) <- outputNames
  log$debug('Output dataframe for dew_point created.')
  
  #Create an output dataframe with U_CVALA1 based off of the following rules:
  ### U_CVALA1 = 0.01 if temp is <= 35 Celsius according to the manual
  ### U_CVALA1 = 0.05 if temp is >35 Celsius according to the manual
  outputDF$ucrtMeas[data <= 35] <-
    0.01
  log$debug('Low range temp uncertainty populated.')
  
  outputDF$ucrtMeas[data > 35] <-
    0.05
  log$debug('High range temp uncertainty populated.')
  
  return(outputDF)
  
}
