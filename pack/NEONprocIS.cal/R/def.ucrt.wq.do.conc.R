##############################################################################################
#' @title Individual measurement uncertainty for dissolved oxygen (DO) concentration (mg/L) as part of the water
#' quality data product

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Alternative calibration uncertainty function. Create file (dataframe) with
#' uncertainty information based off of the L0 dissolved oxygen (DO) concentration data values
#' according to NEON.DOC.004931 - NEON Algorithm Theoretical Basis Document (ATBD): Water Quality.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @param data Numeric data frame of raw measurements. 
#' @param infoCal List of calibration and uncertainty information read from a NEON calibration file
#' (as from NEONprocIS.cal::def.read.cal.xml). Included in this list must be infoCal$ucrt, which is
#' a data frame of uncertainty coefficents. Columns of this data frame are:\cr
#' \code{Name} String. The name of the coefficient. \cr
#' \code{Value} String or numeric. Coefficient value. Will be converted to numeric. \cr
#' @param varUcrt A character string of the target variable (column) in the data frame \code{data} 
#' that represents Dissolved oxygen (DO) concentration data. Note that for other
#' uncertainty functions this variable may not need to be in the input data frame, so long as the function
#' knows that. Defaults to the first column in \code{data}.
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.ucrt.dp0p for what this input is. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return  A data frame with the following variables:\cr
#' \code{ucrtMeas} - combined measurement uncertainty for an individual L0 reading.

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
#   Cove Sturtevant (2020-09-02)
#     adjusted inputs to conform to new generic format 
#     This includes inputting the entire data frame, the 
#     variable to be generate uncertainty info for, and the (unused) argument calSlct
##############################################################################################
def.ucrt.wq.do.conc <- function(data = data.frame(data=base::numeric(0)),
                                infoCal = NULL, 
                                varUcrt = base::names(data)[1],
                                calSlct=NULL,
                                log = NULL) {
  # Start logging, if needed
  if (is.null(log)) {
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
  
  #The cal input is not needed for this function
  #It's just a placeholder input to allow the calibration module to be more generic
  
  #Create the output dataframe
  dataUcrt <- data[[varUcrt]] # Target variable to compute uncertainty for
  outputNames <- c("ucrtPercent", "ucrtMeas")
  outputDF <-
    base::as.data.frame(base::matrix(
      nrow = length(dataUcrt),
      ncol = length(outputNames),
      data = NA
    ),
    stringsAsFactors = FALSE)
  names(outputDF) <- outputNames
  log$debug('Output dataframe for dissolvedOxygenConcUnc created.')
  
  #Create an output dataframe with U_CVALA1 based off of the following rules:
  ### U_CVALA1 = 0.01 if DO is > 0 & <= 20 mg/l according to the manual
  ### U_CVALA1 = 0.05 if DO is >20 mg/l & < 50 mg/l according to the manual
  outputDF$ucrtPercent[dataUcrt <= 20] <-
    0.01
  log$debug('Low range DO uncertainty populated.')
  
  outputDF$ucrtPercent[dataUcrt > 20] <-
    0.05
  log$debug('High range DO uncertainty populated.')
  
  #Determine uncertainty factor
  outputDF$ucrtMeas <- outputDF$ucrtPercent * dataUcrt
  
  return(outputDF)
  
}
