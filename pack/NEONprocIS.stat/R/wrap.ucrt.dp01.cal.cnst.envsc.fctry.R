##############################################################################################
#' @title Compute combined and expanded (95% confidence) temporally aggregated L1 uncertainty 
#' due to natural variation and calibration (fixed manufacturer default constant) for enviroscan
#' factory-calibrated output.

#' @author
#' Teresa Burlingame 
#'
#' @description
#' Wrapper function. Compute the combined and expanded (95% confidence) temporally aggregated L1 
#' uncertainty for a set of values subject to natural variation and calibration uncertainty. 
#' Uncertainty due to natural variation is estimated from the 
#' standard error of the mean. Uncertainty due to calibration is represented by the fixed 
#' Sentek manufacturer default calibration uncertainty (0.1068177), matching the constant applied 
#' in NEONprocIS.cal::def.cal.conv.enviro.multi.out.R when computing the factory-calibrated 
#' (non-soil-specific) VSWC output. Unlike the other cal.cnst wrappers, this value is not looked 
#' up from \code{ucrtCoef} because the factory calibration equation always uses the fixed 
#' manufacturer default coefficients rather than the sensor's own calibration coefficients.

#' @param data Data frame of L0' (calibrated) data. Must contain columns \code{readout_time} (POSIX) and 
#' whatever variable is specified in input parameter \code{VarUcrt} (numeric).
#' A single aggregated uncertainty for the selected variable \code{VarUcrt} will be computed over the full timeseries.
#' @param VarUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which uncertainty data will be computed (all other columns will be ignored in this function). 
#' @param ucrtCoef Unused in this function. Accepted for interface compatibility with 
#' NEONprocIS.stat::wrap.ucrt.dp01, which passes this argument to every uncertainty function 
#' regardless of whether it is used.
#' @param ucrtData Unused in this function. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A single numeric value representing the aggregated L1 calibration uncertainty over the full record. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual
#' NEON.DOC.000007 TIS Soil Water Content and Water Salinity

#' @keywords calibration, uncertainty, fdas L1, average

#' @examples
#' data <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                    VSWCfactoryMean=c(0.277,0.278,0.281))
#' ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst.envsc.fctry(data=data,VarUcrt='VSWCfactoryMean',ucrtCoef=NULL)

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01.cal.cnst}

#' @export

# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-09-15)
#     original creation
##############################################################################################
wrap.ucrt.dp01.cal.cnst.envsc.fctry <- function(data,
                                    VarUcrt,
                                    ucrtCoef=NULL,
                                    ucrtData=NULL,
                                    log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Validate the data. Need columns readout_time and VarUcrt
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=c('readout_time',VarUcrt),TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Pull the variable from data that we care about
  dataComp <- data[[VarUcrt]]
  
  # Check data input is numeric
  if (!NEONprocIS.base::def.validate.vector(dataComp,TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
    stop()
  }
  
  # Compute uncertainty of the mean due to natural variation, represented by the standard error of the mean
  #log$debug(base::paste0('Computing L1 uncertainty due to natural variation (standard error)'))
  numPts <- base::sum(x=!base::is.na(dataComp),na.rm=FALSE)
  se <- stats::sd(dataComp,na.rm=TRUE)/base::sqrt(numPts)
  
  # Calibration uncertainty is the fixed Sentek manufacturer default (matches def.cal.conv.enviro.multi.out.R)
  ucrtCal <- 0.1068177
  
  # Compute combined and expanded uncertainty
  ucrt=2*base::sqrt(se^2 + ucrtCal^2) 
  
  return(ucrt)
  
}
