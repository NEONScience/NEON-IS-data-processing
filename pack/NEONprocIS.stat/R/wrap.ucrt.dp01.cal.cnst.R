##############################################################################################
#' @title Compute combined and expanded (95% confidence) temporally aggregated L1 uncertainty 
#' due to natural variation and calibration (constant value)

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Wrapper function. Compute the combined and expanded (95% confidence) temporally aggregated L1 
#' uncertainty for a set of values subject to natural variation and calibration uncertainty. 
#' Uncertainty due to  natural variation is estimated from the 
#' standard error of the mean. Uncertainty due to calibration is represented by a constant value 
#' coefficient provided by CVAL (U_CVALA3).

#' @param data Data frame of L0' (calibrated) data. Must contain columns \code{readout_time} (POSIX) and 
#' whatever variable is specified in input parameter \code{VarUcrt} (numeric).
#' A single aggregated uncertainty for the selected variable \code{VarUcrt} will be computed over the full timeseries.
#' @param VarUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which uncertainty data will be computed (all other columns will be ignored in this function). 
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list 
#' elements: term (name of L0' term for which the coefficient applies - string), start_date (POSIX), 
#' end_date(POSIX), Name (of the coefficient - string), and 
#' Value (of the coefficient - string or numeric, to be interpreted as numeric). 
#' This will be passed into the calibration uncertainty function. Calibration uncertainty 
#' requires the U_CVALA3 coefficient. 
#' @param ucrtData Unused in this function. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A single numeric value representing the aggregated L1 calibration uncertainty over the full record. 
#' Numeric NA is returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, fdas L1, average

#' @examples
#' data <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                    temp=c(.599,.598,.597))
#' ucrtCoef <- list(list(term='temp',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3',Value='0.0141'))
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        temp=c(100.187,100.195,100.203))
#' ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.cal.cnst(data=data,VarUcrt='temp',ucrtCoef=ucrtCoef)

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{def.ucrt.dp01.cal.cnst}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-12-16)
#     original creation
##############################################################################################
wrap.ucrt.dp01.cal.cnst <- function(data,
                                    VarUcrt,
                                    ucrtCoef,
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
  
  # Compute calibration uncertainty (constant value from CVAL coefficient U_CVALA3)
  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef=ucrtCoef,
                                                     NameCoef='U_CVALA3',
                                                     VarUcrt=VarUcrt,
                                                     TimeAgrBgn=data$readout_time[1],
                                                     TimeAgrEnd=data$readout_time[base::nrow(data)]+as.difftime(.001,units='secs'),
                                                     log=log)
  
  # Compute combined and expanded uncertainty
  ucrt=2*base::sqrt(se^2 + ucrtCal^2) 
  
  return(ucrt)
  
}
