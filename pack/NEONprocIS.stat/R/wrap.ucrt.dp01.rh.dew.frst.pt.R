##############################################################################################
#' @title Compute combined and expanded (95% confidence) temporally aggregated L1 uncertainty for dew/frost point 

#' @author
#' Edward Ayres \email{eayres@battelleecology.org}

#' @description
#' Wrapper function. Compute the combined and expanded (95% confidence) temporally aggregated L1 
#' uncertainty for a set of values subject to natural variation and calibration uncertainty. 
#' Uncertainty due to  natural variation is estimated from the 
#' standard error of the mean. Uncertainty due to calibration is represented by a multiplier (U_CVALA3) of the partial 
#' derivatives of dew/frost point with respect to temperature and relative humidity.

#' @param data Data frame of L0' (calibrated) data. Must contain columns \code{readout_time} (POSIX) and 
#' whatever variable is specified in input parameter \code{VarUcrt} (numeric).
#' A single aggregated uncertainty for the selected variable \code{VarUcrt} will be computed over the full timeseries.
#' @param VarUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which uncertainty data will be computed (all other columns will be ignored in this function). 
#' @param ucrtCoef Unused in this function. 
#' @param ucrtData A data frame of relevant L0' individual measurement uncertainty data for 
#' dew point measurement from NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt. 
#' Columns must include:\cr
#' readout_time (POSIX),\cr
#' VarUcrt_ucrt_dfpt_t_L1 (numeric) - 1st derivative of calibration function with respect to temperature multiplied by the L1 uncertainty coefficient, and\cr 
#' VarUcrt_ucrt_dfpt_rh_L1 (numeric) - 1st derivative of calibration function with respect to relative humidity multiplied by the L1 uncertainty coefficient, and\cr 
#' where VarUcrt is the variable name specified in input \code{VarUcrt}. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A single numeric value representing the aggregated L1 uncertainty due to calibration and natural variation over the full record. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, dew point, frost point, uncertainty, average

#' @examples
#' data <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                    dewPoint=c(10,11,9))
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        dewPoint_ucrt_dfpt_t_L1=c(0.05,0.06,0.055),
#'                        dewPoint_ucrt_dfpt_rh_L1=c(0.065,0.055,0.054),
#'                        stringsAsFactors=FALSE)
#' ucrt <- NEONprocIS.stat::wrap.ucrt.dp01.rh.dew.frst.pt(data=data,VarUcrt='dewPoint',ucrtData=ucrtData)

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{def.ucrt.dp01.cal.rh.dew.frst.pt}

#' @export

# changelog and author contributions / copyrights
#   Edward Ayres (2021-05-13)
#     original creation
##############################################################################################
wrap.ucrt.dp01.rh.dew.frst.pt <- function(data,
                                    VarUcrt,
                                    ucrtCoef=NULL,
                                    ucrtData,
                                    log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Validate the data. Need columns readout_time and VarUcrt
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=c('readout_time',VarUcrt),TestEmpty=FALSE, log = log)
  if (!chk) {
    msg <- NEONprocIS.base::def.generate.err.msg(errmsg="Data frame validation failed", fun_calling=rlang::call_frame(n = 2)$fn_name, fun_called=rlang::call_frame(n = 1)$fn_name, lineNum=getSrcLocation(function() {}, "line"))
    log$error(msg)
    stop()
  }
  
  # Pull the variable from data that we care about
  dataComp <- data[[VarUcrt]]
  nameColUcrtdfpt_t <- base::paste0(VarUcrt,'_ucrt_dfpt_t_L1')
  nameColUcrtdfpt_rh <- base::paste0(VarUcrt,'_ucrt_dfpt_rh_L1')
  
  # Check data input is numeric
  if (!NEONprocIS.base::def.validate.vector(dataComp,TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
    msg <- NEONprocIS.base::def.generate.err.msg(errmsg="Vector validation failed", fun_calling=rlang::call_frame(n = 2)$fn_name, fun_called=rlang::call_frame(n = 1)$fn_name, lineNum=getSrcLocation(function() {}, "line"))
    log$error(msg)
    stop()
  }
  
  # Compute uncertainty of the mean due to natural variation, represented by the standard error of the mean
  #log$debug(base::paste0('Computing L1 uncertainty due to natural variation (standard error)'))
  numPts <- base::sum(x=!base::is.na(dataComp),na.rm=FALSE)
  se <- stats::sd(dataComp,na.rm=TRUE)/base::sqrt(numPts)
  
  # Compute calibration uncertainty (constant value from CVAL coefficient U_CVALA3)
  ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.rh.dew.frst.pt(data=dataComp,
                                                               VarUcrt=VarUcrt,
                                                               ucrtData=ucrtData,
                                                               log=log)
  
  # Compute combined and expanded uncertainty
  ucrt=2*base::sqrt(se^2 + ucrtCal[[nameColUcrtdfpt_t]]^2  + ucrtCal[[nameColUcrtdfpt_rh]]^2) 
  
  return(ucrt)
  
}
