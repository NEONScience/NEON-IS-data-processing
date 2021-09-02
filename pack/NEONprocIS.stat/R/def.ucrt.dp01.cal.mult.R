##############################################################################################
#' @title Compute L1 (temporally aggregated) calibration uncertainty represented as a multiplier to the max L0' value 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Compute the L1 (temporally aggregated) calibration uncertainty due
#' to the accuracy of the instrumentation in the form of Truth and Trueness. The uncertainty 
#' is represented by a percentage multiplier (coefficient provided by CVAL) to the maximum L0' value. 

#' @param data Numeric vector of (calibrated) L0' data.
#' A single aggregated uncertainty will be computed for the full timeseries.
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list 
#' elements: term (name of L0' term for which the coefficient applies - string), start_date (POSIX), 
#' end_date(POSIX), Name (of the coefficient - string), 
#' and Value (of the coefficient - string or numeric, to be interpreted as numeric). 
#' @param NameCoef The name of the coefficient that represents the value to be multiplied with
#' the maximum L0' value. Defaults to U_CVALA3. Note that the units in the calibration file may say percent, but it is really a scale factor.
#' @param VarUcrt A character string of the target variable/term for which uncertainty will be computed 
#' (i.e. the name of the term/variable for the data in \code{data}). 
#' @param TimeAgrBgn A POSIX time value indicating the beginning of the temporal aggregation interval (inclusive)
#' @param TimeAgrEnd A POSIX time value indicating the end of the temporal aggregation interval (non-inclusive)
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A single numeric value representing the aggregated L1 calibration uncertainty. If the aggregation
#' interval spans multiple coefficient start and end periods, the maximum value of the applicable coefficients 
#' is used as the multiplier. Numeric NA is returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, L1, average

#' @examples
#' data <- c(827.7,827.8,831.3)
#' ucrtCoef <- list(list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3',Value='0.0141'),
#'                  list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA1',Value='0.0143'))
#' ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.mult(data=data,ucrtCoef=ucrtCoef,NameCoef='U_CVALA3',VarUcrt='linePAR',TimeAgrBgn=as.POSIXct('2019-01-01 00:00',tz='GMT'),TimeAgrEnd=as.POSIXct('2019-01-01 00:30',tz='GMT'))

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01.cal.mult.fdas.volt}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-10-27)
#     original creation
##############################################################################################
def.ucrt.dp01.cal.mult <- function(data,
                                   ucrtCoef,
                                   NameCoef='U_CVALA3',
                                   VarUcrt,
                                   TimeAgrBgn,
                                   TimeAgrEnd,
                                   log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  #log$debug(base::paste0('Computing L1 calibration uncertainty from constant coefficient ',NameCoef))
  
  # Check data input is numeric
  if (!NEONprocIS.base::def.validate.vector(data,TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
    msg <- NEONprocIS.base::def.generate.err.msg(errmsg="Vector validation failed", fun_calling=rlang::call_frame(n = 2)$fn_name, fun_called=rlang::call_frame(n = 1)$fn_name, lineNum=getSrcLocation(function() {}, "line"))
    log$error(msg)
    stop()
  }
  
  # Which uncertainty coefficients match this term, time period, and the uncertainty coef we want 
  mtch <- base::unlist(base::lapply(ucrtCoef,FUN=function(idxUcrt){idxUcrt$term == VarUcrt && idxUcrt$Name == NameCoef && 
      idxUcrt$start_date < TimeAgrEnd && idxUcrt$end_date > TimeAgrBgn}))
  
  # Pull the uncertainty coeffiecient
  if(base::sum(mtch) == 0){
    # If there are zero, the coef will be NA
    coefCal <- base::as.numeric(NA)
    # log$debug(base::paste0('No uncertainty coefficient was found that matched coefficient name: ', NameCoef,
    #           ', term name: ',VarUcrt, ', and aggregation interval: ',TimeAgrBgn,' to ',TimeAgrEnd))
    msg_in <- base::paste0('No uncertainty coefficient was found that matched coefficient name: ', NameCoef,
                           ', term name: ',VarUcrt, ', and aggregation interval: ',TimeAgrBgn,' to ',TimeAgrEnd)
    msg <- NEONprocIS.base::def.generate.err.msg(errmsg=msg_in, fun_calling=rlang::call_frame(n = 2)$fn_name, fun_called=rlang::call_frame(n = 1)$fn_name, lineNum=getSrcLocation(function() {}, "line"))
    log$error(msg)
  } else {
    # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
    coefCal <- base::max(base::as.numeric(base::unlist(base::lapply(ucrtCoef[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
  }
  
  # Multiply the coefficient with the maximum L0' value
  ucrtCal <- base::max(data,na.rm=TRUE)*coefCal
  
  return(ucrtCal)
  
}
