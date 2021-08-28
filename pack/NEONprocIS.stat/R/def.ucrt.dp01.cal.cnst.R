##############################################################################################
#' @title Compute L1 (temporally aggregated) calibration uncertainty represented as a constant value 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Compute the L1 (temporally aggregated) calibration uncertainty due
#' to the accuracy of the instrumentation in the form of Truth and Trueness. The uncertainty is
#' a constant value stored in an uncertainty coefficient provided by CVAL. 

#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list 
#' elements: term (name of L0' varaible/term for which the coefficient applies - string), start_date (POSIX), 
#' end_date(POSIX), Name (of the coefficient - string), 
#' and Value (of the coefficient - string or numeric, to be interpreted as numeric). 
#' @param NameCoef The name of the coefficient that represents the L1 calibration uncertainty. Defaults
#' to U_CVALA3.
#' @param VarUcrt A character string of the target variable/term for which uncertainty will be computed. 
#' @param TimeAgrBgn A POSIX time value indicating the beginning of the temporal aggregation interval (inclusive)
#' @param TimeAgrEnd A POSIX time value indicating the end of the temporal aggregation interval (non-inclusive)
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A single numeric value representing the aggregated L1 calibration uncertainty. If the aggregation
#' interval spans multiple coefficient start and end periods, the maximum value of the applicable coefficients 
#' is selected. Numeric NA is returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, L1, average

#' @examples
#' ucrtCoef <- list(list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA3',Value='0.0141'),
#'                  list(term='linePAR',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALA1',Value='0.0143'))
#' ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.cnst(ucrtCoef=ucrtCoef,NameCoef='U_CVALA3',VarUcrt='linePAR',TimeAgrBgn=as.POSIXct('2019-01-01 00:00',tz='GMT'),TimeAgrEnd=as.POSIXct('2019-01-01 00:30',tz='GMT'))

#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01.cal.cnst.fdas.rstc}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-10-27)
#     original creation
##############################################################################################
def.ucrt.dp01.cal.cnst <- function(ucrtCoef,
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
  
  # Which uncertainty coefficients match this term, time period, and the uncertainty coef we want 
  mtch <- base::unlist(base::lapply(ucrtCoef,FUN=function(idxUcrt){idxUcrt$term == VarUcrt && idxUcrt$Name == NameCoef && 
      idxUcrt$start_date < TimeAgrEnd && idxUcrt$end_date > TimeAgrBgn}))
  
  # Pull the uncertainty coeffiecient
  if(base::sum(mtch) == 0){
    # If there are zero, the coef will be NA
    ucrtCal <- base::as.numeric(NA)
    # log$debug(base::paste0('No uncertainty coefficient was found that matched coefficient name: ', NameCoef,
    #           ', term name: ',VarUcrt, ', and aggregation interval: ',TimeAgrBgn,' to ',TimeAgrEnd))
    msg_in <-   base::paste0('No uncertainty coefficient was found that matched coefficient name: ', NameCoef,', term name: ',VarUcrt, ', and aggregation interval: ',TimeAgrBgn,' to ',TimeAgrEnd)
    msg <- NEONprocIS.base::def.generate.err.msg(errmsg=msg_in, fun_calling=rlang::call_frame(n = 2)$fn_name, fun_called=rlang::call_frame(n = 1)$fn_name, lineNum=getSrcLocation(function() {}, "line"))
    log$error(msg)
  } else {
    # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
    ucrtCal <- base::max(base::as.numeric(base::unlist(base::lapply(ucrtCoef[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
  }
  
  return(ucrtCal)
  
}
