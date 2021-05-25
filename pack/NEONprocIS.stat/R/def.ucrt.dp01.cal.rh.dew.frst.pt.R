##############################################################################################
#' @title Compute L1 calibration (temporally aggregated) uncertainty of a dew point measurement

#' @author
#' Edward Ayres \email{eayres@battelleecology.org}

#' @description
#' Definition function. Compute the L1 calibration (temporally aggregated) uncertainty of a
#' dew point measurement

#' @param data Vector
#' @param VarUcrt A character string of the name of the dew point variable.
#' @param ucrtData A data frame of relevant L0' individual measurement uncertainty data for 
#' dew point measurement from NEONprocIS.cal::def.ucrt.meas.rh.dew.frst.pt. 
#' Columns must include:\cr
#' readout_time (POSIX),\cr
#' VarUcrt_ucrt_dfpt_t_L1 (numeric) - 1st derivative of calibration function with respect to temperature multiplied by the L1 uncertainty coefficient, and\cr 
#' VarUcrt_ucrt_dfpt_rh_L1 (numeric) - 1st derivative of calibration function with respect to relative humidity multiplied by the L1 uncertainty coefficient, and\cr 
#' where VarUcrt is the variable name specified in input \code{VarUcrt}.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of the L1 dew point calibration uncertainties with respect to temperature and relative humidity.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual
#' NEON.DOC.000851 NEON ATBD Humidity and temperature

#' @keywords calibration, dew point, uncertainty, L1, average

#' @examples
#' data <- c(10,11,9)
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        dewPoint_ucrt_dfpt_t_L1=c(0.05,0.06,0.055),
#'                        dewPoint_ucrt_dfpt_rh_L1=c(0.065,0.055,0.054),
#'                        stringsAsFactors=FALSE)
#' ucrtCal <- NEONprocIS.stat::def.ucrt.dp01.cal.rh.dew.frst.pt(data=data,VarUcrt='dewPoint',ucrtData=ucrtData)


#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.rh.dew.frst.pt}

#' @export

# changelog and author contributions / copyrights
#   Edward Ayres (2021-05-13)
#     original creation
##############################################################################################
def.ucrt.dp01.cal.rh.dew.frst.pt <- function(data,
                               VarUcrt,
                               ucrtData,
                               log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  
  # Construct the column names of the L0' dew point uncertainty data that we need
  nameColUcrtdfpt_t <- base::paste0(VarUcrt,'_ucrt_dfpt_t_L1')
  nameColUcrtdfpt_rh <- base::paste0(VarUcrt,'_ucrt_dfpt_rh_L1')

  # Validate the uncertainty data. Need columns readout_time and the variables above
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=ucrtData,
                                                 TestNameCol=c('readout_time',c(nameColUcrtdfpt_t,nameColUcrtdfpt_rh)),
                                                 TestEmpty=FALSE, 
                                                 log = log)
  if (!chk) {
    stop()
  }
    
  # Make sure ucrtData has the same number of rows
  numData <- base::length(data)
  if(base::nrow(ucrtData) != numData){
    log$error('Uncertainty data must be the same length (rows) as data.')
    stop()
  }
  
  #Go through and remove uncertainty data for any time points that were filtered in the data series (e.g. from the QA/QC step)
  ucrtData[base::is.na(data),base::setdiff(base::names(ucrtData),'readout_time')] <- NA
  
  # Find the index of the max dew point uncertainty with respect to temperature and relative humidity 
  idxMax_t <- utils::head(base::which(ucrtData[[nameColUcrtdfpt_t]] == base::max(ucrtData[[nameColUcrtdfpt_t]],na.rm=TRUE)),n=1)
  idxMax_rh <- utils::head(base::which(ucrtData[[nameColUcrtdfpt_rh]] == base::max(ucrtData[[nameColUcrtdfpt_rh]],na.rm=TRUE)),n=1)
  
  # Compute the uncertainty
  if(!is.na(idxMax_t) && !is.na(idxMax_rh) && base::length(idxMax_t)==1 && base::length(idxMax_rh)==1){
    ucrtCal <-base::data.frame(dewPoint_ucrt_dfpt_t_L1=ucrtData[[nameColUcrtdfpt_t]][idxMax_t], 
                               dewPoint_ucrt_dfpt_rh_L1=ucrtData[[nameColUcrtdfpt_rh]][idxMax_rh],
                               stringsAsFactors=F)
  } else {
    ucrtCal <-base::data.frame(dewPoint_ucrt_dfpt_t_L1=base::as.numeric(NA), 
                               dewPoint_ucrt_dfpt_rh_L1=base::as.numeric(NA),
                               stringsAsFactors=F)
  }
  base::names(ucrtCal) <- c(nameColUcrtdfpt_t,nameColUcrtdfpt_rh)
  return(ucrtCal)
}
  