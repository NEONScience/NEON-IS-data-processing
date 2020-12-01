##############################################################################################
#' @title Compute L1 (temporally aggregated) uncertainty due to a single-variable FDAS measurement

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Compute the L1 (temporally aggregated) uncertainty due to the a
#' single-variable FDAS measurement

#' @param data Numeric vector of (calibrated) L0' data measured from the FDAS.
#' A single aggregated uncertainty will be computed over the full timeseries.
#' @param VarUcrt A character string of the name of the variable contained in \code{data}.
#' @param TypeFdas A single character representing the type of FDAS measurement, either 'R' 
#' for resistance measurement, or 'V' for voltage measurement
#' @param ucrtCoef A list of uncertainty coefficients, each a list containing at a minimum the list 
#' elements: term (name of L0' term for which the coefficient applies - string), start_date (POSIX), 
#' end_date(POSIX), Name (of the coefficient - string), and 
#' Value (of the coefficient - string or numeric, to be interpreted as numeric). 
#' Resistance-based FDAS uncertainty requires U_CVALR3 and U_CVALR4 coefficients. Voltage-based FDAS 
#' uncertainty requires U_CVALV3 and U_CVALV4 coefficients.
#' @param ucrtData A data frame of relevant L0' individual measurement uncertainty data corresponding 
#' as generated from one of the FDAS uncertainty functions in the NEONprocIS.cal package, e.g. def.ucrt.fdas.volt.poly. 
#' Columns must include:\cr
#' readout_time (POSIX),\cr
#' VarUcrt_raw (numeric) - raw L0 reading prior to calibration (resistance or voltage),\cr
#' VarUcrt_dervCal (numeric) - 1st derivative of calibration function evaluated at raw reading value, and\cr 
#' VarUcrt_ucrtComb (numeric) - standard uncertainty of individual measurement introduced by the Field DAS, \cr
#' where VarUcrt is the variable name specified in input \code{VarUcrt}.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A single numeric value representing the aggregated L1 FDAS uncertainty for the . Numeric NA is
#' returned if no applicable coefficient is found, with warning.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, L1, average

#' @examples
#' data <- c(.599,.598,.597)
#' ucrtCoef <- list(list(term='temp',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALR3',Value='0.000195'),
#'                  list(term='temp',start_date=as.POSIXct('2019-01-01',tz='GMT'),end_date=as.POSIXct('2019-01-02',tz='GMT'),Name='U_CVALR4',Value='0.0067'))
#' ucrtData <- data.frame(readout_time=as.POSIXct(c('2019-01-01 00:00','2019-01-01 00:01','2019-01-01 00:02'),tz='GMT'),
#'                        temp_raw=c(100.187,100.195,100.203),
#'                        temp_dervCal=c(2.5483,2.5481,2.5484),
#'                        temp_ucrtComb=c(0.06861,0.06860,0.06863),
#'                        stringsAsFactors=FALSE)
#' ucrtFdas <- NEONprocIS.stat::def.ucrt.dp01.fdas(data=data,VarUcrt='temp',TypeFdas='R',ucrtCoef=ucrtCoef,ucrtData=ucrtData)


#' @seealso \link[NEONprocIS.stat]{wrap.ucrt.dp01}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-10-27)
#     original creation
##############################################################################################
def.ucrt.dp01.fdas <- function(data,
                               VarUcrt,
                               TypeFdas,
                               ucrtCoef,
                               ucrtData,
                               log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  
  #log$debug(base::paste0('Computing L1 FDAS uncertainty of type "', TypeFdas, '" for variable ', VarUcrt))
  

  # Check TypeFdas
  if(!(TypeFdas %in% c('R','V'))){
    log$fatal('Input TypeFdas must either be "R" or "V"')
    stop()
  }
  
  # Check data input is numeric
  if (!NEONprocIS.base::def.validate.vector(data,TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
    stop()
  }
  
  # Construct the column names of the L0' FDAS uncertainty data that we need
  nameColRawIdx <- base::paste0(VarUcrt,'_raw')
  nameColDervCalIdx <- base::paste0(VarUcrt,'_dervCal')
  nameColUcrtCombIdx <- base::paste0(VarUcrt,'_ucrtComb') 
  
  # Validate the uncertainty data. Need columns readout_time and the variables above
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=ucrtData,
                                                 TestNameCol=c('readout_time',c(nameColRawIdx,nameColDervCalIdx,nameColUcrtCombIdx)),
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
  
  # Aggregation period (for pulling coefficients)
  timeAgrBgn=ucrtData$readout_time[1]
  timeAgrEnd=ucrtData$readout_time[numData]+as.difftime(.001,units='secs')
  
  # Get Fdas uncertainty multiplier
  # Which uncertainty coefficients match this term, time period, and the uncertainty coef we want (U_CVALV3 or U_CVALR3, depending on TypeFdas)
  mtch <- base::unlist(base::lapply(ucrtCoef,FUN=function(idxUcrt){idxUcrt$term == VarUcrt && 
      idxUcrt$Name == base::paste0('U_CVAL',TypeFdas,'3') && 
      idxUcrt$start_date < timeAgrEnd && idxUcrt$end_date > timeAgrBgn}))
  
  # Pull the uncertainty coefficient
  if(base::sum(mtch) == 0){
    # If there are zero, the coef will be 0
    coefUcrtFdas <- NA
  } else {
    # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
    coefUcrtFdas <- base::max(base::as.numeric(base::unlist(base::lapply(ucrtCoef[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
  }
    
  # Fdas uncertainty offset
  # Which uncertainty coefficients match this term, time period, and the uncertainty coef we want (U_CVALV3 or U_CVALR3 - only one should be populated)
  mtch <- base::unlist(base::lapply(ucrtCoef,FUN=function(idxUcrt){idxUcrt$term == VarUcrt && 
      idxUcrt$Name == base::paste0('U_CVAL',TypeFdas,'4') && 
      idxUcrt$start_date < timeAgrEnd && idxUcrt$end_date > timeAgrBgn}))
    
  # Pull the uncertainty coefficient
  if(base::sum(mtch) == 0){
    # If there are zero, the coef will be 0
    coefUcrtFdasOfst <- NA
  } else {
    # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
    coefUcrtFdasOfst <- base::max(base::as.numeric(base::unlist(base::lapply(ucrtCoef[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
  }
    
  # Do some error checking
  if (base::is.na(coefUcrtFdas) || base::is.na(coefUcrtFdasOfst)){
    # At least one of the terms is not present (but should be). Set uncertainty to NA
    log$error(base::paste0('At least one of the expected FDAS uncertainty coefficients is not present for the term and aggregation interval. Setting uncertainty to NA for term: ',VarUcrt))
    return(base::as.numeric(NA))
  }
    
  # Find the index of the max combined standard measurement uncertainty 
  idxMax <- utils::head(base::which(ucrtData[[nameColUcrtCombIdx]] == base::max(ucrtData[[nameColUcrtCombIdx]],na.rm=TRUE)),n=1)
      
  # Compute the FDAS uncertainty
  if(!is.na(idxMax) && base::length(idxMax)==1){
    ucrtFdas <-base::abs(ucrtData[[nameColDervCalIdx]][idxMax])*(coefUcrtFdas*ucrtData[[nameColRawIdx]][idxMax] + coefUcrtFdasOfst)
  } else {
    return(base::as.numeric(NA))
  }

  return(ucrtFdas)
}
  