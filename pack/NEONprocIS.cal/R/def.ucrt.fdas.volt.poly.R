##############################################################################################
#' @title Compute uncertainty attributed to NEON FDAS voltage measurements (and CVALA polynomial calibration conversion)

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Computes L0' uncertainty imposed by a NEON field data acquisition system (FDAS)
#' when making voltage measurements and using a polynomial calibration conversion function with CVALA 
#' coefficients.

#' @param data Data frame of raw measurements. Note that the column/variable indicated in varUcrt must be numeric.
#' 
#' @param varUcrt A character string of the target variable (column) in the data frame \code{data} for 
#' which FDAS uncertainty data will be computed (all other columns will be ignored). Note that for other
#' uncertainty functions this variable may not need to be in the input data frame, so long as the function
#' knows that. Defaults to the first column in \code{data}.
#' 
#' @param calSlct A named list of data frames, list element corresponding to the variables in
#' varUcrt. The data frame in each list element holds information about the calibration files and 
#' time periods that apply to the variable, as returned from NEONprocIS.cal::def.cal.slct. 
#' See documentation for that function. Assign NULL to list elements (variables) for which calibration
#' information is not applicable (i.e. a function other than def.ucrt.meas.cnst is used to compute its
#' uncertainty).
#' 
#' @param Meta Named list of metadata for use in this function. Meta is required to contain
#' list element ucrtCoefFdas, which is a data frame of FDAS uncertainty coefficients, as read by 
#' NEONprocIS.cal::def.read.ucrt.coef.fdas. Columns include:\cr
#' \code{Name} Character. Name of the coefficient.\cr
#' \code{Value} Character. Value of the coefficient.\cr
#' \code{.attrs} Character. Relevant attribute (i.e. units)\cr
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A named list of data frames, each list element corresponding to a variable specified in 
#' \code{varUcrt}. Each data frame contains the following data columns:\cr
#' \code{ucrt$raw} - raw reading value (i.e. same as input data)\cr
#' \code{ucrt$dervCal} - 1st derivative of calibration function evaluated at raw reading value (e.g.
#' partial derivative of a temperature measurement with respect to the resistance reading)\cr
#' \code{ucrt$ucrtFdas} - standard uncertainty of individual measurement introduced by the Field DAS \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.func.poly}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{wrap.ucrt.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-04)
#     original creation
#   Cove Sturtevant (2020-05-12)
#     Bug fix - allow code to produce NAs if infoCal$cal is NULL but infoCal$ucrt is not NULL
#   Cove Sturtevant (2020-09-02)
#     adjusted inputs to conform to new generic format 
#     This includes inputting the entire data frame, the 
#     variable to be generate uncertainty info for, and the (unused) argument calSlct
#   Cove Sturtevant (2025-06-23)
#     Add unused Meta input to accommodate changes in upstream calibration & uncertainty module
#   Cove Sturtevant (2025-10-04)
#     Include ucrtCoefFdas in Meta input
#     refactor to loop through all variables provided in varUcrt input, and
#     output a list of data frames named for those variables
##############################################################################################
def.ucrt.fdas.volt.poly <- function(data = data.frame(data=base::numeric(0)),
                                    varUcrt = base::names(data)[1],
                                    calSlct = NULL,
                                    Meta=list(),
                                    log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Ensure input is data frame with the target variables in it
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=varUcrt,TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  if(!("POSIXt" %in% base::class(timeMeas))){
    log$error('Variable readout_time must be of class POSIXt')
    stop()
  }
  
  # Initialize output list of data frames
  ucrtList <- list()
  
  # Run through each variable to compute uncertainty for
  for(varIdx in varUcrt){
    
    # Check data input is numeric
    if (!NEONprocIS.base::def.validate.vector(data[[varIdx]],TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
      stop()
    }
    
    # Pull cal file info for this variable and initialize output data frame
    calSlctIdx <- calSlct[[varIdx]]
    dataUcrtIdx <- data[[varIdx]]
    ucrtIdx <- base::data.frame(raw = dataUcrtIdx,
                                dervCal = NA * dataUcrtIdx,
                                ucrtFdas = NA * dataUcrtIdx)
    
    # Skip if no cal info supplied
    if(base::is.null(calSlctIdx)){
      log$debug(base::paste0('No calibration information supplied for ',
                             varIdx,
                             'returning NA values for individual FDAS uncertainty.')
      )
      ucrtList[[varIdx]] <- ucrtIdx
      next
    }
    
    # Run through each calibration file and apply the uncertainty function for the applicable time period
    for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
      
      # What records in the data correspond to this cal file?
      setCal <- timeMeas >= calSlctIdx$timeBgn[idxRow] & timeMeas < calSlctIdx$timeEnd[idxRow]
      
      # If a calibration file is available for this period, open it and get uncertainty information
      if(!base::is.na(calSlctIdx$file[idxRow])){
        fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
        infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE,log=log)
      } else {
        infoCal <- NULL
      }
      
      # If infoCal is NULL, return NA data
      if (is.null(infoCal)) {
        ucrtIdx$dervCal[setCal] <- as.numeric(NA)
        ucrtIdx$ucrtFdas[setCal] <- as.numeric(NA)
        next
      }
      
      # Add the FDAS uncertainty coefs to those from the cal file 
      infoCal$ucrt <- base::rbind(infoCal$ucrt,Meta$ucrtCoefFdas,stringsAsFactors=FALSE)
  
      # Check format of infoCal
      if (!NEONprocIS.cal::def.validate.info.cal(infoCal,
                                                 CoefUcrt = c('U_CVALV1', 'U_CVALV4'),
                                                 log = log)) {
        stop()
      }
  
  
      # Compute derivative of calibration function
      func <- NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal,Prfx='CVALA',log=log)
      funcDerv <- stats::deriv(func)
      ucrtIdx$dervCal[setCal] <-
        stats::predict(object = funcDerv, newdata = dataUcrtIdx[setCal]) # Eval derivative at each measurement
  
      # Retrieve the uncertainty coefficients for voltage measurements
      # Combined, relative FDAS uncertainty of an individual measurement (U_CVALV1, unitless).
      coefUcrtFdas <-
        base::as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == "U_CVALV1"])
      
      # Check that we have only 1 coefficient to apply
      if(base::length(coefUcrtFdas) > 1){
        log$warn('Multiple "U_CVALV1" coefficients found in the uncertainty coefficients. Using the first encountered.')
        coefUcrtFdas <- coefUcrtFdas[1]
      }
      
      # Offset imposed by the FDAS, in units of voltage (U_CVALV4)
      coefUcrtFdasOfst <-
        base::as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == "U_CVALV4"])
      
      # Check that we have only 1 coefficient to apply
      if(base::length(coefUcrtFdasOfst) > 1){
        log$warn('Multiple "U_CVALV4" coefficients found in the uncertainty coefficients. Using the first encountered.')
        coefUcrtFdasOfst <- coefUcrtFdasOfst[1]
      }
      
      # Compute FDAS uncertainty
      ucrtIdx$ucrtFdas[setCal] <-
        (coefUcrtFdas * dataUcrtIdx[setCal] + coefUcrtFdasOfst) * base::abs(ucrtIdx$dervCal[setCal])
  
    } # End loop around calibration files
    
    # Place in output
    ucrtList[[varIdx]] <- ucrtIdx
    
  } # End loop around variables to compute uncertainty
  
  return(ucrtList)
  
}
