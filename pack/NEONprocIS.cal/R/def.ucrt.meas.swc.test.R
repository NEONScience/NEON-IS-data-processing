##############################################################################################
#' @title Compute standard and alternate measurement calibration uncertainties for SWC measurements

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Accepts L0 data and NEON uncertainty information as produced
#' by NEONprocIS.cal::def.read.cal.xml and returns a list of data frames of individual measurement
#' uncertainties for each data value. Computes the standard CVAL uncertainty as the L0 value 
#' multipled by NEON calibration uncertainty coefficient U_CVALA1. Also computes an alternate 
#' uncertainty data frame computed is the L0 value multipled by NEON calibration uncertainty 
#' coefficient U_CVALD1 minus 10. 

#' @param data Numeric data frame of raw measurements.
#' 
#' @param varUcrt A character array of the target variables (columns) in the data frame \code{data} for 
#' which uncertainty output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' 
#' @param calSlct Unused in this function. Defaults to NULL. See the inputs to 
#' NEONprocIS.cal::wrap.ucrt.dp0p for what this input is. 
#' 
#' @param Meta Unused in this function. Defaults to an empty list. See the inputs to 
#' NEONprocIS.cal::wrap.ucrt.dp0p for what this input is.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A named list of data frames, each list named for the variable for which uncertainty 
#' estimates are contained within. The standard computations are provided in list elements named for
#' each variable provided in \code{varUcrt}, and the alternate computations provided in list elements 
#' named for each variable provided in \code{varUcrt} with "Alt" appended to the name. 
#' Each uncertainty data frame contains the following variables:\cr
#' \code{ucrtMeas} - combined measurement uncertainty for an individual reading. Includes the
#' repeatability and reproducibility of the sensor and the lab DAS and uncertainty of the
#' calibration procedures and coefficients including uncertainty in the standard (truth).

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Level 1 Data products Uncertainty Budget Estimation Plan
#' NEON.DOC.000746 Calibration Fixture and Sensor Uncertainty Analysis: CVAL 2014 Uncertainty Manual

#' @keywords calibration, uncertainty, L0'

#' @examples
#' # Not run
#' data <- data.frame(data=c(1,6,7,0,10))
#' calSlct <- NEONprocIS.cal::wrap.cal.slct(
#'   DirCal = "/path/to/datum/calibration/folder",
#'   NameVarExpc = "data",
#'   TimeBgn = as.POSIXct('2020-01-01',tz='GMT'),
#'   TimeEnd = as.POSIXct('2020-01-02',tz='GMT'),
#'   NumDayExpiMax = NA
#')
#' def.ucrt.meas.mult(data=data,calSlct=calSlct)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.base]{def.log.init}
#' @seealso \link[NEONprocIS.cal]{wrap.ucrt.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2025-10-04)
#     original creation, based on def.ucrt.meas.mult
##############################################################################################
def.ucrt.meas.swc.test <- function(data = data.frame(data=base::numeric(0)),
                                   varUcrt = base::names(data)[1],
                                   calSlct=NULL,
                                   Meta=list(),
                                   log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Ensure input is data frame with the target variable in it
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=varUcrt,TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  # Initialize output list of data frames
  ucrtList <- list()
  
  # Run through each variable to compute uncertainty for
  for(varIdx in varUcrt){
    
    # We're going to create another output uncertainty data frame for the alternate cal computation
    varIdxAlt <- paste0(varIdx,'Alt')
    
    # Check data input is numeric
    if (!NEONprocIS.base::def.validate.vector(data[[varIdx]],TestEmpty = FALSE, TestNumc = TRUE, log=log)) {
      stop()
    }
    
    # Pull cal file info for this variable and initialize output data frame
    calSlctIdx <- calSlct[[varIdx]]
    dataUcrtIdx <- data[[varIdx]]
    ucrtIdx <- base::data.frame(ucrtMeas = NA * dataUcrtIdx) # e.g. standard CVAL uncertainty output
    ucrtIdxAlt <- base::data.frame(ucrtMeas = NA * dataUcrtIdx) # e.g. Create uncertainty for a second output variable (custom comp)
    
    # Skip if no cal info supplied
    if(base::is.null(calSlctIdx)){
      log$debug(base::paste0('No calibration information supplied for ',
                             varIdx,
                             'returning NA values for individual measurement uncertainty.')
      )
      ucrtList[[varIdx]] <- ucrtIdx
      ucrtList[[varIdxAlt]] <- ucrtIdxAlt
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
        ucrtIdx$ucrtMeas[setCal] <- as.numeric(NA)
        next
      }
      
      # Uncertainty coefficient U_CVALA1 represents the combined measurement uncertainty for an
      # individual reading. It includes the repeatability and reproducibility of the sensor and the
      # lab DAS and ii) uncertainty of the calibration procedures and coefficients including
      # uncertainty in the standard (truth).
      ucrtCoef <- infoCal$ucrt[infoCal$ucrt$Name == 'U_CVALA1',]
      ucrtCoefAlt <- infoCal$ucrt[infoCal$ucrt$Name == 'U_CVALD1',] # Our alternate computation uses a different coefficient
      
      # Issue warning if more than one matching uncertainty coefficient was found
      if(base::nrow(ucrtCoef) > 1){
        log$warn("More than one matching uncertainty coefficient was found for U_CVALA1. Using the first.")
      }
      
      # The individual measurement uncertainty is just U_CVALA1 multiplied by each measurement
      ucrtIdx$ucrtMeas[setCal] <- base::as.numeric(ucrtCoef$Value[1])*dataUcrtIdx[setCal]
      
      # Our alternate individual measurement uncertainty is just U_CVALD1 multiplied by each measurement, minus a constant
      ucrtIdxAlt$ucrtMeas[setCal] <- base::as.numeric(ucrtCoefAlt$Value[1])*dataUcrtIdx[setCal] - 10
      
      
      } # End loop around calibration files
    
    # Place in output
    ucrtList[[varIdx]] <- ucrtIdx
    ucrtList[[varIdxAlt]] <- ucrtIdxAlt
    
  } # End loop around variables to compute uncertainty
  
  return(ucrtList)
  
}
