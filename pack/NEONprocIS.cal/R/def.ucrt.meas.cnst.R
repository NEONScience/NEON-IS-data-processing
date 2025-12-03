##############################################################################################
#' @title Compute individual measurement calibration uncertainty as a constant from NEON CVAL coefficient U_CVALA1

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Accepts L0 data and NEON uncertainty information as produced
#' by NEONprocIS.cal::def.read.cal.xml and returns individual measurement uncertainties 
#' for each data variable specified. The uncertainty computed is simply a constant value 
#' represented by NEON calibration coefficient U_CVALA1. 

#' @param data Numeric data frame of raw measurements. Must include POSIXt variable readout_time.
#' 
#' @param varUcrt A character array of the target variables (columns) in the data frame \code{data} for 
#' which uncertainty output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' 
#' @param calSlct A named list of data frames, list element corresponding to the variables in
#' varUcrt. The data frame in each list element holds information about the calibration files and 
#' time periods that apply to the variable, as returned from NEONprocIS.cal::def.cal.slct. 
#' See documentation for that function. Assign NULL to list elements (variables) for which calibration
#' information is not applicable (i.e. a function other than def.ucrt.meas.cnst is used to compute its
#' uncertainty).
#' 
#' @param Meta Unused in this function. Defaults to an empty list. See the inputs to 
#' NEONprocIS.cal::wrap.ucrt.dp0p for what this input is.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A named list of data frames, each list named for the variable for which uncertainty 
#' estimates are computed (matching varUcrt). Each uncertainty data frame contains the following variables:\cr
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
#' def.ucrt.meas.cnst(data=data,calSlct=calSlct)

#' @seealso \link[NEONprocIS.cal]{wrap.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}
#' @seealso \link[NEONprocIS.base]{def.log.init}
#' @seealso \link[NEONprocIS.cal]{wrap.ucrt.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-03)
#     original creation
#   Cove Sturtevant (2020-09-02)
#     adjusted inputs to conform to new generic format 
#     This includes inputting the entire data frame, the 
#     variable to be generate uncertainty info for, and the (unused) argument calSlct
#   Cove Sturtevant (2025-06-23)
#     Add unused Meta input to accommodate changes in upstream calibration & uncertainty module
#   Cove Sturtevant (2025-09-17)
#     Refactor to loop through applicable calibration files within this function
#     Also enable uncertainty comps of multiple variables with this function call
#     Return a list of data frames named for the variables specified in varUcrt
#     Return error if no U_CVALA1 found 
##############################################################################################
def.ucrt.meas.cnst <- function(data = data.frame(data=base::numeric(0)),
                               varUcrt = base::names(data)[1],
                               calSlct=NULL,
                               Meta=list(),
                               log = NULL) {
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Ensure input is data frame with the target variables in it
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=c(varUcrt,'readout_time'),TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
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
    ucrtIdx <- base::data.frame(ucrtMeas = NA * dataUcrtIdx)
    
    # Skip if no cal info supplied
    if(base::is.null(calSlctIdx)){
      log$debug(base::paste0('No calibration information supplied for ',
                             varIdx,
                             'returning NA values for individual measurement uncertainty.')
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
        ucrtIdx$ucrtMeas[setCal] <- as.numeric(NA)
        next
      }
      
      # Uncertainty coefficient U_CVALA1 represents the combined measurement uncertainty for an
      # individual reading. It includes the repeatability and reproducibility of the sensor and the
      # lab DAS and ii) uncertainty of the calibration procedures and coefficients including
      # uncertainty in the standard (truth).
      ucrtCoef <- infoCal$ucrt[infoCal$ucrt$Name == 'U_CVALA1',]
      
      # Issue warning if more than one matching uncertainty coefficient was found, issue error if none found
      if(base::nrow(ucrtCoef) > 1){
        log$warn("More than one matching uncertainty coefficient was found for U_CVALA1. Using the first.")
      } else if (base::nrow(ucrtCoef) == 0){
        log$error("No uncertainty coefficient was found for U_CVALA1.")
        stop()
      }
      
      # The individual measurement uncertainty is just U_CVALA1 for each measurement
      ucrtIdx$ucrtMeas[setCal] <- base::as.numeric(ucrtCoef$Value[1])
      
    } # End loop around calibration files
    
    # Place in output
    ucrtList[[varIdx]] <- ucrtIdx
    
  } # End loop around variables to compute uncertainty

  return(ucrtList)
  
}
