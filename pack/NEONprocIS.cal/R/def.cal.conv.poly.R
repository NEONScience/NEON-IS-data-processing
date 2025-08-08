##############################################################################################
#' @title Convert raw to calibrated data using NEON CVALA polynomial calibration coefficients

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration polynomial function contained in coefficients 
#' CVALA0, CVALA1, CVALA2, etc. to convert raw data to calibrated data.

#' @param data Data frame of raw, uncalibrated measurements. This data frame must have a column
#' called "readout_time"
#' 
#' @param varConv A character string of the target variables (columns) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' 
#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' 
#' @param Meta Unused in this function. Defaults to an empty list. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv for what this input is.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A  Numeric vector of calibrated data\cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' data=data.frame(data=c(1,2,3))
#' infoCal <- data.frame(Name=c('CVALA1','CVALA0'),Value=c(10,1),stringsAsFactors=FALSE)
#' def.cal.conv.poly(data=data,infoCal=infoCal)

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.m}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-02-26)
#     original creation
#   Cove Sturtevant (2019-10-28)
#     Added computation of uncertainty information
#   Mija Choi (2020-01-07)
#     Added parameter validations and logging
#   Cove Sturtevant (2020-01-31)
#     Removed uncertainty quantification (moved to separate function)
#     Split out creation of the polynomial model object into a function
#   Mija Choi (2020-02-24)
#     Added list validations
#   Cove Sturtevant (2020-05-12)
#     Bug fix - incorrectly stopping when infoCal is NULL
#   Cove Sturtevant (2020-07-28)
#     specify CVALA as the coefficient prefix
#   Cove Sturtevant (2020-08-31)
#     adjusted inputs to conform to new generic format for all cal funcs
#     This includes inputting the entire data frame, the 
#     variable to be calibrated, and the (unused) argument calSlct
#   Cove Sturtevant (2025-06-23)
#     Add unused Meta input to accommodate changes in upstream calibration module
#   Cove Sturtevant (2025-08-10)
#     Refactor to loop through applicable calibration files within this function
##############################################################################################
def.cal.conv.poly <- function(data = data.frame(data=base::numeric(0)),
                              varConv = base::names(data)[1],
                              calSlct=NULL,
                              Meta=list(),
                              log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Ensure input is data frame
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=c(varConv,'readout_time'),TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Ensure a single variable is input
  if(base::length(varConv) != 1){
    log$fatal('Calibration function def.cal.conv.poly requires a single character value for the varConv input. Check inputs.')
    stop()
  }
  
  # Check to see if data to be calibrated is a numeric array
  chk <-
    NEONprocIS.base::def.validate.vector(data[[varConv]], TestEmpty = FALSE, TestNumc = TRUE, log = log)
  if (!chk) {
    stop()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  # Run through each variable to be calibrated
  for(varIdx in varConv){
    
    calSlctIdx <- calSlct[[varIdx]]
    dataConvIdx <- data[[varIdx]]
    
    # Return NA if no cal info supplied
    if(base::is.null(calSlctIdx)){
      log$warn(base::paste0('No applicable calibration files available for ',varIdx, '. Returning NA for calibrated output.'))
      dataConvIdx <- as.numeric(NA)
      calSlctIdx <- base::data.frame()
    }
    
    # Run through each calibration file and apply the calibration function for the applicable time period
    for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
      
      # What records in the data correspond to this cal file?
      setCal <- timeMeas >= calSlctIdx$timeBgn[idxRow] & timeMeas < calSlctIdx$timeEnd[idxRow]
      
      # If a calibration file is available for this period, open it and get calibration information
      if(!base::is.na(calSlctIdx$file[idxRow])){
        fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
        infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE,log=log)
      } else {
        infoCal <- NULL
      }
      
      # If infoCal is NULL, return NA data
      if (is.null(infoCal)) {
        dataConvIdx[setCal] <- as.numeric(NA)
        next
      }
      
      # Construct the polynomial calibration function
      func <- NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALA', log = log)
      
      # Convert data using the calibration function
      dataConvIdx[setCal] <- stats::predict(object = func, newdata = dataConvIdx[setCal])
      
    } # End loop around calibration files
    
    # Replace raw data with calibrated data
    data[[varIdx]] <- dataConvIdx
    
  } # End loop around variables
  
  return(data)
  
}
