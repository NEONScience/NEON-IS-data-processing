##############################################################################################
#' @title Convert raw to calibrated data using NEON CVALM polynomial calibration coefficients

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration polynomial function contained in coefficients 
#' CVALM0, CVALM1, CVALM2, etc. to convert raw data to calibrated data.

#' @param data Data frame of raw, uncalibrated measurements. This data frame must have a column
#' called "readout_time" with POSIXct timestamps
#' 
#' @param varConv A character array of the target variables (columns) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Defaults to the first
#' column in \code{data}.
#' 
#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' 
#' @param Meta Unused in this function. Defaults to an empty list. See the inputs to 
#' NEONprocIS.cal::wrap.cal.conv.dp0p for what this input is.
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
#' Not Run
#' data=data.frame(readout_time=as.POSIXct('2025-01-01','2025-01-02','2025-01-03'),var1=c(1,2,3),var2=c(4,5,6))
#' calSlct <- NEONprocIS.cal::wrap.cal.slct(
#'                DirCal = '/path/to/calibration/files',
#'                NameVarExpc = c('var1','var2'),
#'                TimeBgn = as.POSIXct('2025-01-01'),
#'                TimeEnd = as.POSIXct('2025-01-04'),
#'                )
#' dataCal <- def.cal.conv.poly.m(data=data,varConv=c('var1','var2'),calSlct=calSlct)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-07-28)
#     original creation, from def.cal.conv.poly
#   Cove Sturtevant (2020-08-31)
#     adjusted inputs to conform to new generic format for all cal funcs
#     This includes inputting the entire data frame, the 
#     variable to be calibrated, and the (unused) argument calSlct
#   Cove Sturtevant (2025-06-23)
#    Add unused Meta input to accommodate changes in upstream calibration module
#   Cove Sturtevant (2025-08-10)
#     Refactor to loop through applicable calibration files within this function
#     Also enable multiple variables to be calibrated with this function call
##############################################################################################
def.cal.conv.poly.m <- function(data = data.frame(data=base::numeric(0)),
                                varConv = base::names(data)[1],
                                calSlct=NULL,
                                Meta=list(),
                                log = NULL) {
  # Intialize logging if needed
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Ensure input is data frame with variables to be calibrated
  chk <- NEONprocIS.base::def.validate.dataframe(dfIn=data,TestNameCol=c(varConv,'readout_time'),TestEmpty=FALSE, log = log)
  if (!chk) {
    stop()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  if(!("POSIXt" %in% base::class(timeMeas))){
    log$error('Variable readout_time must be of class POSIXt')
    stop()
  }
  
  # Run through each variable to be calibrated
  for(varIdx in varConv){
    
    # Check to see if data to be calibrated is a numeric array
    chk <-
      NEONprocIS.base::def.validate.vector(data[[varIdx]], TestEmpty = FALSE, TestNumc = TRUE, log = log)
    if (!chk) {
      stop()
    }
    
    # Pull cal file info for this variable and initialize the output
    calSlctIdx <- calSlct[[varIdx]]
    dataConvIdx <- data[[varIdx]]
    dataConvOutIdx <- as.numeric(NA)*dataConvIdx
    
    # Skip calibration if no cal info supplied
    if(base::is.null(calSlctIdx)){
      log$warn(base::paste0('No applicable calibration files available for ',varIdx, '. Returning NA for calibrated output.'))
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
        dataConvOutIdx[setCal] <- as.numeric(NA)
        next
      }
      
      # Construct the polynomial calibration function
      func <-  NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALM', log = log)
  
      # Convert data using the calibration function
      dataConvOutIdx[setCal] <- stats::predict(object = func, newdata = dataConvIdx[setCal])
      
    } # End loop around calibration files
    
    # Replace raw data with calibrated data
    data[[varIdx]] <- dataConvOutIdx
    
  } # End loop around variables
  
  return(data)
  
}
