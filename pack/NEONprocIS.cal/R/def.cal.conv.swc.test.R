##############################################################################################
#' @title Test function for producing multiple calibrated outputs for each calibrated SWC stream

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Apply NEON calibration polynomial function contained in coefficients 
#' CVALA0, CVALA1, CVALA2, etc. to convert raw data to calibrated data.

#' @param data Data frame of raw, uncalibrated measurements. This data frame must have a column
#' called "readout_time"
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
#   Cove Sturtevant (2025-08-08)
#     original creation
##############################################################################################
def.cal.conv.swc.test <- function(data = data.frame(data=base::numeric(0)),
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
  
  # Run through each variable to be calibrated
  for(varIdx in varConv){
    
    # Check to see if data to be calibrated is a numeric array
    chk <-
      NEONprocIS.base::def.validate.vector(data[[varIdx]], TestEmpty = FALSE, TestNumc = TRUE, log = log)
    if (!chk) {
      stop()
    }

    # Pull cal and initialize
    calSlctIdx <- calSlct[[varIdx]]
    dataConvIdx <- data[[varIdx]]
    dataConvOutIdx <- as.numeric(NA)*dataConvIdx
    dataConvOutIdx2 <- dataConvOutIdx # Produce a second output
    
    # Return NA if no cal info supplied
    if(base::is.null(calSlctIdx)){
      log$warn(base::paste0('No applicable calibration files available for ',varIdx, '. Returning NA for calibrated output.'))
      dataConvOutIdx <- as.numeric(NA)
      dataConvOutIdx2 <- as.numeric(NA)
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
        dataConvOutIdx2[setCal] <- as.numeric(NA)
        next
      }
      
      # --------- Apply calibration function ----------
      
      # Convert data using the calibration function. Produce two outputs. 
      dataConvOutIdx[setCal] <- 111
      dataConvOutIdx2[setCal] <- 222
      
      # -----------------------------------------------
      
    } # End loop around calibration files
    
    
    # ---------- Place calibrated data in the output --------
    
    # Replace raw data with calibrated data
    data[[varIdx]] <- dataConvOutIdx
    data[[paste0(varIdx,'2')]] <- dataConvOutIdx2
    
    # Re-arrange the data frame to insert the new variable immediately 
    #   after the first calibrated variable (not required, just an example)
    nameVar <- names(data)
    numVar <- length(nameVar)
    idxVarConv <- which(nameVar == varIdx)
    data <- data[,c(1:idxVarConv,numVar,(idxVarConv+1):numVar)]
    
    # -------------------------------------------------------
    
    
  } # End loop around variables
  
  # Remove schema that came with the data (it no longer matches the output because we added a variable)
  attr(data,'schema') <- NULL
  
  return(data)
  
}
