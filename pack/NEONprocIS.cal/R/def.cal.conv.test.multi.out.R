##############################################################################################
#' @title Test function for producing multiple outputs, including uncertainty info and cal flags

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Template for custom calibration function that produces the following:\cr
#' - calibrated data with multiple output variables per input variable
#' - uncertainty estimates for all calibrated outputs
#' - uncertainty coefficients for all calibrated outputs
#' - calibration quality flags for all calibrated outputs (not implemented yet, but possible)

#' @param data Data frame of raw, uncalibrated measurements. This data frame must have a column
#' called "readout_time"
#' 
#' @param varConv A character array of the target variables (columns) in the data frame \code{data} for 
#' which calibrated output will be computed (all other columns will be ignored). Defaults to 
#' all column names in \code{data} except 'source_id','site_id','readout_time'.
#' 
#' @param calSlct A named list of data frames, each list element corresponding to a 
#' variable (column) to calibrate. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, 
#' as returned from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' 
#' @param Meta (Optional) List object containing additional metadata for use in 
#' this function as needed. Defaults to an empty list, but this example requires that the list
#' item Meta$Locations is input to work properly. 
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A named list of outputs: \cr
#' data: The input data frame 'data' transformed by this function. \cr
#' ucrtData: A named list of data frames providing uncertainty data for each output variable in data \cr
#' ucrtCoef: A named list of data frames providing uncertainty coefficents for each output variable in data

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000785 TIS Calibrated Measurements and Level 1 Data Products Uncertainty Budget Plan

#' @keywords Currently none

#' @examples
#' None currently

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.b}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly.m}
#' @seealso \link[NEONprocIS.cal]{wrap.cal.conv.dp0p}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2025-10-08)
#     original creation
##############################################################################################
def.cal.conv.test.multi.out <- function(data = data.frame(data=base::numeric(0)),
                                        varConv = setdiff(base::names(data),c('source_id','site_id','readout_time')),
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
  
  # Initialize the uncertainty data and uncertainty coefficients output lists
  ucrtData <- list()
  ucrtCoef <- list()
  dataConv <- data
    
  # Run through each variable to be calibrated
  for(varIdx in varConv){
    
    # Check to see if data to be calibrated is a numeric array
    chk <-
      NEONprocIS.base::def.validate.vector(dataConv[[varIdx]], TestEmpty = FALSE, TestNumc = TRUE, log = log)
    if (!chk) {
      stop()
    }

    # Pull cal information for this variable and initialize output
    calSlctIdx <- calSlct[[varIdx]]
    dataVarIdx <- dataConv[[varIdx]]
    dataConvIdx <- as.numeric(NA)*dataVarIdx
    dataConvIdxAlt <- dataConvIdx # Produce a second calibrated output
    dataUcrtIdx <- data.frame(ucrtMeas=dataConvIdx) # Uncertainty for each var must be a data frame. At least one var should be ucrtMeas
    dataUcrtIdxAlt <- data.frame(ucrtMeas=dataConvIdx) # Uncertainty for the second calibrated output
    varIdxAlt <- paste0(varIdx,'Alt') # This is what we're going to name the alternate output variable
    
    # Return NA if no cal info supplied
    if(base::is.null(calSlctIdx)){
      log$warn(base::paste0('No applicable calibration files available for ',varIdx, '. Returning NA for calibrated output.'))
      calSlctIdx <- base::data.frame() # Will cause skipping to the end
    }
    
    # Run through each calibration file and apply the calibration function for the applicable time period
    # Normally there will be a single cal file, but if the cal file changed mid-day, this will account for it
    ucrtCoefIdx <- base::vector(mode = "list", length = base::nrow(calSlctIdx)) # Initialize uncertainty coefs 
    ucrtCoefIdxAlt <- base::vector(mode = "list", length = base::nrow(calSlctIdx)) # Initialize uncertainty coefs for alternate output var
    for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
      
      # What records in the data correspond to this cal file?
      setCal <- timeMeas >= calSlctIdx$timeBgn[idxRow] & timeMeas < calSlctIdx$timeEnd[idxRow]
      
      # If a calibration file is available for this period, open it and get calibration information
      if(!base::is.na(calSlctIdx$file[idxRow])){
        fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
        infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE,log=log)
      } else {
        infoCal <- NULL
        next
      }
      
      # --------- Apply calibration function ----------
      
      if(grepl(pattern="VSWC",varIdx)){
        # Soil water content
        # Construct a polynomial calibration function using the CVALA coefficients (probably not the correct one for this product)
        funcCal <- NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALA', log = log)
        
        # Convert data using the calibration function
        dataConvIdx[setCal] <- stats::predict(object = funcCal, newdata = dataVarIdx[setCal])
        
        # Produce alternate computation in a 2nd output variable
        zOfst <- Meta$Locations[[1]]$geolocations[[1]]$z_offset # Get the z offset from the location info in Meta$Locations
        dataConvIdxAlt[setCal] <- zOfst + as.numeric(tail(strsplit(varIdx,character(0))[[1]],1)) # Dummy calc. that varies by measurement index
        
      } else if (grepl(pattern="VSIC",varIdx)){
        
        # Soil ion content
        # Construct a polynomial calibration function using the CVALT coefficients (probably not the correct one for this product)
        funcCal <- NEONprocIS.cal::def.cal.func.poly(infoCal = infoCal, Prfx='CVALT', log = log)
        
        # Convert data using the calibration function
        dataConvIdx[setCal] <- stats::predict(object = funcCal, newdata = dataVarIdx[setCal])
        
        # Produce alternate computation in a 2nd output variable
        zOfst <- Meta$Locations[[1]]$geolocations[[1]]$z_offset # Get the z offset from the location info in Meta$Locations
        dataConvIdxAlt[setCal] <- dataConvIdx[setCal] + zOfst  # Dummy calc. that adds the z offset to the calibrated output above
      }
      
      
      # --------- Compute uncertainty ----------
      
      # This section doesn't differentiate between VSWC and VSIC, but you could do something similar to above if needed
      
      # Use the U_CVALA1 coefficient
      coefUcrt <- as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == 'U_CVALA1'])
      dataUcrtIdx$ucrtMeas[setCal] <- coefUcrt*dataVarIdx[setCal]
      
      
      # Produce uncertainty estimate for alternate calibrated output variable
      coefUcrtAlt <- as.numeric(infoCal$ucrt$Value[infoCal$ucrt$Name == 'U_CVALD3']) + 0.1
      dataUcrtIdxAlt$ucrtMeas[setCal] <- coefUcrtAlt # Alternate comp 
      
      
      
      # -------- Record uncertainty coefficients -------------
      # Add in cal metadata to the coefs, excluding the directory path
      infoCal$ucrt$id <- calSlctIdx$id[idxRow]
      infoCal$ucrt$var <- varIdx
      ucrtCoefIdx[[idxRow]] <- base::merge(x=calSlctIdx[idxRow,!(names(calSlctIdx) %in% 'path')],y=infoCal$ucrt,by='id')
      
      # Uncertainty coefs for alternate output variable output
      ucrtCoefIdxAlt[[idxRow]] <- ucrtCoefIdx[[idxRow]][1,] # Use the regular var as template
      ucrtCoefIdxAlt[[idxRow]]$id <- NA # remove the value
      ucrtCoefIdxAlt[[idxRow]]$file <- NA # remove the value. Could also keep if the alternate coefs are w.r.t. each cal file
      ucrtCoefIdxAlt[[idxRow]]$var <- varIdxAlt 
      ucrtCoefIdxAlt[[idxRow]]$Name <- 'offset' # Name it whatever you want 
      ucrtCoefIdxAlt[[idxRow]]$Value <- as.character(coefUcrtAlt)


    } # End loop around calibration files
    
    
    # ---------- Place calibrated & uncertainty data in the output --------
    
    # Replace raw data with calibrated data and add alternate cal outputs
    dataConv[[varIdx]] <- dataConvIdx
    dataConv[[varIdxAlt]] <- dataConvIdxAlt
    
    # Re-arrange the data frame to insert the new variable immediately 
    #   after the first calibrated variable (not required, just an example)
    nameVar <- names(dataConv)
    numVar <- length(nameVar)
    idxVarConv <- which(nameVar == varIdx)
    if(idxVarConv < numVar-1){
      dataConv <- dataConv[,c(1:idxVarConv,numVar,(idxVarConv+1):(numVar-1))]
    } 
    
    # Compute combined uncertainty (add in quadrature any vars starting with ucrtMeas and ucrtFdas)
    nameVarUcrt <- base::names(dataUcrtIdx)
    nameVarUcrtComb <- nameVarUcrt[base::substr(nameVarUcrt,1,8) %in% c('ucrtMeas','ucrtFdas')]
    dataUcrtIdx <- base::cbind(dataUcrtIdx,
                               NEONprocIS.cal::def.ucrt.comb(ucrt=dataUcrtIdx[,nameVarUcrtComb,drop=FALSE],log=log)
    )
    
    # Compute combined for the alternate output                           
    nameVarUcrt <- base::names(dataUcrtIdxAlt)
    nameVarUcrtComb <- nameVarUcrt[base::substr(nameVarUcrt,1,8) %in% c('ucrtMeas','ucrtFdas')]
    dataUcrtIdxAlt <- base::cbind(dataUcrtIdxAlt,
                                  NEONprocIS.cal::def.ucrt.comb(ucrt=dataUcrtIdxAlt[,nameVarUcrtComb,drop=FALSE],log=log)
    )
    
    
    # Compute expanded uncertainty
    dataUcrtIdx <- base::cbind(dataUcrtIdx,
                               NEONprocIS.cal::def.ucrt.expn(ucrtComb=dataUcrtIdx[['ucrtComb']],log=log)
    )
    dataUcrtIdxAlt <- base::cbind(dataUcrtIdxAlt,
                               NEONprocIS.cal::def.ucrt.expn(ucrtComb=dataUcrtIdxAlt[['ucrtComb']],log=log)
    )
    
    # Place uncertainty data in list output (list elements named for the variable - should match the corresponding column name in data)
    ucrtData[[varIdx]] <- dataUcrtIdx
    ucrtData[[varIdxAlt]] <- dataUcrtIdxAlt
    
    # Combine uncertainty coefs for all selected calibrations for this variable
    ucrtCoef[[varIdx]] <- base::Reduce(f=base::rbind,x=ucrtCoefIdx)
    ucrtCoef[[varIdxAlt]] <- base::Reduce(f=base::rbind,x=ucrtCoefIdxAlt)
    
    
  } # End loop around variables
  
  # Remove schema that came with the data (it no longer matches the output because we added variables)
  attr(dataConv,'schema') <- NULL
  
  # Normally we would just return the calibrated data frame. 
  #   But if we include a list, our other outputs will be detected and used
  rpt <- list(data = dataConv,
              ucrtData = ucrtData,
              ucrtCoef = ucrtCoef)
  return(rpt)
  
}
