##############################################################################################
#' @title Wrapper for computing calibration flags for all variables and time ranges

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Compute valid calibration and suspect calibration flags for NEON L0 data.   

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.  
#' @param calSlct A named list of data frames, list element corresponding to the variable for which
#' uncertainty coefficients are to be compiled. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, as returned 
#' from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' @param mappNameVar A data frame with in/out variable name mapping as produced by 
#' NEONprocIS.base::def.var.mapp.in.out. See documentation for that function.   
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list of qfExpi and qfSusp, each holding data frames with the same dimension as data, 
#' with the exception that the variable readout_time is removed.\cr
#' \code{qfExpi} Integer. The expired/valid calibration flag. 0 = valid, non expired calibration available; 
#' 1 = no calibration or expired calibration available. \cr
#' \code{qfSusp} Integer. The suspect calibration flag. 0 = calibration not suspect, 1 = calibration suspect, 
#' -1 = no cal to evaluate 

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.qf.cal.susp}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
#   Cove Sturtevant (2020-12-09)
#     removed DirCal from inputs since the calibration path is now included in calSlct
##############################################################################################
wrap.qf.cal <- function(data,
                        calSlct,
                        mappNameVar=NULL,
                        log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  varQf <- base::intersect(base::names(data),base::names(calSlct))
  
  # Initialize output
  qfExpi <- base::subset(data,select=varQf); # Initialize flag output
  qfExpi[] <- 1 # Initialize to "expired calibration or no valid calibration available"
  qfSusp <- qfExpi # Initialize flag output
  qfSusp[] <- -1 # Initialize to "cannot evaluate"
  

  # Run through each variable
  for(idxVar in varQf){
    
    # Run through each selected calibration and apply the flag
    calSlctIdx <- calSlct[[idxVar]]
    for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
      
      # What points in the output correspond to this row?
      setCal <- timeMeas >= calSlctIdx$timeBgn[idxRow] & timeMeas < calSlctIdx$timeEnd[idxRow]
      
      # If the cal is not expired, populate valid calibration flag as having a valid calibration
      if(!calSlctIdx$expi[idxRow]){
        qfExpi[[idxVar]][setCal] <- 0
      }
      
      # If no calibration file is available for this period, move on. (Suspect cal already set to -1.)
      if(base::is.na(calSlctIdx$file[idxRow])){
        next
      } 
      
      # We have a calibration file to open
      fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
      infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
      
      # Populate suspect calibration flag
      qfSusp[[idxVar]][setCal] <- NEONprocIS.cal::def.qf.cal.susp(data=data[setCal,idxVar],infoCal=infoCal,log=log)
      
    }
    
  }
  
  # Ensure the output is integer
  qfExpi <- base::lapply(qfExpi,base::as.integer)
  qfSusp <- base::lapply(qfSusp,base::as.integer)
  
  # Map input column names to output column names 
  qfExpi <- NEONprocIS.base::def.df.renm(qfExpi,mappNameVar=mappNameVar,log=log)
  qfSusp <- NEONprocIS.base::def.df.renm(qfSusp,mappNameVar=mappNameVar,log=log)
  
  return(base::list(qfExpi=qfExpi,qfSusp=qfSusp))
  
}
