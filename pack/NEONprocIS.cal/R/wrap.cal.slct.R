##############################################################################################
#' @title Wrapper for selecting the applicable calibrations and their time ranges for all variables

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Select the calibrations and their time ranges that apply for each 
#' selected variable.

#' @param DirCal Character string. Relative or absolute path (minus file name) to the main calibration
#' directory. Nested within this directory are directories for each variable, each holding
#' calibration files for that variable. Defaults to "./"
#' @param NameVarExpc Character vector of minimum variables for which to supply calibration information 
#' (even if there are no applicable calibrations). Default to character(0), which will return cal info 
#' for only the variable directories found in DirCal. 
#' @param TimeBgn A POSIXct timestamp of the start date of interest (inclusive)
#' @param TimeEnd A POSIXct timestamp of the end date of interest (exclusive)
#' @param NumDayExpiMax A data frame indicating the max days since expiration that calibration 
#' information is still considered usable for each variable. Calibrations beyond this allowance period
#' are treated as if they do not exist. Columns in this data frame are:\cr
#' \code{var} Character. Variable name.\cr
#' \code{NumDayExpiMax} Numeric. Max days after expiration that a calibration is considered usable.\cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each element corresponding to the variables found in the DirCal directory
#' and holding a data frame of selected calibrations for the time range of interst as output by 
#' NEONprocIS.cal::def.cal.slct. See that function for details. 

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.meta}


#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
##############################################################################################
wrap.cal.slct <- function(DirCal="./",
                          NameVarExpc=character(0),
                          TimeBgn,
                          TimeEnd,
                          NumDayExpiMax,
                          log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Basic info
  varCal <- base::unique(c(NameVarExpc,base::dir(DirCal)))
  numVarCal <- base::length(varCal)
  
  # Intialize
  metaCal <- base::vector(mode = "list", length = numVarCal) # initialize
  base::names(metaCal) <- varCal # initialize
  calSlct <- metaCal # initialize
  
  # Loop through variables for which calibration information is supplied
  for(idxVarCal in varCal){
    
    # Directory listing of cal files for this data stream
    DirCalVar <- base::paste0(DirCal,'/',idxVarCal)
    fileCal <- base::dir(DirCalVar)
    numCal <- base::length(fileCal) 
    
    # Get metadata for all the calibration files in the directory, saving the valid start/end dates & certificate number
    if(numCal > 0){
      metaCal[[idxVarCal]] <- NEONprocIS.cal::def.cal.meta(fileCal=base::paste0(DirCalVar,'/',fileCal),log=log)
    } else {
      metaCal[[idxVarCal]] <- NULL
    }
    
    # Determine the time period for which each calibration file applies (and whether it is expired)
    NumDayExpiMaxIdx <- NumDayExpiMax$NumDayExpiMax[NumDayExpiMax$var == idxVarCal]
    
    if(base::length(NumDayExpiMaxIdx) == 0 || base::is.na(NumDayExpiMaxIdx)){
      calSlct[[idxVarCal]] <- NEONprocIS.cal::def.cal.slct(metaCal=metaCal[[idxVarCal]],TimeBgn=TimeBgn,TimeEnd=TimeEnd,TimeExpiMax=NULL)
    } else {
      calSlct[[idxVarCal]] <- NEONprocIS.cal::def.cal.slct(metaCal=metaCal[[idxVarCal]],TimeBgn=TimeBgn,TimeEnd=TimeEnd,TimeExpiMax=base::as.difftime(NumDayExpiMaxIdx,units='days'))
    }
  }
  
  return(calSlct)
}
