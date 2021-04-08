##############################################################################################
#' @title Wrapper for compiling uncertainty coefficients for all variables and time ranges

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Compute individual measurement uncertainty for calibrated data at native
#' frequency (NEON L0' data).   

#' @param calSlct A named list of data frames, list element corresponding to the variable for which
#' uncertainty coefficients are to be compiled. The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, as returned 
#' from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' @param ucrtCoefFdas A data frame of FDAS uncertainty coefficients, as read by 
#' NEONprocIS.cal::def.read.ucrt.coef.fdas. Columns include:\cr
#' \code{Name} Character. Name of the coefficient.\cr
#' \code{Value} Character. Value of the coefficient.\cr
#' \code{.attrs} Character. Relevant attribute (i.e. units)\cr
#' Defaults to NULL, in which case no FDAS uncertainty coefficients will be combined with calibration
#' uncertanty coefs. Conversely, if this argument is present, the FDAS uncertainty coefficients contained
#' in this argument will be combined (and output) with calibration uncertianty coefs for all variables.
#' @param mappNameVar A data frame with in/out variable name mapping as produced by 
#' NEONprocIS.base::def.var.mapp.in.out. See documentation for that function. If input (default is
#' NULL), input variable names in the output data frames will be replaced by their corresponding 
#' output name.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each element corresponding to those in FuncUcrt$var and holding a data
#' frame of uncertainty coefficients and applicable time ranges.  \cr

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.base]{def.var.mapp.in.out}
#' @seealso \link[NEONprocIS.cal]{def.read.ucrt.coef.fdas}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
#   Cove Sturtevant (2020-12-09)
#     removed DirCal from inputs since the calibration path is now included in calSlct
#   Cove Sturtevant (2021-04-01)
#     fix bug that deleted the directory path to the cal file before all cal files were read in
##############################################################################################
wrap.ucrt.coef <- function(calSlct,
                           ucrtCoefFdas=NULL,
                           mappNameVar=NULL,
                           log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Initialize output
  varCal <- base::names(calSlct)
  ucrtCoef <- base::vector(mode = "list", length = length(varCal)) # Initialize 
  base::names(ucrtCoef) <- varCal
  
  # Loop around variables
  for(idxVar in varCal){
    
    # Grab the L0' term for this L0 variable
    idxVarOut <- mappNameVar$nameVarOut[mappNameVar$nameVarIn==idxVar]
    
    # Get the list of selected calibrations and applicable time periods
    calSlctIdx <- calSlct[[idxVar]]
    
    # Run through each selected calibration & grab the coefficients
    ucrtCoefIdx <- base::vector(mode = "list", length = base::nrow(calSlctIdx))
    for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
      
      # If no calibration file is available for this period, move on. (No coefficients to compile)
      if(base::is.na(calSlctIdx$file[idxRow])){
        next
      } 
      
      # We have a calibration file to open
      fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
      infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
      
      # Add in FDAS uncertainty
      if(!base::is.null(ucrtCoefFdas)){
        # Add the applicable FDAS uncertainty coefs to those from the cal file
        infoCal$ucrt <- base::rbind(infoCal$ucrt,ucrtCoefFdas,stringsAsFactors=FALSE)
      }
      
      # Add in cal metadata to the coefs, excluding the directory path
      infoCal$ucrt$id <- calSlctIdx$id[idxRow]
      if(!base::is.null(idxVarOut)){
        infoCal$ucrt$var <- idxVarOut
      } else {
        infoCal$ucrt$var <- idxVar
      }
      ucrtCoefIdx[[idxRow]] <- base::merge(x=calSlctIdx[idxRow,!(names(calSlctIdx) %in% 'path')],y=infoCal$ucrt,by='id')
      
    } # End loop around selected calibrations 
    
    # Combine coefs for all selected calibrations for this variable
    ucrtCoef[[idxVar]] <- base::Reduce(f=base::rbind,x=ucrtCoefIdx)
    
  } # End loop around variables
  
  return(ucrtCoef)
  
}
