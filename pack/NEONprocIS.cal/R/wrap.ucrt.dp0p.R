##############################################################################################
#' @title Wrapper for computing individual measurement uncertainty for NEON L0' data

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Compute individual measurement uncertainty for calibrated data at native
#' frequency (NEON L0' data).   

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.  
#' @param FuncUcrt A data frame of the variables for which individual measurement uncertainty is
#' to be calculated. Columns include:\cr
#' \code{var} Character. The variable in data for which to compute uncertainty \cr
#' \code{FuncUcrtMeas} A character string indicating the individual measurement (calibration) uncertainty 
#' function within the NEONprocIS.cal package that should be used. Note that this does not include 
#' FDAS uncertainty. For most NEON data products, this will be "def.ucrt.meas.cnst". Note that any 
#' alternative function must accept the same arguments as def.ucrt.meas.cnst, even if they are unused, and 
#' return the same 
#' See that function for details. \cr
#' \code{FuncUcrtFdas} A character string indicating the FDAS uncertainty function within the NEONprocIS.cal 
#' package that should be used, if FDAS uncertainty applies. This field may be NA, which indicates that
#' FDAS uncertainty does not apply and will not be calculated (e.g. digital L0 output).
#' @param ucrtCoefFdas A data frame of FDAS uncertainty coefficients, as read by 
#' NEONprocIS.cal::def.read.ucrt.coef.fdas. Columns include:\cr
#' \code{Name} Character. Name of the coefficient.\cr
#' \code{Value} Character. Value of the coefficient.\cr
#' \code{.attrs} Character. Relevant attribute (i.e. units)\cr
#' Defaults to NULL, in which case no variables in FuncUcrt may indicate that FDAS uncertainty 
#' applies.
#' @param calSlct A named list of data frames, list element corresponding to the variables in
#' FuncUcrt. The data frame in each list element holds information about the calibration files and 
#' time periods that apply to the variable, as returned from NEONprocIS.cal::def.cal.slct. 
#' See documentation for that function. Assign NULL to list elements (variables) for which calibration
#' information is not applicable (i.e. a function other than def.ucrt.meas.cnst is used to compute its
#' uncertainty).
#' @param mappNameVar A data frame with in/out variable name mapping as produced by 
#' NEONprocIS.base::def.var.mapp.in.out. See documentation for that function. If input (default is NULL),
#' output variable names will be appended as prefixes to the column names in each output data frame. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each element corresponding to those in FuncUcrt$var and holding a data
#' frame of uncertainty data. Note that each row in each data frame corresponds to the times in 
#' data$readout_time, but the variable readout_time is not included in the output. One column
#' in each data frame is labeled ucrtComb, corresponding to the combined measurement uncertainty
#' of the individual measurements and FDAS (if applicable). If FDAS uncertainty does not apply, 
#' ucrtComb is simply a copy of ucrtMeas. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.base]{def.var.mapp.in.out}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
#   Mija Choi (2020-08-14)
#     modified uncertainty function to use, FuncUcrtIdx, by replacing FuncUcrt$ with ParaUcrt$
#   Cove Sturtevant (2020-08-31)
#     adjusted calls to uncertainty funcs to conform to new generic format 
#     This includes inputting the entire data frame, the 
#     variable to be generate uncertainty info for, and the (unused) argument calSlct
#     Changed input to also specify the FDAS uncertainty function to use, instead of 
#     determining it within the code 
#     Changed input argument ParaUcrt to FuncUcrt, and changed input column names to support above changes
#   Cove Sturtevant (2020-12-09)
#     removed DirCal from inputs since the calibration path is now included in calSlct
##############################################################################################
wrap.ucrt.dp0p <- function(data,
                           FuncUcrt,
                           ucrtCoefFdas=NULL,
                           calSlct,
                           mappNameVar=NULL,
                           log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  # Initialize
  ucrtData <- base::vector(mode = "list", length = base::length(FuncUcrt$var))
  base::names(ucrtData) <- FuncUcrt$var

  # Loop through the variables
  for(idxVar in FuncUcrt$var){
    
    # Determine the individual measurement uncertainty function to use
    FuncUcrtMeasIdx <- base::get(FuncUcrt$FuncUcrtMeas[FuncUcrt$var == idxVar], base::asNamespace("NEONprocIS.cal"))
    
    # Get output variable name
    nameVarUcrtOut <- mappNameVar$nameVarOut[mappNameVar$nameVarIn==idxVar]
    
    # Determine whether FDAS uncertainty applies to this variable, and what function
    FuncUcrtFdasIdx <- NULL
    if(!base::is.na(FuncUcrt$FuncUcrtFdas[FuncUcrt$var == idxVar])){
      FuncUcrtFdasIdx <- base::get(FuncUcrt$FuncUcrtFdas[FuncUcrt$var == idxVar], base::asNamespace("NEONprocIS.cal"))
    }

    # Run through each selected calibration and apply the uncertainty function for the applicable time period
    calSlctIdx <- calSlct[[idxVar]]
    for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
      
      # What points in the output correspond to this row?
      setCal <- timeMeas >= calSlctIdx$timeBgn[idxRow] & timeMeas < calSlctIdx$timeEnd[idxRow]
      
      # If a calibration file is available for this period, open it and get calibration information
      if(!base::is.na(calSlctIdx$file[idxRow])){
        fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
        infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE)
      } else {
        infoCal <- NULL
      }
      
      # Pass all calibration information and the data to the calibration uncertainty function
      ucrtMeas <- base::do.call(FuncUcrtMeasIdx,args=base::list(data=base::subset(data,subset=setCal,drop=FALSE),
                                                                infoCal=infoCal,
                                                                varUcrt=idxVar,
                                                                calSlct=calSlct,
                                                                log=log))
      
      
      # Compute FDAS uncertainty, if applicable
      if(!base::is.null(FuncUcrtFdasIdx)){
        
        # Add the FDAS uncertainty coefs to those from the cal file
        infoCal$ucrt <- base::rbind(infoCal$ucrt,ucrtCoefFdas,stringsAsFactors=FALSE)
        
        # Get applicable FDAS uncertainty
        ucrtFdas <- base::do.call(FuncUcrtFdasIdx,args=base::list(data=base::subset(data,subset=setCal,drop=FALSE),
                                                                  infoCal=infoCal,
                                                                  varUcrt=idxVar,
                                                                  calSlct=calSlct,
                                                                  log=log))
        
        # Combine with ucrtMeas
        if(base::nrow(ucrtMeas) != base::nrow(ucrtFdas)){
          log$error('Number of rows returned from measurement calibration uncertainty function do not equal that from FDAS uncertainty function. Something is wrong in the code.')
          stop()
        }
        ucrtMeas <- base::cbind(ucrtMeas,ucrtFdas)
      }
      
      # Put in overall output for this variable
      if(idxRow == 1){
        # Initialize the output with our known columns names
        ucrtDataIdx <-
          base::as.data.frame(
            base::matrix(
              data=as.numeric(NA),
              nrow = base::length(timeMeas),
              ncol = base::ncol(ucrtMeas),
              dimnames = base::list(NULL, base::names(ucrtMeas))
            ),
            stringsAsFactors = FALSE
          )
      }
      # Place this round of uncertainty data in the output
      ucrtDataIdx[setCal,] <- ucrtMeas
      
    } # End loop around selected calibrations
    
    # Compute combined uncertainty for this variable. This will combine (in quadrature) any and all outputs from the 
    # measurement and fdas uncertainty functions that begin with ucrtMeas or ucrtFdas.
    nameVarUcrtIdx <- base::names(ucrtDataIdx)
    nameVarUcrtComb <- nameVarUcrtIdx[base::substr(nameVarUcrtIdx,1,8) %in% c('ucrtMeas','ucrtFdas')]
    
    # Error check
    if(base::length(nameVarUcrtComb) == 0){
      log$warn(base::paste0('No variables output from the measurement uncertainty function ',
                            FuncUcrt$FuncUcrtMeas[FuncUcrt$var == idxVar], 
                            ' or the FDAS uncertainty function ', 
                            FuncUcrt$FuncUcrtFdas[FuncUcrt$var == idxVar],
                            ' begin with ucrtMeas or ucrtFdas. Combined uncertainty for variable ',
                            FuncUcrt$var, ' will be NA')
      )
      ucrtDataIdx$ucrtComb <- NA 
    } else {
      
      # Compute combined uncertainty (ucrtMeas and ucrtFdas)
      ucrtDataIdx <- base::cbind(ucrtDataIdx,NEONprocIS.cal::def.ucrt.comb(ucrt=base::subset(ucrtDataIdx,select=nameVarUcrtComb),log=log))
      
    }

    # Compute expanded uncertainty
    ucrtDataIdx <- base::cbind(ucrtDataIdx,NEONprocIS.cal::def.ucrt.expn(ucrtComb=ucrtDataIdx[['ucrtComb']],log=log))
    
    # Append the output variable name as a prefix to each column
    if(!base::is.null(nameVarUcrtOut)){
      base::names(ucrtDataIdx) <- base::paste0(nameVarUcrtOut,'_',base::names(ucrtDataIdx))
    }
    
    # Place uncertainty for this variable in overall output
    ucrtData[[idxVar]] <- ucrtDataIdx
    
  } # End loop around variables for which to compute individual combined measurement uncertainty 
  
  return(ucrtData)
  
}
