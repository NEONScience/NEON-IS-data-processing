##############################################################################################
#' @title Wrapper for computing individual measurement uncertainty for NEON L0' data

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Compute individual measurement uncertainty for calibrated data at native
#' frequency (NEON L0' data).   

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.  
#' 
#' @param FuncUcrt A data frame of the variables for which individual measurement uncertainty is
#' to be calculated. Columns include:\cr
#' \code{FuncUcrtMeas} A character string indicating the individual measurement (calibration) uncertainty 
#' function within the NEONprocIS.cal package that should be used. Note that this does not include 
#' FDAS uncertainty. For most NEON data products, this will be "def.ucrt.meas.cnst". Note that any 
#' alternative function must accept the same arguments as def.ucrt.meas.cnst, even if they are unused, and 
#' return the same 
#' See that function for details. \cr
#' \code{FuncUcrtFdas} A character string indicating the FDAS uncertainty function within the NEONprocIS.cal 
#' package that should be used, if FDAS uncertainty applies. This field may be NA, which indicates that
#' FDAS uncertainty does not apply and will not be calculated (e.g. L0 data is a digital measurement).\cr
#' \code{var} Character. The variable(s) in input data frame 'data' that will be used in the 
#' uncertainty function specified in FuncUcrtMeas. In most cases, this will be a single L0 variable for 
#' which to compute uncertainty, but it can be any character string so long as the specified (custom) 
#' uncertainty function knows what to do with it. Note that the uncertainty function is responsible
#' for naming the output list elements containing uncertainty data frames for each variable, the names should
#' be unique across the output list. This function simply appends them.\cr
#' 
#' @param FuncUcrtFdas
#' 
#' 
#' @param calSlct A named list of data frames, list element corresponding to the variables in
#' FuncUcrt. The data frame in each list element holds information about the calibration files and 
#' time periods that apply to the variable, as returned from NEONprocIS.cal::def.cal.slct. 
#' See documentation for that function. Assign NULL to list elements (variables) for which calibration
#' information is not applicable (i.e. a function other than def.ucrt.meas.cnst is used to compute its
#' uncertainty).
#' 
#' @param Meta (optional). A named list (default is an empty list) containing additional metadata to pass to 
#' calibration and uncertainty functions. This can contain whatever information might be needed in the
#' calibration and/or uncertainty functions in addition to calibration and uncertainty information. 
#' Note that most/all fdas uncertainty functions require fdas uncertainty coefficients to be 
#' provided in Meta$ucrtCoefFdas.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each list element containing a data frame of uncertainty data for a single
#' variable matching the name of the list element. Note that the names of each list element are 
#' produced by the uncertainty functions themselves, and should be unique across the output list.
#' Note that each row in each data frame corresponds to the times in 
#' data$readout_time, but the variable readout_time is not included in the output. One column
#' in each data frame is labeled ucrtComb, corresponding to the combined measurement uncertainty
#' of the individual measurements and FDAS (if applicable). If FDAS uncertainty does not apply, 
#' ucrtComb is simply a copy of ucrtMeas. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}

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
#   Cove Sturtevant (2025-06-23)
#     accept Meta object for passing additional metadata to uncertainty functions
#   Cove Sturtevant (2025-09-16)
#     Refactor to loop through applicable calibration files within individual ucrt funcs
#     Also shift variable naming to each function in order to remove reliance on mappNameVar
##############################################################################################
wrap.ucrt.dp0p <- function(data,
                           FuncUcrtMeas,
                           FuncUcrtFdas=NULL, # Compute separately
                           calSlct,
                           Meta=list(),
                           log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  # Loop through the UcrtMeas functions
  ucrtMeas <- base::lapply(base::seq_len(base::nrow(FuncUcrtMeas)),FUN=function(idxFunc){
    
    # Determine the individual measurement uncertainty function to use
    FuncUcrtMeasIdx <- base::get(FuncUcrtMeas$FuncUcrtMeas[idxFunc], base::asNamespace("NEONprocIS.cal"))
    
    # Pass the the uncertainty information to the uncertainty function. 
    # ucrtMeasIdx should be a named list, each list element named for a variable for which the 
    #   uncertainty estimates correspond
    varUcrtIdx <- base::unique(base::unlist(base::strsplit(FuncUcrtMeas$var[idxFunc],"|",fixed=TRUE)))
    ucrtMeasIdx <- base::do.call(FuncUcrtMeasIdx,args=base::list(data=data,
                                                                 varUcrt=varUcrtIdx,
                                                                 calSlct=calSlct,
                                                                 Meta=Meta,
                                                                 log=log)
    )
    return(ucrtMeasIdx)
  })  
  
  # Un-nest the list of lists
  ucrtMeas <- unlist(ucrtMeas)
    
  # Loop through the UcrtFdas functions
  ucrtFdas <- base::lapply(base::seq_len(base::nrow(FuncUcrtFdas)),FUN=function(idxFunc){

    # Determine the individual fdas uncertainty function to use
    FuncUcrtFdasIdx <- base::get(FuncUcrtFdas$FuncUcrtFdas[idxFunc], base::asNamespace("NEONprocIS.cal"))
    
    # Pass in the fdas uncertainty in Meta
    varUcrtIdx <- base::unique(base::unlist(base::strsplit(FuncUcrtFdas$var[idxFunc],"|",fixed=TRUE)))
    ucrtFdasIdx <- base::do.call(FuncUcrtFdasIdx,args=base::list(data=data,
                                                                 varUcrt=varUcrtIdx,
                                                                 calSlct=calSlct,
                                                                 Meta=Meta,
                                                                 log=log)
    )
    return(ucrtFdasIdx)
  })  

  # Un-nest the list of lists
  ucrtFdas <- unlist(ucrtFdas)
  
  # Compute combined & expanded uncertainty for each variable. 
  #   This will combine (in quadrature) any and all outputs from the measurement 
  #   and fdas uncertainty functions that begin with ucrtMeas or ucrtFdas, 
  #   then expand to 95% confidence
  varAll <-base::unique(base::names(ucrtMeas),base::names(ucrtFdas))
  ucrtData <- base::lapply(varAll,FUN=function(varIdx){
    ucrtMeasVar <- ucrtMeas[[varIdx]]
    ucrtFdasVar <- ucrtFdas[[varIdx]]
    
    # Combine ucrtMeas and ucrtFdas
    if (base::is.null(ucrtFdasVar)){
      ucrtDataIdx <- ucrtMeasVar
    } else if base::is.null(ucrtMeasVar){
      ucrtDataIdx <- ucrtFdasVar
    } else {
      
      # Error-check
      if(base::nrow(ucrtMeasVar) != base::nrow(ucrtFdasVar)){
        log$error(paste0('Number of rows returned from measurement uncertainty function for variable ',varIdx,' do not equal that from FDAS uncertainty function. Something is wrong in the code.'))
        stop()
      }
      
      # Combine ucrtMeas and ucrtFdas
      ucrtDataIdx <- base::cbind(ucrtMeasVar,ucrtFdasVar)
    }
    
    # Compute combined uncertainty for each variable. 
    nameVarUcrtIdx <- base::names(ucrtDataIdx)
    nameVarUcrtComb <- nameVarUcrtIdx[base::substr(nameVarUcrtIdx,1,8) %in% c('ucrtMeas','ucrtFdas')]
    
    # Error check
    if(base::length(nameVarUcrtComb) == 0){
      log$warn(base::paste0('No columns in the uncertainty output for ',
                            varIdx,
                            ' begin with ucrtMeas or ucrtFdas. Combined uncertainty for ',
                            varIdx, 
                            ' will be NA')
      )
      ucrtDataIdx$ucrtComb <- NA 
    } else {
      
      # Compute combined uncertainty (ucrtMeas and ucrtFdas)
      ucrtDataIdx <- base::cbind(ucrtDataIdx,
                                 NEONprocIS.cal::def.ucrt.comb(ucrt=ucrtDataIdx[,nameVarUcrtComb,drop=FALSE]),log=log)
      )
      
    }
    
    # Compute expanded uncertainty
    ucrtDataIdx <- base::cbind(ucrtDataIdx,
                               NEONprocIS.cal::def.ucrt.expn(ucrtComb=ucrtDataIdx[['ucrtComb']],log=log)
                               )
    
    return(ucrtDataIdx)

  })
  
  return(ucrtData)
  
}
