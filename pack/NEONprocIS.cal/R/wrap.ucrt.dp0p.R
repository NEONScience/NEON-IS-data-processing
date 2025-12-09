##############################################################################################
#' @title Wrapper for computing individual measurement uncertainty for NEON L0' data

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Compute individual measurement and/or FDAS uncertainty for 
#' calibrated data at native frequency (NEON L0' data).   

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.  
#' 
#' @param FuncUcrt A data frame of the functions and variables for which individual measurement 
#' and/or FDAS uncertainty is to be calculated. Columns include:\cr
#' \code{FuncUcrt} A character string indicating the individual measurement (calibration) or FDAS 
#' uncertainty function within the NEONprocIS.cal package. For most NEON data products, 
#' this will be "def.ucrt.meas.cnst" or "def.ucrt.meas.mult" for measurement/calibration 
#' uncertainty, and "def.ucrt.fdas.rstc.poly" or "def.ucrt.fdas.volt.poly" for FDAS 
#' (data acquisition system) uncertainty. Note that any alternative function must accept 
#' the same arguments as these functions, even if they are unused, and return the same 
#' output format. See one of those functions for details. \cr
#' \code{var} Character. The variable(s) in input data frame 'data' that will be used in the 
#' uncertainty function specified in FuncUcrt. In most cases, this will be a single L0 
#' variable for which to compute uncertainty, but it can be any character string so long 
#' as the specified (custom) uncertainty function knows what to do with it. Note that the 
#' uncertainty function is responsible for naming the output list containing 
#' uncertainty data frames for each variable, and that any overlap in the names across 
#' the output list will cause the uncertainty data frames to be combined (intentionally -
#' see return information). Thus, ensure that the column names of data frames for the 
#' same variable (list name) are unique. In the standard measurement and FDAS uncertainty functions, 
#' the output list names will match the name of the L0 variable specified in \code{var}.\cr
#' 
#' @param calSlct A named list of data frames, list elements typically corresponding to the variables in
#' FuncUcrt$var. The data frame in each list element holds information about the calibration files and 
#' time periods that apply to the variable, as returned from NEONprocIS.cal::def.cal.slct. 
#' See documentation for that function. Assign NULL to list elements (variables) for which calibration
#' information is not applicable.
#' 
#' @param Meta (optional). A named list (default is an empty list) containing additional metadata to pass to 
#' calibration and uncertainty functions. This can contain whatever information might be needed in the
#' calibration and/or uncertainty functions in addition to calibration and uncertainty information. 
#' Note that the standard fdas uncertainty functions require fdas uncertainty coefficients to be 
#' provided in Meta$ucrtCoefFdas.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A named list, each list element containing a data frame of uncertainty data for a single
#' variable matching the name of the list element. 
#' Note that each row in each uncertainty data frame corresponds to the times in 
#' data$readout_time, but the variable readout_time is and should not be included in the output. 
#' The data frames for list elements with the same name will be combined, all columns
#' beginning with either ucrtMeas or ucrtFdas across the data frames for that variable
#' will be combined in quadrature and placed into a column labeled ucrtComb.
#' Each output uncertainty data frame will also includes a column named ucrtExpn,
#' which is the expanded (95%) uncertainty of ucrtComb (currently just multiplied by 2).

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.cnst}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.meas.mult}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.rstc.poly}
#' @seealso \link[NEONprocIS.cal]{def.ucrt.fdas.volt.poly}

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
#     Refactor to specify measurement and fdas uncertainty functions in 
#        separate rows of the input parameter FuncUcrt, and allow more flexibility
#        in how uncertainty is computed
#     Shift looping through applicable calibration files within individual ucrt funcs
#     Shift variable naming to each function in order to remove reliance on mappNameVar
##############################################################################################
wrap.ucrt.dp0p <- function(data,
                           FuncUcrt, 
                           calSlct,
                           Meta=list(),
                           log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  numFunc <- base::nrow(FuncUcrt)
  if(base::is.null(numFunc) || numFunc == 0){
    return(list())
  }
  
  # Loop through the uncertainty functions
  ucrt <- base::lapply(base::seq_len(base::nrow(FuncUcrt)),FUN=function(idxFunc){
    
    # Determine the individual measurement uncertainty function to use
    FuncUcrtIdx <- base::get(FuncUcrt$FuncUcrt[idxFunc], base::asNamespace("NEONprocIS.cal"))
    
    # Pass the the uncertainty information to the uncertainty function. 
    # ucrtMeasIdx should be a named list, each list element named for a variable for which the 
    #   uncertainty estimates correspond
    varUcrtIdx <- base::unique(base::unlist(base::strsplit(FuncUcrt$var[idxFunc],"|",fixed=TRUE)))
    ucrtMeasIdx <- base::do.call(FuncUcrtIdx,args=base::list(data=data,
                                                             varUcrt=varUcrtIdx,
                                                             calSlct=calSlct,
                                                             Meta=Meta,
                                                             log=log)
    )
    return(ucrtMeasIdx)
  })  
  
  # Un-nest the list of lists
  ucrt <- unlist(ucrt,recursive=FALSE)
    
  # Compute combined & expanded uncertainty for each variable. 
  #   This will combine (in quadrature) any and all outputs from the measurement 
  #   and fdas uncertainty functions that begin with ucrtMeas or ucrtFdas, 
  #   then expand to 95% confidence
  varAll <-base::unique(base::names(ucrt))
  ucrtData <- base::lapply(varAll,FUN=function(varIdx){
    
    # Error-check that the column names across data frames for this var are unique
    nameColVar <- base::unlist(base::lapply(ucrt[base::names(ucrt) == varIdx],base::names))
    if(base::length(nameColVar) != base::unique(base::length(nameColVar))){
      log$warn(base::paste0("Uncertainty data column names for variable: "),
               varIdx,
               " are not unique. Output columns names will not conform to expectations.")
    }
    ucrtDataIdx <- base::do.call(base::cbind,ucrt[base::names(ucrt) == varIdx])
    base::names(ucrtDataIdx) <- nameColVar 

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
                                 NEONprocIS.cal::def.ucrt.comb(ucrt=ucrtDataIdx[,nameVarUcrtComb,drop=FALSE],log=log)
      )
      
    }
    
    # Compute expanded uncertainty
    ucrtDataIdx <- base::cbind(ucrtDataIdx,
                               NEONprocIS.cal::def.ucrt.expn(ucrtComb=ucrtDataIdx[['ucrtComb']],log=log)
                               )
    
    return(ucrtDataIdx)

  })
  
  # Assign variable names to output list elements
  base::names(ucrtData) <- varAll
  
  return(ucrtData)
  
}
