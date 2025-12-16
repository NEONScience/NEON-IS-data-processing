##############################################################################################
#' @title Wrapper for applying calibration conversion to NEON L0 data

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Apply calibration conversion function to NEON L0 data, thus generating NEON
#' L0' data. 

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.  
#' 
#' @param calSlct A named list of data frames, list element corresponding to calibrated terms.
#' The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, as returned 
#' from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' 
#' @param FuncConv A data frame indicating the calibration functions to apply and (optionally) the 
#' L0 terms to apply them to. The columns of the data frame are:
#' \code{FuncConv}: Character. The calibration conversion function within the NEONprocIS.cal package . Note that 
#' any and all calibration functions specified here must accept arguments "data", "infoCal", "varCal", "slctCal", 
#' "Meta", and "log", even if they are unused in the function. See any def.cal.conv.____.R 
#' \code{var}: Character. The name of the variable/term to be calibrated. Typically this will be a single L0 term matching
#' a column in the input data frame. However, it can be a term not found in the input data frame, multiple terms separated 
#' by pipes (e.g. "resistance|voltage") or no term at all (indicated by an NA). These uncommon cases are acceptable so long 
#' as the calibration conversion function is able to handle the case, for example if multiple L0 terms are used to create 
#' a single calibrated output. \cr
#' 
#' @param Meta (optional). A named list (default is an empty list) containing additional metadata to pass to 
#' calibration and uncertainty functions. This can contain whatever information might be needed in the
#' calibration and/or uncertainty functions in addition to calibration and uncertainty information. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of the converted (calibrated) L0' data after running through the 
#' calibration functions listed in FuncConv. Note that the input data frame \code{data} 
#' is passed into each function in sequence, where the input to each subsequent function
#' is the data frame output by the previous function. 

#' @references Currently none

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.cal]{def.cal.slct}
#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#' @seealso \link[NEONprocIS.cal]{def.cal.conv.poly}
#' @seealso \link[NEONprocIS.cal]{def.cal.func.poly}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-13)
#     original creation
#   Cove Sturtevant (2020-08-31)
#     adjusted calls to cal funcs to conform to new generic format
#     This includes inputting the entire data frame, the 
#     variable to be calibrated, and the (unused) argument calSlct
#   Cove Sturtevant (2020-12-09)
#     removed DirCal from inputs since the calibration path is now included in calSlct
#   Cove Sturtevant (2025-06-23)
#     accept Meta object for passing additional metadata to calibration functions
#   Cove Sturtevant (2025-08-10)
#     Refactor to loop through applicable calibration files within individual cal funcs
##############################################################################################
wrap.cal.conv.dp0p <- function(data,
                               calSlct,
                               FuncConv,
                               Meta=list(),
                               log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Loop through rows of FuncConv
  for(idxFunc in base::seq_len(nrow(FuncConv))){

    log$debug(base::paste0('Applying calibration function: ', FuncConv$FuncConv[idxFunc], ' with indicated term(s): ',FuncConv$var[idxFunc]))
    
    # Get the calibration function
    FuncConvIdx <- base::get(FuncConv$FuncConv[idxFunc], base::asNamespace("NEONprocIS.cal"))
    
    # Pass the the calibration information to the calibration function. 
    # Note that "data" is updated with each function call and passed to subsequent functions
    varConvIdx <- base::unique(base::unlist(base::strsplit(FuncConv$var[idxFunc],"|",fixed=TRUE)))
    data <- base::do.call(FuncConvIdx,args=base::list(data=data,
                                                      varConv=varConvIdx,
                                                      calSlct=calSlct,
                                                      Meta=Meta,
                                                      log=log)
    )
    
  }
  
  return(data)
  
}
