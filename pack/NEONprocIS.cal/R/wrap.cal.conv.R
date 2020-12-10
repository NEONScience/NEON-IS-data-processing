##############################################################################################
#' @title Wrapper for applying calibration conversion to NEON L0 data

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Wrapper function. Apply calibration conversion function to NEON L0 data, thus generating NEON
#' L0' data. 

#' @param data Data frame of L0 data. Must include POSIXct time variable readout_time.  
#' @param calSlct A named list of data frames, list element corresponding to calibrated terms.
#' The data frame in each list element holds 
#' information about the calibration files and time periods that apply to the variable, as returned 
#' from NEONprocIS.cal::def.cal.slct. See documentation for that function. 
#' @param FuncConv A data frame of the terms/variables to convert and the function to convert 
#' them with. Columns include:\cr
#' \code{var} Character. The variable in data to apply calibration to. If this variable does not 
#' exist in the data, it must be created by the associated calibration function in FuncConv. \cr
#' \code{FuncConv} A character string indicating the calibration conversion function  
#' within the NEONprocIS.cal package that should be used. For most NEON data products, this will be 
#' "def.cal.conv.poly". Note that any alternative function must accept the same arguments as 
#' def.cal.conv.poly, even if they are unused. See that function for details. 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of the converted (calibrated) L0' data, limited to the variables in FuncConv.

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
##############################################################################################
wrap.cal.conv <- function(data,
                          calSlct,
                          FuncConv,
                          log=NULL){
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Basic starting info
  timeMeas <- data$readout_time
  
  # Initialize
  varData <- base::names(data)
  varExst <- base::intersect(FuncConv$var,varData) 
  dataConv <- base::subset(data,select=varExst)
  dataConv[] <- NA
  # Tack on any newly created variables to the end
  varNew <- base::setdiff(FuncConv$var,varData)
  if(base::length(varNew) > 0){
    dataConv[[varNew]] <- base::as.numeric(NA)
  }
  
  # Loop through variables
  for(idxVarCal in FuncConv$var){
    
    log$debug(base::paste0('Applying calibration to term: ',idxVarCal))
    
    calSlctIdx <- calSlct[[idxVarCal]]

    # Run through each selected calibration and apply the calibration function for the applicable time period
    for(idxRow in base::seq_len(base::nrow(calSlctIdx))){
      
      # What points in the output correspond to this row?
      setCal <- timeMeas >= calSlctIdx$timeBgn[idxRow] & timeMeas < calSlctIdx$timeEnd[idxRow]
      
      # If a calibration file is available for this period, open it and get calibration information
      if(!base::is.na(calSlctIdx$file[idxRow])){
        fileCal <- base::paste0(calSlctIdx$path[idxRow],calSlctIdx$file[idxRow])
        infoCal <- NEONprocIS.cal::def.read.cal.xml(NameFile=fileCal,Vrbs=TRUE,log=log)
      } else {
        infoCal <- NULL
      }
      
      # Determine the calibration function to use
      FuncConvIdx <- base::get(FuncConv$FuncConv[FuncConv$var == idxVarCal], base::asNamespace("NEONprocIS.cal"))
      
      # Pass the the calibration information to the calibration function
      dataConv[setCal,idxVarCal] <- base::do.call(FuncConvIdx,args=base::list(data=base::subset(data,subset=setCal,drop=FALSE),
                                                                              infoCal=infoCal,
                                                                              varConv=idxVarCal,
                                                                              calSlct=calSlct,
                                                                              log=log)
                                                  )
    }
    
  }
  
  return(dataConv)
  
}
