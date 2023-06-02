##############################################################################################
#' @title Read QAQC thresholds for NEON instrumented systems data products from JSON file to list

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read QA/QC threshold information from json file to list. Convert timestamps to POSIX.

#' @param NameFile Character value. Filename (including relative or absolute path) of the 
#' thresholds file. Must be json format.
#' @param strJson character string of data in JSON format (as produced by rjson::toJSON()). Note that
#' only one of NameFile, strJson, or thsh may be entered. If more than one are supplied, the first
#' valid input will be used.
#' @param listThsh A list of thresholds already read in (as produced by rjson::fromJSON() and extracted 
#' from the main 'thresholds' list. Note that only one of NameFile, strJson, or listThsh may be entered. 
#' If more than one are supplied, the first valid input will be used.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be
#' created and used within the function.

#' @return A list of thresholds, with dates in fields start_date and end_date converted to POSIXct.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' FileThsh <- "~/pfs/threshold/thresholds.json"
#' thshRaw <- rjson::fromJSON(file=FileThsh,simplify=TRUE),silent=FALSE)
#' thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(listThsh=thshRaw$thresholds) # This turns dates to POSIXct, which is required


#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.df}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-14)
#     original creation
##############################################################################################
def.read.thsh.qaqc.list <- function(NameFile=NULL,
                                    strJson=NULL,
                                    listThsh=NULL,
                                    log=NULL){
  
  # Initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  
  # Load in the raw json info
  if(!is.null(NameFile)){
    thshRaw <- base::try(rjson::fromJSON(file=NameFile,simplify=TRUE),silent=FALSE)
    if(base::class(thshRaw) == 'try-error'){
      # Generate error and stop execution
      log$error(base::paste0('Threshold file ', NameFile, ' is unreadable or contains no data. Aborting...')) 
      stop()
    }
    thshRaw <- thshRaw$thresholds
  } else if(!base::is.null(strJson)){
    thshRaw <- rjson::fromJSON(json_str=strJson,simplify=TRUE)
    thshRaw <- thshRaw$thresholds
  } else {
    if(base::is.null(listThsh)){
      stop('One of NameFile, strJson, or listThsh must be supplied.')
    } else {
      thshRaw <- listThsh
      # Account for the scenario in which the thresholds list has not been extracted
      if(base::length(base::names(thshRaw) == 1) && base::names(thshRaw) == 'thresholds'){
        thshRaw <- thshRaw$thresholds
      }
    }
  }
  
  # Turn dates to POSIXct
  thsh <- base::lapply(thshRaw,function(idxThsh){
    if(!base::is.null(idxThsh$start_date)){
      idxThsh$start_date <- base::as.POSIXct(idxThsh$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
    }
    if(!base::is.null(idxThsh$end_date)){
      idxThsh$end_date <- base::as.POSIXct(idxThsh$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
    }
    return(idxThsh)
  })
  
  return(thsh)
  
}

