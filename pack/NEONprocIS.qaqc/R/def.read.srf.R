##############################################################################################
#' @title Read Science Review Flags for NEON instrumented systems data products from JSON file to data frame

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read Science Review Flags information from json file and convert to data frame.

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param strJson character string of data in JSON format (as produced by rjson::toJSON()). Note that
#' only one of NameFile or strJson may be entered. If more than one are supplied, the first
#' valid input will be used.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A data frame with all science review flags contained in the json file. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.list}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-01-27)
#     original creation
##############################################################################################
def.read.srf <- function(NameFile=NULL,
                         strJson=NULL,
                         log=NULL)
  {
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Load in the raw json info
  if(!is.null(NameFile)){
    listSrf <- rjson::fromJSON(file=NameFile,simplify=TRUE)
    listSrf <- listSrf$science_review_flags
  } else if(!base::is.null(strJson)){
    listSrf <- rjson::fromJSON(json_str=strJson,simplify=TRUE)
    listSrf <- listSrf$science_review_flags
  } else {
    log$error('NameFile or strJson must be supplied.')
  }
  
  # Turn all the NULLs into NAs
  listSrf <- base::lapply(listSrf,function(list){
            base::lapply(list,function(valu){
              if(base::is.null(valu) || base::length(valu)==0){
                return(NA)
              } else {
                return(valu)
              }
            })
          })
  

  # Convert to data frame
  listSrf <- base::lapply(listSrf,base::as.data.frame,stringsAsFactors=FALSE)
  srf <- base::do.call(base::rbind,listSrf)
  
  # Interpret the dates 
  if(!base::is.null(srf) && base::nrow(srf) > 0){
    srf$start_date <- base::as.POSIXct(srf$start_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
    srf$end_date <- base::as.POSIXct(srf$end_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
    srf$create_date <- base::as.POSIXct(srf$create_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
    srf$last_update_date <- base::as.POSIXct(srf$last_update_date,format='%Y-%m-%dT%H:%M:%SZ',tz='GMT')
  }
  
  return(srf)
}
