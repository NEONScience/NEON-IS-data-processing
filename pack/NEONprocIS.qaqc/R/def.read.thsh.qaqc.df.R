##############################################################################################
#' @title Read QAQC thresholds for NEON instrumented systems data products from JSON file to data frame

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read QA/QC threshold information from json file and convert to data frame.

#' @param NameFile Filename (including relative or absolute path). Must be json format.
#' @param strJson character string of data in JSON format (as produced by rjson::toJSON()). Note that
#' only one of NameFile, strJson, or thsh may be entered. If more than one are supplied, the first
#' valid input will be used.
#' @param listThsh A list of thresholds already read in (as produced by rjson::fromJSON()) and extracted 
#' from the main 'thresholds' list. Note that only one of NameFile, strJson, or listThsh may be entered. 
#' If more than one are supplied, the first valid input will be used.

#' @return A data frame with all thresholds contained in the json file. Note that the context entries
#' for each threshold have been combined into a single pipe-delimited string.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso \link[NEONprocIS.qaqc]{def.read.thsh.qaqc.list}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-05-16)
#     original creation
##############################################################################################
def.read.thsh.qaqc.df <- function(NameFile=NULL,
                                  strJson=NULL,
                                  listThsh=NULL){
  
  # Load in the raw json info
  if(!is.null(NameFile)){
    listThsh <- rjson::fromJSON(file=NameFile,simplify=TRUE)
    listThsh <- listThsh$thresholds
  } else if(!base::is.null(strJson)){
    listThsh <- rjson::fromJSON(json_str=strJson,simplify=TRUE)
    listThsh <- listThsh$thresholds
  } else {
    if(base::is.null(listThsh)){
      stop('One of NameFile, strJson, or listThsh must be supplied.')
    }
  }
  
  # Turn all the NULLs into NAs
  listThsh <- base::lapply(listThsh,function(list){
            base::lapply(list,function(valu){
              if(base::is.null(valu) || base::length(valu)==0){
                return(NA)
              } else {
                return(valu)
              }
            })
          })
  
  # Turn context into pipe-separated string
  listThsh <- base::lapply(listThsh,function(idxThsh){
    idxThsh$context <- base::paste0(idxThsh$context,collapse='|')
    return(idxThsh)
  })
  
  # Convert to data frame
  listThsh <- base::lapply(listThsh,base::as.data.frame,stringsAsFactors=FALSE)
  thsh <- base::do.call(base::rbind,listThsh)
  
  # Interpret the dates 
  if(!base::is.null(thsh) && base::nrow(thsh) > 0){
    thsh$start_date <- base::as.POSIXct(thsh$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
    thsh$end_date <- base::as.POSIXct(thsh$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
  }
  
  return(thsh)
}
