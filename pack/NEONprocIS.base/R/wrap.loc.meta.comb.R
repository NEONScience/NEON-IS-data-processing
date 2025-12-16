##############################################################################################
#' @title Combine location metadata read from asset and/or named location files

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read in and merge location metadata from asset-focused location
#' file(s) and/or named-location-focused file(s). Any geolocation information found in the files 
#' is included.

#' @param NameFile String array of filenames (including relative or absolute paths) of location files. Must be json format.
#' @param NameLoc Character value of the named location to restrict output to. Defaults to NULL, 
#' in which case no filtering is done for named location
#' @param TimeBgn POSIXct timestamp of the start time of interest (inclusive). Defaults to NULL, 
#' in which case no filtering is done for installed time range. Note that 
#' no time filtering is performed for location-based location files, since there is no sensor install 
#' information included, and only one location is included in the location-based location files.
#' @param TimeEnd POSIXct timestamp of the end time of interest (non-inclusive). Defaults to NULL, in 
#' which case the location information will be filtered for the exact time of TimeBgn. Note that 
#' no time filtering is performed for location-based location files, since there is no sensor install 
#' information included, and only one location is included in the location-based location files.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return 
#' A list, length equal to the number of named locations found in the set of input
#' files. Each named location list element contains a nested list of location properties.
#' Note that location properties for each named location are populated with the first non-empty value
#' found among the files.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples 
#' # Not run
#' # NameFile <- c('prt_767_locations.json','CFGLOC100140.json')
#' # NameLoc <- 'CFGLOC100140'
#' # TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT)
#' # TimeEnd <- base::as.POSIXct('2019-01-02',tz='GMT)
#' # locMeta <- NEONprocIS.base::wrap.loc.meta.comb(NameFile=NameFile,NameLoc=NameLoc,TimeBgn=TimeBgn,TimeEnd=TimeEnd)

#' @seealso \link[NEONprocIS.base]{def.loc.meta}
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2025-06-18)
#     original creation
##############################################################################################
wrap.loc.meta.comb <- function(NameFile,
                               NameLoc=NULL,
                               TimeBgn=NULL,
                               TimeEnd=NULL,
                               log=NULL
                               ){

  # Initialize log if not input
  if (is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Read in location files
  numFile <- length(NameFile)
  loc <- base::lapply(NameFile,FUN=function(fileLocIdx){
    
    # Basic location properties
    locMeta <- NEONprocIS.base::def.loc.meta(fileLocIdx,
                                             NameLoc = NameLoc,
                                             TimeBgn = TimeBgn,
                                             TimeEnd = TimeEnd,
                                             log = log)
    
    # Geolocation data
    locGeo <- list()
    try({
      locGeo <- NEONprocIS.base::def.loc.geo.hist(fileLocIdx,log = log)
      locGeo <- base::lapply(locGeo,FUN=function(locListIdx){list(geolocations=locListIdx)})
    },
    silent=TRUE)
    
    # Combine basic location properties and geolocation data
    for(idxLoc in base::seq_len(base::nrow(locMeta))){
      nameLoc <- locMeta$name[idxLoc]
      locGeo[[nameLoc]] <- base::append(locGeo[[nameLoc]],base::as.list(locMeta[idxLoc,]))
    }
    
    return(locGeo)
  })
  
  # Merge together information from multiple location files
  locAll <- list()
  nameLoc <- base::unique(base::unlist(base::lapply(loc,base::names)))
  for (nameLocIdx in nameLoc){
    
    # Combine list fields across all files
    keys <- base::unique(base::unlist(base::lapply(loc,FUN=function(fileIdx){names(fileIdx[[nameLocIdx]])})))
    
    for(key in keys){
      
      # Take first non-empty value for this key
      for (fileIdx in base::seq_len(numFile)){
        valu <- loc[[fileIdx]][[nameLocIdx]][[key]]
        valuExst <- locAll[[nameLocIdx]][[key]]
        if(is.null(valuExst) || base::is.na(valuExst)){
          locAll[[nameLocIdx]][[key]] <- valu
        }
      } # End loop around files
    } # End loop around keys
  } # End loop around named locations

  return(locAll)
  
}
