##############################################################################################
#' @title Truncate active periods in group file to date-time range of interest

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read in a group JSON file and truncate the active periods to the date-time range of interest. 
#' NOTE: This function does not include error checking of the input json, since this function is often run 
#' in a large loop after the input json has already been checked for conformance to the expected schema. 
#' If error checking of the input json is desired, use a function like NEONprocIS.base::def.loc.meta prior
#' to executing this function.

#' @param NameFileIn Filename (including relative or absolute path). Must be json format.
#' @param NameFileOut Filename (including relative or absolute path). Must be json format. Defaults to
#' NULL, in which case only the filtered json will be returned in list format
#' @param TimeBgn POSIX timestamp of the start time (inclusive)
#' @param TimeEnd POSIX timestamp of the end time (non-inclusive). Defaults to NULL, in which case the
#' group information will be filtered for the exact time of TimeBgn

#' @return The file in list format with truncated active dates. If NameFileOut is specified, 
#' the truncated information will also be writted to file in the same json format of NameFileIn. 
#' If an active period is empty or becomes empty during truncation (completely outside the range of interest), the 
#' entire group is removed. If no active groups remain, no file is written. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' TimeBgn <- base::as.POSIXct('2018-01-01',tz='GMT)
#' TimeEnd <- base::as.POSIXct('2018-01-02',tz='GMT)
#' NameFileIn <- 'groups.json'
#' NameFileOut <- 'filtered_groups.json'
#' listFile <- def.grp.trnc.actv(NameFileIn,NameFileOut,TimeBgn,TimeEnd)


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2022-11-23)
#     original creation, from def.loc.trnc.actv
##############################################################################################
def.grp.trnc.actv <- function(
                             NameFileIn,
                             NameFileOut = NULL,
                             TimeBgn,
                             TimeEnd = NULL,
                             log = NULL
  ) {
    
    # Initialize log if not input
    if (is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }

    FmtTime <- '%Y-%m-%dT%H:%M:%SZ' # Time format in the file
    
    # If NULL, set TimeEnd to 1 second after TimeBgn
    if (base::is.null(TimeEnd)) {
      TimeEnd <- TimeBgn + as.difftime(1, units = 'secs')
    }
    
    # Get formatted character representations of the start and end times
    TimeBgnFmt <- base::format(TimeBgn,format=FmtTime)
    TimeEndFmt <- base::format(TimeEnd,format=FmtTime)
    
    # Load in the raw json info
    listFile <- rjson::fromJSON(file = NameFileIn, simplify = FALSE)
    
    # Run through each group that is present in the file
    for (idx in base::seq_len(base::length(listFile$features))){
      
      # Pull the active dates, and add start/end dates if not present
      timeActv <- base::lapply(listFile$features[[idx]]$properties$active_periods,
                               FUN=function(idxList){
                                   if(!base::is.null(idxList$start_date)){
                                     timeBgnIdx <- base::as.POSIXct(idxList$start_date,format=FmtTime,tz='GMT')
                                   } else {
                                     timeBgnIdx <- TimeBgn
                                   }
                                   if(!base::is.null(idxList$end_date)){
                                     timeEndIdx <- base::as.POSIXct(idxList$end_date,format=FmtTime,tz='GMT')
                                   } else {
                                     timeEndIdx <- TimeEnd
                                   }
                                   if(timeBgnIdx < TimeEnd && timeEndIdx > TimeBgn){
                                     # This is a relevant set of active dates. 
                                     # Truncate the active range to our range of interest
                                     if(timeBgnIdx <= TimeBgn){
                                       timeBgnIdx <- TimeBgnFmt
                                     } else {
                                       timeBgnIdx <- idxList$start_date
                                     }
                                     if(timeEndIdx >= TimeEnd){
                                       timeEndIdx <- TimeEndFmt
                                     } else {
                                       timeEndIdx <- idxList$end_date
                                     }
                                     
                                     return(list(start_date=timeBgnIdx,end_date=timeEndIdx))
                                   } else {
                                     return(NULL)
                                   }
                               }
      )
      
      # Get rid of the active dates we nulled out
      if (base::length(timeActv) > 0){
        setKeep <- !(base::unlist(base::lapply(timeActv,is.null)))
        listFile$features[[idx]]$properties$active_periods <- timeActv[setKeep]
      }
      
    } # End loop around feature list (number of groups)
    
    # Get rid of the groups with no active periods left
    numActv <- base::unlist(
      base::lapply(listFile$features,FUN=function(idxGrp){
        length(idxGrp$properties$active_periods)
      })
    )
    listFile$features[numActv == 0] <- NULL
    if(base::length(listFile$features) == 0){
      listFile <- NULL
      log$debug(base::paste0('No applicable active periods for time period ',TimeBgn,' to ',TimeEnd, ' in ',NameFileIn))
    }
    
    # Write to file
    if (!base::is.null(NameFileOut) && !base::is.null(listFile)) {
      base::write(rjson::toJSON(listFile, indent = 4), file = NameFileOut)
      log$debug(base::paste0('Filtered group file written successfully to ',NameFileOut))
    }
  
  return(listFile)
}
