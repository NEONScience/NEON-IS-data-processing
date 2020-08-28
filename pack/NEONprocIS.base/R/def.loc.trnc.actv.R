##############################################################################################
#' @title Truncate active periods in location-based location file to date-time range of interest

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Read in a location JSON file for a particular named location
#' and truncate the active periods to the date-time range of interest

#' @param NameFileIn Filename (including relative or absolute path). Must be json format.
#' @param NameFileOut Filename (including relative or absolute path). Must be json format. Defaults to
#' NULL, in which case only the filtered json will be returned in list format
#' @param TimeBgn POSIX timestamp of the start time (inclusive)
#' @param TimeEnd POSIX timestamp of the end time (non-inclusive). Defaults to NULL, in which case the
#' location information will be filtered for the exact time of TimeBgn

#' @return A list of location information with truncated active dates. If NameFileOut is specified, 
#' the truncated location information will also be writted to file in the same json format of NameFileIn

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' TimeBgn <- base::as.POSIXct('2018-01-01',tz='GMT)
#' TimeEnd <- base::as.POSIXct('2018-01-02',tz='GMT)
#' NameFileIn <- 'locations.json'
#' NameFileOut <- 'filtered_locations.json'
#' loc <- def.loc.trnc.actv(NameFileIn,NameFileOut,TimeBgn,TimeEnd)


#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-08-19)
#     original creation
##############################################################################################
def.loc.trnc.actv <-
  function(NameFileIn,
           NameFileOut = NULL,
           TimeBgn,
           TimeEnd = NULL,
           log = NULL) {
    # Initialize log if not input
    if (is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    #
    # First, validate the syntax of input json to see if it is valid
    #
    validateJson <-
      NEONprocIS.base::def.validate.json (NameFileIn)
   
    # Second, validate the json against the schema only if the syntax is valid.
    # Otherwise, validateJsonSchema errors out due to the syntax error
    #
    validateJsonSchema <- FALSE
    if(validateJson == TRUE){
      locJsonSchema <- system.file("extdata", "locations-namedLocation-schema.json", package="NEONprocIS.base")
      validateJsonSchema <-
        NEONprocIS.base::def.validate.json.schema (NameFileIn, locJsonSchema)
    }
    #if the validation fails, the function will not be executed 
    if (validateJson == FALSE || validateJsonSchema == FALSE) {
      log$error(
        base::paste0(
          'In def.loc.trnc.actv::: Erred out due to the json validation failure of this file, ',
          NameFileIn
        )
      )
      stop("In def.loc.trnc.actv::::: Erred out due to the validation failure of the input JSON")
    }

    FmtTime <- '%Y-%m-%dT%H:%M:%SZ' # Time format in the location file
    
    # If NULL, set TimeEnd to 1 second after TimeBgn
    if (base::is.null(TimeEnd)) {
      TimeEnd <- TimeBgn + as.difftime(1, units = 'secs')
    }
    
    # Get formatted character representations of the start and end times
    TimeBgnFmt <- base::format(TimeBgn,format=FmtTime)
    TimeEndFmt <- base::format(TimeEnd,format=FmtTime)
    
    # Load in the raw json info
    loc <- rjson::fromJSON(file = NameFileIn, simplify = FALSE)
    
    # Pull the active dates, and add end dates if not present
    timeActv <- base::lapply(loc$features[[1]]$properties$active_periods,
                             FUN=function(idxList){
                                 nameListIdx <- names(idxList)
                                 if('start_date' %in% nameListIdx){
                                   timeBgnIdx <- base::as.POSIXct(idxList$start_date,format=FmtTime,tz='GMT')
                                 } else {
                                   timeBgnIdx <- TimeBgn
                                 }
                                 if('end_date' %in% nameListIdx){
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
    setKeep <- !(base::unlist(base::lapply(timeActv,is.null)))
    loc$features[[1]]$properties$active_periods <- timeActv[setKeep]
    log$info('Named location file filtered successfully.')
    
    # Write to file
    if (!base::is.null(NameFileOut)) {
      base::write(rjson::toJSON(loc, indent = 4), file = NameFileOut)
      log$info(base::paste0('Filtered named location file written successfully to ',NameFileOut))
    }
  
  return(loc)
}
