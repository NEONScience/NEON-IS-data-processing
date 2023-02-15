##############################################################################################
#' @title Filter Science Review Flag file 

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Filter a data frame of SRF flags for a particular time range and return information
#' relevant only to data processing. Any SRF records outside the input time range will be removed from 
#' the output, and values in user_comment, create_date, and update_date are replaced
#' with NA (null is written for these fields in the output json file, if an output file is indicated in 
#' the input parameters). Date-times in the columns start_date and end_date that are outside the time
#' range of interest will be truncated to the time range of interest. Any start_dates or end_dates that
#' fall within the time of interest are not modified.  
#' NOTE: This function does not include error checking of the data frame, since this function is often run 
#' in a large loop after the input json file has already been checked for conformance to the expected schema. 
#' If error checking of the srf contents is desired, use a function like NEONprocIS.qaqc::def.read.srf to 
#' read in the SRF data fed into this function.

#' @param srf Data frame of science review flags, as read from NEONprocIS.qaqc::def.read.srf
#' @param NameFileOut Optional. Filename (including relative or absolute path) to write the filtered output.
#' Defaults to NULL, in which case only the filtered data frame will be returned.
#' @param TimeBgn POSIX timestamp of the start time (inclusive)
#' @param TimeEnd POSIX timestamp of the end time (non-inclusive). Defaults to NULL, in which case the
#' group information will be filtered for a the exact time of TimeBgn
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return The filtered SRF data frame. If NameFileOut is specified, 
#' the truncated information will also be written to file in json format (the same json format as the function 
#' NEONprocIS.qaqc::def.read.srf expects. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # NOT RUN
#' srf <- NEONprocIS.qaqc('/path/to/input/srfs.json')
#' TimeBgn <- base::as.POSIXct('2018-01-01',tz='GMT)
#' TimeEnd <- base::as.POSIXct('2018-01-02',tz='GMT)
#' NameFileOut <- '/path/to/filtered/srfs.json'
#' srfFilt <- def.srf.filt(srf,NameFileOut,TimeBgn,TimeEnd)


#' @seealso \link[NEONprocIS.qaqc]{def.read.srf}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-01-27)
#     original creation
##############################################################################################
def.srf.filt <- function(srf,
                         NameFileOut = NULL,
                         TimeBgn,
                         TimeEnd = NULL,
                         log = NULL
  ) {
    
    # Initialize log if not input
    if (is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }

    FmtTime <- '%Y-%m-%dT%H:%M:%SZ' # Time format
    
    # If NULL, set TimeEnd to 1 second after TimeBgn
    if (base::is.null(TimeEnd)) {
      TimeEnd <- TimeBgn + as.difftime(1, units = 'secs')
    }
    
    # Filter for relevant records based on date range
    srf <- srf[srf$end_date > TimeBgn & srf$start_date <= TimeEnd,]
    
    # End early if no remaining srfs
    if(base::nrow(srf) == 0){
      log$debug(
        base::paste0('No relevant srfs between ',
                     TimeBgn, 
                     ' and ',
                     TimeEnd,
                     '. No file will be written (',
                     NameFileOut,
                     ').')
        )
      return(srf)
    }
    
    # Truncate start/end dates outside TimeBgn to TimeEnd
    srf$start_date[srf$start_date < TimeBgn] <- TimeBgn
    srf$end_date[srf$end_date > TimeEnd] <- TimeEnd

    # Null out user_comment, create_date, and last_update_date
    srf$user_comment <- NA
    srf$create_date <- base::as.POSIXct(NA)
    srf$last_update_date <- base::as.POSIXct(NA)
    
    # Write to file
    if (!base::is.null(NameFileOut)) {
      # Format timestamps for writing to json
      rpt <- srf
      rpt$start_date <- base::format(rpt$start_date,format=FmtTime)
      rpt$end_date <- base::format(rpt$end_date,format=FmtTime)
      rpt <- list(science_review_flags=rpt)
      
      base::write(
        jsonlite::toJSON(x=rpt,
                         dataframe='rows',
                         na='null',
                         pretty=TRUE),
        file = NameFileOut)
      log$debug(base::paste0('Filtered SRF file written successfully to ',NameFileOut))
    }
  
  return(srf)
}
