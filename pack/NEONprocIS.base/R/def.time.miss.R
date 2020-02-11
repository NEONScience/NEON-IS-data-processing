##############################################################################################
#' @title Determine missing time periods

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Given overall beginning and ending times and a data frame of time ranges, 
#' determine the time periods that are missing. 

#' @param TimeBgn A POSIXct timestamp of the start date of interest (inclusive)
#' @param TimeEnd A POSIXct timestamp of the end date of interest (exclusive)
#' @param timeFull A data frame of non-overlapping time ranges considered covered, with columns: \cr
#' \code{timeBgn} POSIXct timestamps of the start date of the time range (inclusive)\cr
#' \code{timeEnd} POSIXct timestamps of the end date of the time range (exclusive)\cr
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of missing time ranges:\cr
#' \code{timeBgn} POSIXct. The start date-time (inclusive) of the missing time range \cr
#' \code{timeEnd} POSIXct. The end date-time (exclusive) of the missing time range \cr

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples
#' TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
#' TimeEnd <- base::as.POSIXct('2019-01-10',tz='GMT')
#' timeFull <- base::data.frame(timeBgn=as.POSIXct(c('2019-01-02','2019-01-05'),tz='GMT'),
#'                              timeEnd=as.POSIXct(c('2019-01-03','2019-01-07'),tz='GMT'))
#' # NEONprocIS.base::def.time.miss(TimeBgn=TimeBgn,TimeEnd=TimeEnd,timeFull=timeFull)

#' @seealso None currently
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-06)
#     original creation
##############################################################################################
def.time.miss <- function(TimeBgn,TimeEnd,timeFull,log){
  # Sort the covered time ranges
  timeFull <- timeFull[base::order(timeFull$timeBgn,decreasing=FALSE),]
  
  # Initialize
  dmmyTime <-
    base::as.POSIXct(x = numeric(0), origin = as.POSIXct('1970-01-01', tz = 'GMT'))
  base::attr(dmmyTime,'tzone') <- base::attr(TimeBgn,'tzone')
  timeMiss <-
    base::data.frame(
      timeBgn = dmmyTime,
      timeEnd = dmmyTime,
      stringsAsFactors = FALSE
    )
  
  # Look for the entire time period being missing
  if(base::nrow(timeFull) == 0){
    timeMiss <- base::data.frame(timeBgn=TimeBgn,timeEnd=TimeEnd,stringsAsFactors = FALSE)
    return(timeMiss)
  } 
  
  # Look for gap at beginning
  if(TimeBgn < timeFull$timeBgn[1]){
    timeMiss <- base::rbind(timeMiss,base::data.frame(timeBgn=TimeBgn,timeEnd=timeFull$timeBgn[1],stringsAsFactors = FALSE))
  }
  # Look for gap at End
  if(TimeEnd > utils::tail(timeFull$timeEnd,n=1)){
    timeMiss <- base::rbind(timeMiss,base::data.frame(timeBgn=utils::tail(timeFull$timeEnd,n=1),timeEnd=TimeEnd,stringsAsFactors = FALSE))
  }
  
  # Look for gaps between rows
  if(base::nrow(timeFull) > 1){
    
    for(idxRowFull in 2:nrow(timeFull)){
      if(timeFull$timeBgn[idxRowFull] > timeFull$timeEnd[idxRowFull-1]){
        timeMiss <- base::rbind(timeMiss,base::data.frame(timeBgn=timeFull$timeEnd[idxRowFull-1],timeEnd=timeFull$timeBgn[idxRowFull],stringsAsFactors = FALSE))
      }
    }
  }
  
  # Sort the missing periods
  timeMiss <- timeMiss[base::order(timeMiss$timeBgn,decreasing=FALSE),]
  
  return(timeMiss)
}
