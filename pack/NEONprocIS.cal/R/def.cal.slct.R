##############################################################################################
#' @title Determine applicable date ranges for calibration

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Given calibration information from several calibration files (as returned
#' from NEONprocIS.cal::def.cal.met), and a maximum number of days allowed to use expired
#' calibrations after their expiration date, determine the calibrations that apply (and their
#' application time ranges) for a date range of interest. Calibration valid date ranges and
#' calibration certificate numbers (ID) determine the most relevant calibration to apply, following
#' this choice order (1 chosen first):
#'    1. higher ID & date of interest within valid date range
#'    2. lower ID & date of interest within valid date range
#'    3. expired cal with nearest valid end date to beginning date of interest
#'    4. lower ID if expiration dates equal for multipe cals in #3
#' Note that calibrations with a valid date range beginning after the date range of interest and
#' calibrations that are expired more than their max allowable days since expiration are treated
#' as if they don't exist. These time periods in the output will be filled with NA values for the 
#' calibration metadata with the expired flag = TRUE.

#' @param metaCal A data frame of calibration metadata as returned from NEONprocIS.cal::def.cal.meta
#' @param TimeBgn A POSIXct timestamp of the start date of interest (inclusive)
#' @param TimeEnd A POSIXct timestamp of the end date of interest (exclusive)
#' @param TimeExpiMax A difftime object of the maxumum time since expiration for which an expired
#' calibration may be used (e.g. TimeExpiMax <- as.difftime(10,units='days'). Defaults to NULL,
#' which allows an expired calibration to always be used.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of \cr
#' \code{timeBgn} POSIXct. The start date-time (inclusive) for which the calibration applies \cr
#' \code{timeEnd} POSIXct. The end date-time (exclusive) for which the calibration applies \cr
#' \code{file} Character. The name of the calibration file referring to metaCal$file that applies for the date range in columns timeBgn and timeEnd \cr
#' \code{id} Numeric. The calibration id referring to metaCal$id that applies for the date range in columns timeBgn and timeEnd \cr
#' \code{expi} Boolean (TRUE/FALSE). TRUE if the best available calibration is expired for the time period

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords currently none

#' @examples
#' # Not run
#' # fileCal <- c('/path/to/file1.xml','/path/to/file2.xml')
#' # metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=fileCal)
#' # TimeBgn <- base::as.POSIXct('2019-01-01',tz='GMT')
#' # TimeEnd <- base::as.POSIXct('2019-01-02',tz='GMT')
#' # TimeExpiMax <- base::as.difftime(30,units='days') # allow cals to be used up to 30 days after expiration
#' # NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=timeBgn,TimeEnd=timeEnd)

#' @seealso \link[NEONprocIS.cal]{def.cal.meta}
#'
#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-02-06)
#     original creation
#   Cove Sturtevant (2020-12-08)
#     added path to calibration directory in output
##############################################################################################
def.cal.slct <-
  function(metaCal=NULL,
           TimeBgn,
           TimeEnd,
           TimeExpiMax = NULL,
           log = NULL) {
    # Intialize logging if needed
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    
# Initialize the output data frame
    dmmyTime <-
      base::as.POSIXct(x = numeric(0), origin = as.POSIXct('1970-01-01', tz = 'GMT'))
    base::attr(dmmyTime, 'tzone') <- 'GMT'
    rpt <-
      base::data.frame(
        timeBgn = dmmyTime,
        timeEnd = dmmyTime,
        path = base::character(0),
        file = base::character(0),
        id = base::character(0),
        expi = base::logical(0),
        stringsAsFactors = FALSE
      )
    
    # Check inputs: TimeBgn and TimeEnd are POSIXct. TimeEnd > TimeBgn. Validate metaCal is a data frame with columns file, timeValiBgn, timeValiEnd, id. Single difftime value or NULL for TimeExpiMax.
    
    if (!base::is.null(TimeBgn)) {
      TimeBgn <-
        base::as.POSIXct(TimeBgn, format = '%Y-%m-%dT%H:%M:%SZ', tz = 'GMT')
    }
    if (!base::is.null(TimeEnd)) {
      TimeEnd <-
        base::as.POSIXct(TimeEnd, format = '%Y-%m-%dT%H:%M:%SZ', tz = 'GMT')
    }
    
    if (TimeEnd < TimeBgn)  {
      stop("In def.cal.slct::::: Check the input, TimeEnd, needs to be later than TimeBgn")
    }
    
    NameList = c('path', 'file', 'timeValiBgn', 'timeValiEnd', 'id')
    
    if (!base::is.null(metaCal) && !(all(NameList %in% colnames(metaCal))))
    {
      stop("In def.cal.slct::::: Check input metaCal - data frame has columns missing")
    }
    
    # What if metaCal is an empty data frame, or empty? Need to return the time period as if there were no applicable cal. Do so by creating a data frame with expected columns but zero rows.
    if ((base::is.null(metaCal)) || (nrow(metaCal) == 0)){
      metaCal <- base::data.frame(path=base::character(0),
                                  file=base::character(0),
                                  timeValiBgn=dmmyTime,
                                  timeValiEnd=dmmyTime,
                                  id=base::numeric(0),
                                  stringsAsFactors=FALSE)
    }
    
    # Sort the calibration IDs. Higher calibration ID (more recently generated) is always preferrable unless it is expired.
    metaCal <- metaCal[base::order(metaCal$id, decreasing = TRUE),]
    
    # Do initial filtering of calibrations to those that fulfill minimum requirements
    # 1. Remove cals where valid date range begins after time range of interest
    metaCal <- metaCal[metaCal$timeValiBgn < TimeEnd,]
    # 2. Remove cals where time range of interest is after the expiration + allowance period
    if (!base::is.null(TimeExpiMax)) {
      metaCal <- metaCal[metaCal$timeValiEnd + TimeExpiMax > TimeBgn,]
    }
    
    
    # Run through the calibrations, filling in valid periods. 
    # Start with highest ID, which is the most prefereable for a given date-time
    for (idxRow in base::seq_len(base::nrow(metaCal))) {
      # Get an inventory of the remaining time periods without a valid cal 
      timeMiss <-
        NEONprocIS.base::def.time.miss(TimeBgn = TimeBgn,
                                       TimeEnd = TimeEnd,
                                       timeFull = rpt,
                                       log=log)
      
      # Quit early if we've covered the entire range of interest with valid calibrations
      if (base::nrow(timeMiss) == 0) {
        break
      }
      
      # Run through each missing time period, trying to fill it with this calibration (if it's valid)
      for (idxTimeMiss in base::seq_len(base::nrow(timeMiss))) {
        timeBgnIdx <-
          base::max(metaCal$timeValiBgn[idxRow], timeMiss$timeBgn[idxTimeMiss])
        timeEndIdx <-
          base::min(metaCal$timeValiEnd[idxRow], timeMiss$timeEnd[idxTimeMiss])
        
        # Place in the output if there is a valid calibration for this period
        if (timeBgnIdx <= timeEndIdx) {
          rpt <-
            base::rbind(
              rpt,
              base::data.frame(
                timeBgn = timeBgnIdx,
                timeEnd = timeEndIdx,
                path = metaCal$path[idxRow],
                file = metaCal$file[idxRow],
                id = metaCal$id[idxRow],
                expi = FALSE,
                stringsAsFactors = FALSE
              )
            )
        }
      }
      
    } # End filling valid cal periods
    
    # Sort (for ease in debugging)
    rpt <- rpt[base::order(rpt$timeBgn), ]
    
    # Get an inventory of the remaining time periods without a valid cal 
    timeMiss <-
      NEONprocIS.base::def.time.miss(TimeBgn = TimeBgn,
                                     TimeEnd = TimeEnd,
                                     timeFull = rpt,
                                     log=log)
    
    # Now run through the remaining periods without a valid calibration.
    # Fill with most recently expired (if available)
    for (idxTimeMiss in base::seq_len(base::nrow(timeMiss))) {
      fillNa <- FALSE # Initialize for later
      
      # Get the time difference between the beginning of this period and the end date of available calibrations
      timeDiffExpi <-
        timeMiss$timeBgn[idxTimeMiss] - metaCal$timeValiEnd
      base::names(timeDiffExpi) <- metaCal$id
      # Must be expired (since we filled valid cal periods already) & within allowance period
      if (base::is.null(TimeExpiMax)) {
        # no limit on time past expiration
        timeDiffExpi <- timeDiffExpi[timeDiffExpi >= 0]
      } else {
        # Within expiration allowance period
        timeDiffExpi <-
          timeDiffExpi[timeDiffExpi >= 0 & timeDiffExpi < TimeExpiMax]
      }
      
      # Do we have any acceptable expired cals to use?
      if (base::length(timeDiffExpi) > 0) {
        # Choose the one that expired closest to our time range start date
        timeDiffExpi <-
          timeDiffExpi[timeDiffExpi == base::min(timeDiffExpi)]
        idCalExpi <- base::names(timeDiffExpi)
        
        # If there is more than one closest expired cal (expired on same date),
        # take the highest cal ID
        if (base::length(timeDiffExpi) > 1) {
          idCalExpiMax <- base::max(idCalExpi)
          timeDiffExpi <- timeDiffExpi[idCalExpi == idCalExpiMax]
          idCalExpi <- idCalExpiMax
        }
        
        # Get the file associated with this id
        idxRowCalExpi <- base::which(metaCal$id == idCalExpi)[1]
        fileCalExpi <- metaCal$file[idxRowCalExpi]
        
        # Use the cal only up to the allowable expired period
        if (base::is.null(TimeExpiMax)) {
          # We can fill the whole period. No limit on time past expiration
          timeEndExpi <- timeMiss$timeEnd[idxTimeMiss]
        } else {
          # Fill only to the expiration allowance period
          timeEndExpi <-
            base::min(metaCal$timeValiEnd[idxRowCalExpi] + TimeExpiMax,timeMiss$timeEnd[idxTimeMiss])
          
          # Fill in NA values for the time past expiration allowance
          if(timeEndExpi < timeMiss$timeEnd[idxTimeMiss]){
            rpt <-
              base::rbind(
                rpt,
                base::data.frame(
                  timeBgn = timeEndExpi,
                  timeEnd = timeMiss$timeEnd[idxTimeMiss],
                  path = NA,
                  file = NA,
                  id = NA,
                  expi = TRUE,
                  stringsAsFactors = FALSE
                )
              )
          }
        }
        
        # Place expired cal in the output
        rpt <-
          base::rbind(
            rpt,
            base::data.frame(
              timeBgn = timeMiss$timeBgn[idxTimeMiss],
              timeEnd = timeEndExpi,
              path = metaCal$path[idxRowCalExpi],
              file = metaCal$file[idxRowCalExpi],
              id = idCalExpi,
              expi = TRUE,
              stringsAsFactors = FALSE
            )
          )
        
      } else {
        # Cannot fill this missing time period. No expired cal within allowance period
        # Fill file, id, and expi fields with NA
        rpt <-
          base::rbind(
            rpt,
            base::data.frame(
              timeBgn = timeMiss$timeBgn[idxTimeMiss],
              timeEnd = timeMiss$timeEnd[idxTimeMiss],
              path = NA,
              file = NA,
              id = NA,
              expi = TRUE,
              stringsAsFactors = FALSE
            )
          )
        
        next
      }
      
    } # End loop through periods without a valid cal
    
    # Sort for output
    rpt <- rpt[base::order(rpt$timeBgn), ]
    
    return(rpt)
  } # End function
