##############################################################################################
#' @title Assign the calibration file(s) for a sensor ID to each data day for which each should be used

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Assign the calibration file(s) for a sensor ID to each data day which it applies between a 
#' given start and end date.
#' Valid date ranges and certificate numbers in calibration files are used to determine the most applicable
#' calibration for each data day. The most applicable cal follows this choice order (1 chosen first):
#'    1. higher ID & date of interest within valid date range
#'    2. lower ID & date of interest within valid date range
#'    3. expired cal with nearest valid end date to beginning date of interest
#'    4. lower ID if multiple cals wtih same expiration dates in #3
#' Note that calibrations with a valid date range beginning after the data day of interest are treated
#' as if they don't exist, which expired calibrations are considered applicable after the valid date
#' range if no other valid calibration exists.
#'     
#' @param DirIn Character value. The input path to the calibration files from a single sensor ID and term,
#' structured as follows: \cr
#' #/pfs/BASE_REPO/SOURCE_TYPE/SOURCE_ID/TERM \cr
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' The TERM folder holds any number of calibration files pertaining to the SOURCE_ID and TERM combination.  
#' There may be no further subdirectories of TERM.\cr
#'
#' For example: \cr
#' Input path = /scratch/pfs/proc_group/prt/27134/resistenace \cr
#'     
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param TimeBgn POSIX. The minimum date for which to assign calibration files.
#' @param TimeEnd POSIX. The maximum date for which to assign calibration files.
#' @param PadDay (optional). 2-element difftime object with units of days indicating the days to include applicable 
#' calibration files before/after a given data day. A negative value will copy in the calibration file(s) 
#' that are applicable to the given data day AND # number of days before the data day. A positive value 
#' will copy in the calibration file(s) applicable to the given data day AND # number of days after the data day. 
#' Default is 0. For example, if the current data day is 2019-01-15, "PadDay=-2" will copy in any calibration file(s)
#' that are applicable between 2019-01-13 00:00 and 2019-01-15 24:00. "PadDay=2" will copy in calibration file(s) 
#' that are applicable between 2019-01-15 00:00 and 2019-01-17 24:00. To provide both negative and positive pads 
#' (a window around a given day), separate the values with pipes (e.g. "PadDay=-2|2"). 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A directory structure in the format DirOutBase/SOURCE_TYPE/YEAR/MONTH/DAY/SOURCE_ID/calibration/TERM, 
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the 
#' terminal path (TERM) is populated with the calibration files applicable to the year, month, day, source_id, 
#' and term indicated in the path (plus any additional calibration files according to the PadDay input). 
#'  

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.cal.asgn(DirIn="/pfs/tempSoil_context_group/prt/25658",
#'              DirOutBase="/pfs/out",
#'              TimeBgn=as.POSIXct('2019-01-01',tz='GMT),
#'              TimeEnd=as.POSIXct('2019-06-01',tz='GMT),
#'              PadDay=as.difftime(c(-1,1),units='days')
#'              )

#' @seealso None
#' 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-03-04)
#     original creation
##############################################################################################
wrap.cal.asgn <- function(DirIn,
                          DirOutBase,
                          TimeBgn,
                          TimeEnd,
                          PadDay=base::as.difftime(c(0,0),units='days'),
                          log=NULL
                          ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Directory listing of cal files for this data stream
  fileCal <- base::dir(DirIn)
  
  # Move on if no calibration files
  if(base::length(fileCal) == 0){
    log$info(
      base::paste0(
        'No calibration files in datum path ',
        DirIn,
        '. Skipping...'
      )
    )
    return()
  }
  
  
  # Get metadata for all the calibration files in the directory, saving the valid start/end dates & certificate number
  metaCal <-
    NEONprocIS.cal::def.cal.meta(fileCal = base::paste0(DirIn, '/', fileCal),
                                 log = log)
  
  # Determine the calibrations that apply for each time period
  calSlct <-
    NEONprocIS.cal::def.cal.slct(
      metaCal = metaCal,
      TimeBgn = TimeBgn,
      TimeEnd = TimeEnd,
      log = log
    )
  calSlct <- calSlct[!base::is.na(calSlct$file),] # Get rid of periods where no appropriate cal exists
  
  # Move on if no calibrations apply during the time interval
  if(base::nrow(calSlct) == 0){
    log$info(
      base::paste0(
        'No calibration files in datum path ',
        DirIn,
        ' apply to time interval: ',
        TimeBgn,
        ' to ',
        TimeEnd,
        '. Skipping...'
      )
    )
    return()
  }
  
  # Get the date sequence 
  ts <- seq.POSIXt(from=base::trunc(base::min(calSlct$timeBgn),units='days'),
                   to=base::trunc(base::max(calSlct$timeEnd),units='days'),
                   by='day')
  ts <- ts[ts != TimeEnd] # Get rid of final date
  tsChar <- format(ts,format='%Y/%m/%d') # Format as year/month/day repo structure
  
  # Create the output directory structure
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  typeSrc <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+1]
  idSrc <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+2]
  term <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+3]
  dirOut <- base::paste0(DirOutBase,'/',typeSrc,'/',tsChar,'/',idSrc,'/calibration/',term)
  NEONprocIS.base::def.dir.crea(DirSub=dirOut,log=log)
  
  # Populate the directory structure with the appropriate calibrations
  for(idxPrd in base::seq_len(base::nrow(calSlct))){
    # What dates folders does this calibration belong in?
    setDir <- ts >= (calSlct$timeBgn[idxPrd]- base::as.difftime(1, units = 'days') - PadDay[2]) &
      ts < (calSlct$timeEnd[idxPrd]-PadDay[1])
    
    # Copy the file to the appropriate output directories
    if (base::sum(setDir) > 0) {
      base::system(
        base::paste0(
          'ln -s ',
          calSlct$path[idxPrd],
          calSlct$file[idxPrd],
          ' ',
          dirOut[setDir],
          collapse = ' && '
        )
      )
    }
    
    log$info(
      base::paste0(
        calSlct$file[idxPrd],
        ' copied to all daily folders between ',
        dirOut[setDir][1],
        ' and ',
        utils::tail(dirOut[setDir],1)
      )
    )
    
  }
  
  return()

}
