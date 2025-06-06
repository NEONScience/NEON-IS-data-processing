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
#' @param DirErrBase (optional) Character value. The output path for errored datums that will replace the 
#' #/pfs/BASE_REPO portion of DirIn. Default is a directory named "errored_datums" appended to the end of DirOutBase.
#' Paths to calibration files that failed for any reason will be placed in this directory.
#' @param TimeBgn POSIX. The minimum date for which to assign calibration files.
#' @param TimeEnd POSIX. The maximum date for which to assign calibration files (non-inclusive).
#' @param PadDay (optional). 2-element difftime object with units of days indicating the days to include applicable 
#' calibration files before/after a given data day. A negative value will copy in the calibration file(s) 
#' that are applicable to the given data day AND # number of days before the data day. A positive value 
#' will copy in the calibration file(s) applicable to the given data day AND # number of days after the data day. 
#' Default is 0. For example, if the current data day is 2019-01-15, "PadDay=-2" will copy in any calibration file(s)
#' that are applicable between 2019-01-13 00:00 and 2019-01-15 24:00. "PadDay=2" will copy in calibration file(s) 
#' that are applicable between 2019-01-15 00:00 and 2019-01-17 24:00. To provide both negative and positive pads 
#' (a window around a given day), separate the values with pipes (e.g. "PadDay=-2|2"). 
#' @param Arry (optional). Logical value indicating whether the calibration files should be assigned separately for 
#' each stream ID. (Normally there would be a single stream ID corresponding to a given TERM, so no checking for 
#' multiple stream IDs is needed). A value of TRUE would be needed if the TERM is an array, meaning that calibrations 
#' for multiple stream IDs are stored within the same TERM folder and should really be treated as separate terms 
#' (one for each stream ID). This is relatively rare, but is the case for e.g. the tchain source type. The default is 
#' FALSE, but there is also really no harm in setting it to TRUE unless the stream ID for a SOURCE_ID changes over 
#' its lifetime.
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
#   Cove Sturtevant (2021-04-15)
#     add support for array variables/calibrations that utilize multiple stream IDs for the same term
#   Cove Sturtevant (2023-03-23)
#     fix bug when cal is copied in a block of directories where one fails causing the whole block to fail
#    Cove Sturtevant (2025-05-01)
#     route individual files that fail checks to an error directory and allow successful files to be assigned
##############################################################################################
wrap.cal.asgn <- function(DirIn,
                          DirOutBase,
                          DirErrBase=fs::path(DirOutBase,'errored_datums'),
                          TimeBgn,
                          TimeEnd,
                          PadDay=base::as.difftime(c(0,0),units='days'),
                          Arry=FALSE,
                          log=NULL
                          ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Directory listing of cal files for this data stream
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
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
  metaCal <- NEONprocIS.cal::def.cal.meta(fileCal = base::paste0(DirIn, '/', fileCal), log = log)
  
  # Redirect calibration files that failed for any reason to the error directory, and then remove them from metaCal
  setFail <- metaCal$err == TRUE
  if(base::all(setFail==TRUE)){
    # None left, fail the entire datum
    stop('All calibrations for ',DirIn,' failed to be processed.')
  } else if (base::any(setFail==TRUE)){
    base::lapply(fileCal[setFail], 
           FUN=function(idxFileFail){
             dir.create(fs::path(DirErrBase, '/', InfoDirIn$dirRepo),recursive=TRUE)
             base::system(paste0('touch ',DirErrBase, '/', InfoDirIn$dirRepo, '/', idxFileFail))
             }
           )
    metaCal <- metaCal[!setFail,]
  }
  
  # In the case of arrays, there can be multiple stream IDs for the same term name.
  # We want to treat each stream ID as if it were a separate term 
  if(Arry==TRUE){
    metaCal <- base::split(metaCal,metaCal$strm)
  } else {
    metaCal <- base::list(metaCal)
  }
  
  # Determine the calibrations that apply for each time period (and each streamID if an array)
  calSlct <- base::lapply(X=metaCal,
                          FUN=NEONprocIS.cal::def.cal.slct,
                          TimeBgn = TimeBgn,
                          TimeEnd = TimeEnd,
                          log = log)
  calSlct <- base::do.call(base::rbind,calSlct) # Recombine into single data frame
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
      
      for(dirOutIdx in dirOut[setDir]){
        rpt <- base::system(
          base::paste0(
            'ln -s ',
            calSlct$path[idxPrd],
            calSlct$file[idxPrd],
            ' ',
            dirOutIdx
          )
        )
        
        if (rpt == 127){
          log$error(
            base::paste0(
              calSlct$file[idxPrd],
              ' could NOT be copied to ',
              dirOutIdx,
              '. Look for warnings from system command.'
            )
          )
          stop()
        }
      } # End copy of calibration file to relevant date folders
      
      log$info(
        base::paste0(
          calSlct$file[idxPrd],
          ' copied to all daily folders between ',
          dirOut[setDir][1],
          ' and ',
          utils::tail(dirOut[setDir],1)
        )
      )
    } # End if-statement for any relevant output dates
    
  } # End loop around calibration periods
  
  return()

}
