##############################################################################################
#' @title Assign and filter the science review flag file(s) for a GROUP ID to each data day for which each applies

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Assign the science review flag file(s) for a group to each data day 
#' that it applies. When assigning the file to each data day, the information is filtered to exclude 
#' information not relevant to the data day. This includes truncating
#' any applicable dates in the file to the start and end of the data day, and removes things
#' like create/update dates and user comment that may change without changing the flagging behavior
#' Original start/end dates falling within the data day will not be modified. 
#'     
#' @param DirIn Character value. The input path to the location or group files,
#' structured as follows: \cr
#' #/pfs/BASE_REPO/GROUP_ID \cr
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' There may be no further subdirectories of GROUP_ID.\cr
#'
#' For example: \cr
#' Input path = /scratch/pfs/proc_group/surfacewater-physical_PRLA130100 \cr
#'     
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param TimeBgn POSIX. The minimum date for which to assign location files.
#' @param TimeEnd POSIX. The maximum date for which to assign location files.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A directory structure in the format DirOutBase/YEAR/MONTH/DAY/GROUP_ID, 
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the 
#' terminal path (GROUP_ID) is populated with the filtered SRF file(s) applicable to the year, month, and 
#' day indicated in the path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.srf.asgn(DirIn="/pfs/proc_group/surfacewater-physical_ARIK101100",
#'               DirOutBase="/pfs/out",
#'               TimeBgn=as.POSIXct('2020-01-01',tz='GMT),
#'               TimeEnd=as.POSIXct('2020-13-31',tz='GMT),
#'               )

#' @seealso None
#' 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-01-27)
#     original creation
##############################################################################################
wrap.srf.asgn <- function(DirIn,
                          DirOutBase,
                          TimeBgn,
                          TimeEnd,
                          log=NULL
                          ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Directory listing of files for this datum
  file <- base::dir(DirIn)
  numFile <- base::length(file)
  
  # Move on if no files
  if(numFile == 0){
    log$info(
      base::paste0(
        'No files in datum path ',
        DirIn,
        '. Skipping...'
      )
    )
    return()
  } else if (numFile > 1){
    log$info(
      base::paste0(
        numFile,
        ' files found in datum path ',
        DirIn
      )
    )
    
  }
  
  # Decide the name of the terminal output directory
  dirFinl <- 'science_review_flags'

  # Initialize output
  timeDiffDay <- base::as.difftime(1,units='days')
  
  # Assign and filter each file
  for(idxFile in file){
    
    # Read in the srf file
    nameFile <- fs::path(DirIn,idxFile)
    srf <- NEONprocIS.pub::def.read.srf(NameFile=nameFile)
    
    # Filter for srfs in our time range of interest
    srf <- NEONprocIS.pub::def.srf.filt(srf=srf,TimeBgn=TimeBgn,TimeEnd=TimeEnd,log=log)
    
    # Compile the dates over which this file applies.
    timeAsgn <- base::data.frame(timeBgn=srf$start_date,
                                 timeEnd=srf$end_date,
                                 stringsAsFactors = FALSE) # Asset location file

    # Move on if no periods to process
    if(base::nrow(timeAsgn) == 0){
      log$info(
        base::paste0(
          'No time ranges found in file ',
          nameFile,
          ' apply to time interval: ',
          TimeBgn,
          ' to ',
          TimeEnd,
          '. Skipping...'
        )
      )
      next
    }
    
    # Get the date sequence 
    ts <- base::lapply(
      base::seq_len(base::nrow(timeAsgn)),
      FUN=function(idxRow){
          timeBgnIdxRow <- base::trunc(timeAsgn$timeBgn[idxRow],units='days')
          timeEndIdxRow <- base::trunc(timeAsgn$timeEnd[idxRow],units='days')
          # If the assignment period ends at 00:00 on a day, remove that day
          if (timeEndIdxRow == timeAsgn$timeEnd[idxRow]){
            timeEndIdxRow <- timeEndIdxRow-base::as.difftime(1,units='days')
          }
          base::seq.POSIXt(from=timeBgnIdxRow,
                           to=timeEndIdxRow,
                           by='day')
        
      }
    )
    ts <- base::unique(base::do.call(c,ts)) # unlist
    base::attr(ts,'tzone') <- base::attr(TimeBgn,'tzone') # Re-assign time zone, which was stripped in unlisting
    tsChar <- base::unique(format(ts,format='%Y/%m/%d')) # Format as year/month/day repo structure and get rid of duplicates
    
    # Create the output directory structure
    InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
    idGrp <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+1]
    dirOut <- fs::path(DirOutBase,tsChar,idGrp,dirFinl)
    NEONprocIS.base::def.dir.crea(DirSub=dirOut,log=log)
    
    # Populate the directory structure with a srf file filtered for the data day
    numDay <- base::length(ts)
    for(idxDay in base::seq_len(numDay)){
      
      # Filter the file for the data day and save it to the output
      nameFileOut <- fs::path(dirOut[idxDay],idxFile)
      
      srfFilt <- NEONprocIS.pub::def.srf.filt(srf=srf,
                                                NameFileOut=nameFileOut,
                                                TimeBgn=ts[idxDay],
                                                TimeEnd=ts[idxDay]+timeDiffDay,
                                                log=log
      )

      
    } # End loop around data days for this file
    
    log$info(base::paste0('Filtered and assigned ',nameFile,' to all applicable days'))
    
  } # End loop around files
  
  return()

} # End function
