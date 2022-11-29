##############################################################################################
#' @title Assign and filter the location or group file(s) for a sensor ID to each data day for which each applies

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Assign the location or group file(s) for an asset, named location, or group to each data day 
#' that it applies. When assigning the file to each data day, the information is filtered to exclude 
#' information not relevant to the data day. This includes truncating
#' any applicable dates in the file to the start and end of the data day. 
#' Original dates falling within the data day will not be modified. This code works for 
#' asset location files, named-location location files, and group files.
#'     
#' @param DirIn Character value. The input path to the location or group files,
#' structured as follows: \cr
#' #/pfs/BASE_REPO/SOURCE_TYPE/ID \cr
#' where # indicates any number of parent and child directories of any name, so long as they are not pfs.
#' 
#' There may be no further subdirectories of ID.\cr
#'
#' For example: \cr
#' Input path = /scratch/pfs/proc_group/prt/27134 \cr
#'     
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param TimeBgn POSIX. The minimum date for which to assign location files.
#' @param TimeEnd POSIX. The maximum date for which to assign location files.
#' @param TypeFile String value. The type of file that is being distributed and filtered. 
#' Options are 'asset', 'namedLocation', and 'group'. Only one may be specified. 'asset' corresponds to a 
#' location file for a particular asset, which includes information about where and for how long
#' the asset was installed, including its geolocation history. 'namedLocation' corresponds to a 
#' location file specific to a named location, including the properties of that named location and
#' the dates over which it was active (should have been producing data). 'group' corresponds to a group
#' file specific to a group member, including what groups the member is in and properties of the group.
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A directory structure in the format DirOutBase/SOURCE_TYPE/YEAR/MONTH/DAY/ID/<'location' or 'group'>, 
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the 
#' terminal path (ID) is populated with the filtered location or group files applicable to the year, month, and 
#' day indicated in the path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.loc.grp.asgn(DirIn="/pfs/tempSoil_group/prt/25658",
#'              DirOutBase="/pfs/out",
#'              TimeBgn=as.POSIXct('2019-01-01',tz='GMT),
#'              TimeEnd=as.POSIXct('2019-06-01',tz='GMT),
#'              TypeFile='asset'
#'              )

#' @seealso None
#' 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-03-15)
#     original creation
#   Cove Sturtevant (2022-09-21)
#     incorporate IS default processing start date
#   Cove Sturtevant (2022-11-22)
#     Add option for group files
##############################################################################################
wrap.loc.grp.asgn <- function(DirIn,
                          DirOutBase,
                          TimeBgn,
                          TimeEnd,
                          TypeFile=c('asset','namedLocation','group')[0],
                          log=NULL
                          ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Error check
  if(base::length(TypeFile) != 1 || !(TypeFile %in% c('asset','namedLocation','group'))){
    log$fatal("TypeFile must be either 'asset', 'namedLocation', or 'group'. See documentation.")
    stop()
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
  
  # Decide the name of the terminal output directory based on TypeFile
  dirFinl <- base::switch(TypeFile,
                          asset = 'location',
                          namedLocation = 'location',
                          group ='group'
  )

  # Initialize output
  dmmyChar <- base::character(0)
  dmmyPosx <- base::as.POSIXct(dmmyChar,tz='GMT')
  timeAsgnInit <- base::data.frame(timeBgn=dmmyPosx,
                                   timeEnd=dmmyPosx,
                                   stringsAsFactors = FALSE
  )
  timeDiffDay <- base::as.difftime(1,units='days')
  
  # Assign and filter each file
  for(idxFile in file){
    
    # Get metadata for all thefiles in the directory, including install and remove dates and/or active periods
    nameFile <- base::paste0(DirIn, '/', idxFile)
    # ------- Compile the dates over which this location file applies. -------
    # Install/remove dates for asset install
    if(TypeFile == 'asset'){
      meta <- NEONprocIS.base::def.loc.meta(NameFile = nameFile, log = log)
      
      timeAsgn <- base::data.frame(timeBgn=base::as.POSIXct(meta$install_date,tz='GMT'),
                                   timeEnd=base::as.POSIXct(meta$remove_date,tz='GMT'),
                                   stringsAsFactors = FALSE) # Asset location file
    } else if (TypeFile %in% c('namedLocation','group')){
      
      meta <- base::switch(TypeFile,
                           namedLocation = NEONprocIS.base::def.loc.meta(NameFile = nameFile, log = log),
                           group = NEONprocIS.base::def.grp.meta(NameFile = nameFile, log = log)
      )

      # Active periods for named location or group
      timeAsgn <- lapply(meta$active_periods,
                         FUN=function(idx){
                           if(base::length(idx) == 1 && base::is.na(idx)){
                             return(NULL)
                           } else {
                             # Rename the columns to match
                             idx <- NEONprocIS.base::def.df.renm(
                                df=idx,
                                mappNameVar=
                                  base::data.frame(nameVarIn=c('start_date','end_date'),
                                                   nameVarOut=c('timeBgn','timeEnd'),
                                                   stringsAsFactors=FALSE
                                                   ),
                                log=log)
                             return(idx)
                           }
                         }
      )
      timeAsgn <- base::do.call(base::rbind,timeAsgn)  
      
    } 
    
    # Ensure time zone in GMT
    base::attr(timeAsgn$timeBgn,'tzone') <- 'GMT'
    base::attr(timeAsgn$timeEnd,'tzone') <- 'GMT'
    
    # Note that a location or group without any active periods is considered never active. This will result in 
    # a timeAsgn table with zero rows. A location/group with both start and end dates as NULL is considered 
    # always active (as of the default processing start date for the site in the case of a named location, or since TimeBgn for a group)
    # Replace any NA start dates with the IS default processing start date, and if that fails then use TimeBgn
    timeAsgn$timeBgn[base::is.na(timeAsgn$timeBgn)] <- meta$IS_Processing_Default_Start_Date
    timeAsgn$timeBgn[base::is.null(timeAsgn$timeBgn) | base::is.na(timeAsgn$timeBgn)] <- TimeBgn
    
    # Get rid of rows where the time period is fully outside our range of interest
    timeAsgn <- base::subset(timeAsgn,subset= timeAsgn$timeBgn < TimeEnd & 
                             (base::is.na(timeAsgn$timeEnd) | timeAsgn$timeEnd >= TimeBgn)
                             )
                                           
    # Truncate start & end times to time range of interest
    timeAsgn$timeBgn[timeAsgn$timeBgn < TimeBgn] <- TimeBgn
    timeAsgn$timeEnd[base::is.na(timeAsgn$timeEnd) | timeAsgn$timeEnd > TimeEnd] <- TimeEnd
    
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
          # If the active period ends at 00:00 on a day, remove that day
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
    typeSrc <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+1]
    id <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+2]
    dirOut <- base::paste0(DirOutBase,'/',typeSrc,'/',tsChar,'/',id,'/',dirFinl)
    NEONprocIS.base::def.dir.crea(DirSub=dirOut,log=log)
    
    # Populate the directory structure with a location file filtered for the data day
    numDay <- base::length(ts)
    for(idxDay in base::seq_len(numDay)){
      
      # Filter the file for the data day and save it to the output
      nameFileOut <- base::paste0(dirOut[idxDay],'/',idxFile)
      
      if (TypeFile=='asset'){
        listFile <- NEONprocIS.base::def.loc.filt(NameFileIn=nameFile,
                                                  NameFileOut=nameFileOut,
                                                  TimeBgn=ts[idxDay],
                                                  TimeEnd=ts[idxDay]+timeDiffDay,
                                                  log=log
        )
      } else if (TypeFile == 'namedLocation'){
        listFile <- NEONprocIS.base::def.loc.trnc.actv(NameFileIn=nameFile,
                                                       NameFileOut=nameFileOut,
                                                       TimeBgn=ts[idxDay],
                                                       TimeEnd=ts[idxDay]+timeDiffDay,
                                                       log=log
        )
      } else if (TypeFile == 'group'){
          listFile <- NEONprocIS.base::def.grp.trnc.actv(NameFileIn=nameFile,
                                                         NameFileOut=nameFileOut,
                                                         TimeBgn=ts[idxDay],
                                                         TimeEnd=ts[idxDay]+timeDiffDay,
                                                         log=log
       
          )
      }
      
      
    } # End loop around data days for this file
    
    log$info(base::paste0('Filtered and assigned ',nameFile,' to all applicable days'))
    
  } # End loop around files
  
  

  return()

} # End function
