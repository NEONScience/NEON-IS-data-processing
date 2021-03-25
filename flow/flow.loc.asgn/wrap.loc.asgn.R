##############################################################################################
#' @title Assign and filter the location file(s) for a sensor ID to each data day for which each applies

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Assign the location file(s) for an asset or named location to each data day which it
#' applies over 1 or more data years. When assigning the location file to each data day, the location 
#' information is filtered to exclude information not relevant to the data day. This includes truncating
#' any applicable dates in the locations file to the start or end of the data day. 
#' Original dates falling within the data day will not be modified. This code works for 
#' both asset location files as well as named-location location files.
#'     
#' @param DirIn Character value. The input path to the location files from a single asset or location ID,
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
#' @param TimeBgn POSIX. The minimum date for which to assign calibration files.
#' @param TimeEnd POSIX. The maximum date for which to assign calibration files.
#' @param TypeFile String value. The type of location file that is being distributed and filtered. 
#' Options are 'asset' and 'namedLocation'. Only one may be specified. 'asset' corresponds to a 
#' location file for a particular asset, which includes information about where and for how long
#' the asset was installed, including its geolocation history. 'namedLocation' corresponds to a 
#' location file specific to a named location, including the properties of that named location and
#' the dates over which it was active (should have been producing data).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A directory structure in the format DirOutBase/SOURCE_TYPE/YEAR/MONTH/DAY/ID, 
#' where DirOutBase replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) and the 
#' terminal path (ID) is populated with the filtered location files applicable to the year, month, and 
#' day indicated in the path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.loc.asgn(DirIn="/pfs/tempSoil_context_group/prt/25658",
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
##############################################################################################
wrap.loc.asgn <- function(DirIn,
                          DirOutBase,
                          TimeBgn,
                          TimeEnd,
                          TypeFile=c('asset','namedLocation')[0],
                          log=NULL
                          ){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Error check
  if(base::length(TypeFile) != 1 || !(TypeFile %in% c('asset','namedLocation'))){
    log$fatal("Input TypeFile must be either 'asset' or 'namedLocation'. See documentation.")
    stop()
  }
  
  # Directory listing of location files for this data stream
  fileLoc <- base::dir(DirIn)
  numFileLoc <- base::length(fileLoc)
  
  # Move on if no location files
  if(numFileLoc == 0){
    log$info(
      base::paste0(
        'No location files in datum path ',
        DirIn,
        '. Skipping...'
      )
    )
    return()
  } else if (numFileLoc > 1){
    log$info(
      base::paste0(
        numFileLoc,
        ' location files found in datum path ',
        DirIn
      )
    )
    
  }
  

  # Initialize output
  dmmyChar <- base::character(0)
  dmmyPosx <- base::as.POSIXct(dmmyChar,tz='GMT')
  timeAsgnInit <- base::data.frame(timeBgn=dmmyPosx,
                                   timeEnd=dmmyPosx,
                                   stringsAsFactors = FALSE
  )
  timeDiffDay <- base::as.difftime(1,units='days')
  
  # Assign and filter each location file
  for(idxFileLoc in fileLoc){
    
    # Get metadata for all the location files in the directory, including install and remove dates and/or active periods for each location
    nameFile <- base::paste0(DirIn, '/', idxFileLoc)
    locMeta <- NEONprocIS.base::def.loc.meta(NameFile = nameFile,
                                             log = log)
    
    # ------- Compile the dates over which this location file applies. -------
    # Install/remove dates for asset install
    if(TypeFile == 'asset'){
      timeAsgn <- base::data.frame(timeBgn=base::as.POSIXct(locMeta$install_date,tz='GMT'),
                                   timeEnd=base::as.POSIXct(locMeta$remove_date,tz='GMT'),
                                   stringsAsFactors = FALSE) # Asset location file
    } else if (TypeFile == 'namedLocation'){
      # Active dates for named location
      timeAsgn <- lapply(locMeta$active_periods,
                         FUN=function(idxLoc){
                           if(base::length(idxLoc) == 1 && base::is.na(idxLoc)){
                             return(NULL)
                           } else {
                             # Rename the columns to match
                             idxLoc <- NEONprocIS.base::def.df.renm(
                                df=idxLoc,
                                mappNameVar=
                                  base::data.frame(nameVarIn=c('start_date','end_date'),
                                                   nameVarOut=c('timeBgn','timeEnd'),
                                                   stringsAsFactors=FALSE
                                                   ),
                                log=log)
                             return(idxLoc)
                           }
                         }
      )
      timeAsgn <- base::do.call(base::rbind,timeAsgn)      
    }

    # Ensure time zone in GMT
    base::attr(timeAsgn$timeBgn,'tzone') <- 'GMT'
    base::attr(timeAsgn$timeEnd,'tzone') <- 'GMT'
      
    # Get rid of rows where start time is NA or time period is fully outside our range of interest
    timeAsgn <- base::subset(timeAsgn,subset=!base::is.na(timeAsgn$timeBgn) &
                             timeAsgn$timeBgn < TimeEnd & 
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
          base::seq.POSIXt(from=base::trunc(timeAsgn$timeBgn[idxRow],units='days'),
                           to=base::trunc(timeAsgn$timeEnd[idxRow],units='days'),
                           by='day')
        
      }
    )
    ts <- base::unique(base::do.call(c,ts)) # unlist
    ts <- ts[ts != TimeEnd] # Get rid of final date
    tsChar <- base::unique(format(ts,format='%Y/%m/%d')) # Format as year/month/day repo structure and get rid of duplicates
    
    # Create the output directory structure
    InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
    typeSrc <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+1]
    id <- InfoDirIn$dirSplt[InfoDirIn$idxRepo+2]
    dirOut <- base::paste0(DirOutBase,'/',typeSrc,'/',tsChar,'/',id,'/location')
    NEONprocIS.base::def.dir.crea(DirSub=dirOut,log=log)
    
    # Populate the directory structure with a location file filtered for the data day
    numDay <- base::length(ts)
    for(idxDay in base::seq_len(numDay)){
      
      # Filter the location file for the data day and save it to the output
      nameFileOut <- base::paste0(dirOut[idxDay],'/',idxFileLoc)
      
      if(TypeFile=='asset'){
        NEONprocIS.base::def.loc.filt(NameFileIn=nameFile,
                                      NameFileOut=nameFileOut,
                                      TimeBgn=ts[idxDay],
                                      TimeEnd=ts[idxDay]+timeDiffDay,
                                      log=log
        )
      } else if (TypeFile == 'namedLocation'){
        NEONprocIS.base::def.loc.trnc.actv(NameFileIn=nameFile,
                                           NameFileOut=nameFileOut,
                                           TimeBgn=ts[idxDay],
                                           TimeEnd=ts[idxDay]+timeDiffDay
        )
      }
    } # End loop around data days for this location file
    
    
  } # End loop around location files

  return()

} # End function
