##############################################################################################
#' @title Truncate and combine/merge sensor-based by location  

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Wrapper function. Truncate and combine/merge sensor-based by location. 
#' Before running this function, data should already be grouped by location, meaning that data 
#' and metadata from multiple sensors installed at a location during the relevant
#' time period (e.g. before/after a sensor swap) are located in the same directory for each data 
#' type. This grouping is done in flow.loc.repo.strc. This function will read the location files,
#' truncate and/or merge the contents of multiple data and metadata files based on 
#' the time period that each sensor was installed at the location.
#' 
#' @param DirIn Character value. The input path to the data from a single location ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, 
#' where # indicates any number of parent and child directories of any name, so long as they are not 
#' 'pfs', 'location',or recognizable as the 'yyyy/mm/dd' structure which indicates the 4-digit year, 
#' 2-digit month, and 2-digit day of the data contained in the folder. The data day is identified from 
#' the input path. The location-id is the unique identifier of the location. \cr
#' 
#' Nested within the input path path is (at a minimum) the folder:
#'         location/
#' The location folder holds json files with the location data corresponding to the data files in the data
#' directory. The source-ids for which data are expected are gathered from the location files. 
#' 
#' Other folders at the same level as location can hold data or metadata (mergeable data types are listed below).
#'    
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' @param DirSubCombData (optional) Character vector. The names of subfolders holding timeseries files (e.g. data, flags)
#' to be truncated and/or merged. These directories must be at the same level as the location directory. Within 
#' each folder are timeseries files from one or more source-ids. The source-id is the identifier for the sensor 
#' that collected the data in the file, and must be included somewhere in the file name (and must match one of the 
#' source-ids gathered from the location files). The data folders listed here may contain files holding different 
#' types of data (e.g. one file holds calibration flags, another holds sensor diagnostic flags). However, the file 
#' names for each type must be identical with the exception of the source ID. An attempt will be made to merge/truncate 
#' the files that have the same naming convention. For example, prt_12345_sensorFlags.parquet will be merged with 
#' prt_678_sensorFlags.parquet, and prt_12345_calFlags.parquet will be merged with prt_678_calFlags.parquet, since 
#' the file names in each of these two groups are identical with the exception of the source ID.
#' @param DirSubCombUcrt (optional) Character vector. The names of subfolders holding uncertainty coefficient 
#' json files to be merged. These directories must be at the same level as the location directory. Within each 
#' subfolder are uncertainty json files, one for each source-id. The source-id is the identifier for the sensor 
#' pertaining to the uncertainty info in the file, and must be somewhere in the file name.
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders, separated by pipes, at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return A repository structure in DirOut, where DirOut replaces the input directory 
#' structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure 
#' of the input path. A single merged file will replace the originals in each of the subdirectories
#' specified in the input arguments. The merged files will be written with the same schema as returned from 
#' reading the input file(s). The characters representing the source-id in the merged filenames will be replaced 
#' by the location ID. All other subdirectories indicated in DirSubCopy will be carried through unmodified.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
#' wrap.loc.data.trnc.comb(DirIn="/pfs/prt_structure_repo_by_location/prt/2018/01/01/CFGLOC12345",
#'                         DirOutBase="/pfs/out",
#'                         DirSubCombData=c('data','flags'),
#'                         DirSubCombUcrt='uncertainty_coef',
#'                         DirSubCopy='location')
 
#' @seealso None
 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-05-10)
#     original creation of wrapper function, from flow.loc.data.trnc.comb
##############################################################################################
wrap.loc.data.trnc.comb <- function(DirIn,
                                    DirOutBase,
                                    DirSubCombData=NULL,
                                    DirSubCombUcrt=NULL,
                                    DirSubCopy=NULL,
                                    log=NULL
){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  # Gather info about the input directory and formulate the parent output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  nameLoc <- utils::tail(InfoDirIn$dirSplt,1) # Location identifier
  dirOutPrnt <- base::paste0(DirOut,InfoDirIn$dirRepo)
  
  # Copy with a symbolic link the desired subfolders we aren't modifying
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn,'/',DirSubCopy),dirOutPrnt,log=log)
  }  
  
  # Get a list of location files
  DirInLoc <- base::paste0(DirIn,'/location')
  fileLoc <- base::dir(DirInLoc)

  # Pull all source IDs installed at this location for this day from the location files 
  locMeta <- base::do.call(base::rbind,base::lapply(base::paste0(DirInLoc,'/',fileLoc), 
                                                    NEONprocIS.base::def.loc.meta,
                                                    TimeBgn=InfoDirIn$time,
                                                    TimeEnd=InfoDirIn$time+as.difftime(1,units='days'),
                                                    log=log))
  idSrc <- base::unique(locMeta$source_id)
  
  # For each data directory, truncate/merge the data within based on its installation period at the specified named location
  for (idxDirSubCombData in DirSubCombData){
    
    # Create the output directory
    base::suppressWarnings(base::dir.create( base::paste0(dirOutPrnt,'/',idxDirSubCombData),recursive=TRUE))

    # Get a file listing
    fileData <- base::dir(base::paste0(DirIn,'/',idxDirSubCombData))
    
    
    # Parse the names of the files to form groupings of files to be merged. Files that should be merged will have 
    # the same name except for the source ID. 
    fileDataNoSrc <- fileData
    for(idxIdSrc in idSrc){
        fileDataNoSrc <- base::sub(pattern=idxIdSrc,replacement='SOURCEID',x=fileDataNoSrc)
    }
    grpFileData <- base::unique(fileDataNoSrc)
    grpFileData <- lapply(grpFileData,FUN=function(idxGrpFileData){
      return(fileData[fileDataNoSrc==idxGrpFileData])
    })
    
    # Do a quick check: are any groups empty? Something went wrong so let's assume there are no groups
    numFileGrp <- base::unlist(base::lapply(grpFileData,base::length))
    if(base::any(numFileGrp == 0)){
      grpFileData <- list(grp=fileData)
    }
    
    # Go through each group of similar files
    for(idxGrp in base::seq_len(base::length(grpFileData))){
      
      dataOut <- NULL # Initialize output
      
      # Go through each sensor file, grabbing data installed at the location
      for(idxFileData in grpFileData[[idxGrp]]){
        
        nameFileData <- base::paste0(DirIn,'/',idxDirSubCombData,'/',idxFileData) # Full path to file
        
        # Determine its source id
        idSrcIdx <- idSrc[base::unlist(base::lapply(idSrc,base::grepl,x=idxFileData,fixed=TRUE))] # source id
        if(base::length(idSrcIdx) != 1){
          # Generate error and stop execution
          log$error(base::paste0('Cannot unambiguously determine source id and matching location info for file name: ', nameFileData)) 
          stop()
        }
        
        # Open the data file
        data  <- base::try(NEONprocIS.base::def.read.parq(NameFile=nameFileData,log=log),silent=FALSE)
        if(base::any(base::class(data) == 'try-error')){
          # Generate error and stop execution
          log$error(base::paste0('File: ', nameFileData, ' is unreadable.')) 
          stop()
        }
        
        # If we haven't saved any data. Initialize columns
        if(base::is.null(dataOut)){
          dataOut <- data[base::numeric(0),]
        }
        
        # Pull the location metadata for this sensor & location
        loc <- locMeta[locMeta$source_id==idSrcIdx & locMeta$name==nameLoc,]
        
        # Find the location id in the locations file
        numLoc <- base::nrow(loc)
        if(numLoc == 0){
          log$warn(base::paste0('No matching location information for location',nameLoc,' and source id ',
                                idSrcIdx, ' was found in the location files ', 
                                ' as part of processing data file: ',nameFileData,
                                ' . This should not happen. You should investigate...'))
          next()
        }
        
        # For each install and removal at this location, mark the data to pull over to the output
        setData <- base::numeric(0)
        for (idxLoc in base::seq_len(numLoc)){
          if(base::is.na(loc$remove_date[idxLoc])){
            setData <- c(setData,base::which(data$readout_time >= loc$install_date[idxLoc]))
          } else {
            setData <- c(setData,base::which(data$readout_time >= loc$install_date[idxLoc] & 
                                               data$readout_time < loc$remove_date[idxLoc]))
          }
        }
        setData <- base::unique(setData)
        
        # Put data applicable to this named location into the output
        dataOut <- base::rbind(dataOut,data[setData,])
        
      } # End loop around sensor files
      
      # Sort the output by date and write the output
      if(!base::is.null(dataOut)){
        dataOut <- dataOut[base::order(dataOut$readout_time),]
        
        # Replace the source id in the file name with the location id
        fileDataOut <- base::sub(pattern=idSrcIdx,replacement=nameLoc,x=idxFileData)
        nameFileDataOut <- base::paste0(dirOutPrnt,'/',idxDirSubCombData,'/',fileDataOut) # Full path to output file
        
        # Write the data
        rptDataOut <- base::try(NEONprocIS.base::def.wrte.parq(data=dataOut,NameFile=nameFileDataOut,log=log),silent=TRUE)
        if(base::any(base::class(rptDataOut) == 'try-error')){
          log$error(base::paste0('Cannot write Truncated/merged file ', nameFileDataOut,'. ',attr(rptDataOut,"condition"))) 
          stop()
        } else {
          log$info(base::paste0('Truncated/merged timeseries data (by location) written successfully in ',nameFileDataOut))
        }
      }
      
    } # End loop around file groups
    
    
  } # End loop around data directories
  
    
  # For each uncertainty directory, truncate/merge the data within based on its installation period at the specified named location
  for (idxDirSubCombUcrt in DirSubCombUcrt){
    
    # Get a file listing
    fileUcrt <- base::dir(base::paste0(DirIn,'/',idxDirSubCombUcrt))
    
    # Create the output directory
    base::suppressWarnings(base::dir.create( base::paste0(dirOutPrnt,'/',idxDirSubCombUcrt),recursive=TRUE))
    
    ucrtOut <- NULL # Initialize output
    
    # Go through each sensor file, adjusting the start/end dates to match the time period it was installed at the location
    for(idxFileUcrt in fileUcrt){
      
      nameFileUcrt <- base::paste0(DirIn,'/',idxDirSubCombUcrt,'/',idxFileUcrt) # Full path to file
      
      # Determine its source id and find its matching location file
      idSrcIdx <- idSrc[base::unlist(base::lapply(idSrc,base::grepl,x=idxFileUcrt,fixed=TRUE))] # source id
      if(base::length(idSrcIdx) != 1){
        # Generate error and stop execution
        log$error(base::paste0('Cannot unambiguously determine source id and matching location info for file name: ', nameFileUcrt))
        stop()
      }
      
      # Open the uncertainty file
      ucrt  <- base::try(rjson::fromJSON(file=nameFileUcrt,simplify=TRUE),silent=FALSE)
      if(base::class(ucrt) == 'try-error'){
        # Generate error and stop execution
        log$error(base::paste0('File: ', nameFileUcrt, ' is unreadable.')) 
        stop()
      }
      
      # If we haven't saved any data, initialize uncertainty output
      if(base::is.null(ucrtOut)){
        ucrtOut <- base::list()
      }
      
      # Pull the location metadata for this sensor & location
      loc <- locMeta[locMeta$source_id==idSrcIdx & locMeta$name==nameLoc,]
      
      # Find the location id in the locations file
      numLoc <- base::nrow(loc)
      if(numLoc == 0){
        log$warn(base::paste0('No matching location information for location',nameLoc,' and source id ',
                              idSrcIdx, ' was found in the location files ', 
                              ' as part of processing data file: ',nameFileUcrt,
                              ' . This should not happen. You should investigate...'))
        next()
      }
      
      # For each install and removal at this location, find the matching uncertainty info, adjust dates as needed, and save to the output
      for (idxLoc in base::seq_len(numLoc)){
        ucrtIdxLoc <- base::lapply(ucrt,FUN=function(idxUcrt){
          # Turn uncertainty dates to POSIX
          timeUcrtBgn <- base::strptime(x=idxUcrt$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
          timeUcrtEnd <- base::strptime(x=idxUcrt$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
          
          # Does this uncertainty application period overlap with this install period
          NaTimeLocEnd <- base::is.na(loc$remove_date[idxLoc]) # Is the remove date NA?
          if(loc$install_date[idxLoc] < timeUcrtEnd && (NaTimeLocEnd || loc$remove_date[idxLoc] > timeUcrtBgn)){
            # It does, so do we need to truncate the uncertainty application period to match the install period?
            if(timeUcrtBgn < loc$install_date[idxLoc]){
              # Bump up uncertainty start date to match install date
              idxUcrt$start_date <- base::format(loc$install_date[idxLoc],format='%Y-%m-%dT%H:%M:%OSZ')
            }
            if(!base::is.na(loc$remove_date[idxLoc]) && timeUcrtEnd > loc$remove_date[idxLoc]){
              # Truncate uncertainty end date to match remove date
              idxUcrt$end_date <- base::format(loc$remove_date[idxLoc],format='%Y-%m-%dT%H:%M:%OSZ')
            }
            return(idxUcrt)
          } else {
            return(NULL)
          }
        })
        
        # Get rid of NULL entries
        ucrtIdxLoc <- ucrtIdxLoc[base::unlist(base::lapply(ucrtIdxLoc,FUN=function(idx){!base::is.null(idx)}))]
        
        # Append remaining/updated entries to output list
        numUcrtAdd <- base::length(ucrtIdxLoc)
        if(numUcrtAdd > 0){
          numUcrtOut <- base::length(ucrtOut)
          ucrtOut[(numUcrtOut+1):(numUcrtOut+numUcrtAdd)] <- ucrtIdxLoc
        }
      } # End loop around install/removal dates for this sensor at this location

    } # End loop around sensor files
    
    # Write the output
    if(!base::is.null(ucrtOut)){

      # Replace the source id in the file name with the location id
      fileUcrtOut <- base::sub(pattern=idSrcIdx,replacement=nameLoc,x=idxFileUcrt)
      nameFileUcrtOut <- base::paste0(dirOutPrnt,'/',idxDirSubCombUcrt,'/',fileUcrtOut) # Full path to output file
      
      # Write
      rptUcrt <- base::try(base::write(rjson::toJSON(ucrtOut,indent=3),file=nameFileUcrtOut),silent=TRUE)
      if(base::class(rptUcrt) == 'try-error'){
        log$error(base::paste0('Cannot write truncated/merged uncertainty file ', nameFileUcrtOut,'. ',attr(rptUcrt,"condition"))) 
        stop()
      } else {
        log$info(base::paste0('Truncated/merged uncertainty information (by location) written successfully in ',nameFileUcrtOut))
      }
    }
    
  } # End loop around uncertainty directories
  
} # End function
