##############################################################################################
#' @title Fill date gaps in date- and location-structured repository. Module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Module for NEON IS data processing to fill in date gaps in a repository that is 
#' structured by date and location. The highest level of the input repository structure is year, with 
#' progressively nested folders for month and day. Another level deeper (in the daily folders) are folders 
#' for different location identifiers, each with their own contents. Gaps constitute location-id folders that 
#' are missing in particular daily folders but exist in earlier and later daily folders. Location-id folders 
#' (and if necessary their higher-level date folders) will be created to fill these gaps, retaining the child 
#' folder structure that exists within the location-ID folder. This child structure is as follows:
#' /YYYY/MM/DD/LOCATION-ID
#'      ./location
#'      ./data
#'      ./flags
#'      ./uncertainty_coef
#' At least the 'data' folder must be present.
#' For the filled date gaps,  any folders specified in argument DirSubCopy are copied 
#' from the correponding folder of the same location-ID for the date prior to the gap. For data directories 
#' specified in argument DirSubData, a single file will be populated with the schema of the data in the same 
#' folder for the date prior to the gap and will contain an empty data frame (column headers but no rows). 
#' All other directories found in the date prior to the gap will be created in the output but  will remain 
#' empty.
#'
#' Note: This script does not take into account the active periods for a particular location-ID, as it would
#' require reading the location files for each filled date, which would require downloading files in some 
#' production environments (e.g. Pachyderm, which would require downloading and uploading the entire repository 
#' in one container). The reading of active periods and removal of filled directories during non-active periods 
#' is done in a separate step where each daily directory can be passed to the environment individually. 
#' 
#' This script is run at the command line with the following arguments. Each argument must be a string in the 
#' format "Para=value", where "Para" is the intended parameter name and "value" is the value of the 
#' parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter 
#' will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the path to the parent of the yearly folders, structured as follows: 
#' #/pfs/BASE_REPO/#, where # indicates any number of parent and child directories of any name, so long as they 
#' are not 'pfs',data','flags','uncertainty_coef', or 'location'. 
#'    
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' 3. "DirSubCopy=value" (optional), where the value is the names of folders which should be copied directly from the date
#' prior to the gap.
#' 
#' 4. "DirSubData=value" (optional), where the value is the names of folders which should be populated with empty data files
#' using the same schema as the files in the same folders found previous to the gap.
#' 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return A gap-filled repository. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.loc.fill.gap.repo.R 'DirIn=/pfs/proc_group/prt/' 'DirOut=/pfs/out' "DirSubCopy=location|uncertainty_coef" "DirSubData=data|flags|uncertainty_data"
#' 
#' Using environment variable for input directory
#' Sys.setenv(DIR_IN='/pfs/proc_group/prt/')
#' Rscript flow.loc.fill.gap.repo.R 'DirIn=$DIR_IN' 'DirOut=/pfs/out' "DirSubCopy=location|uncertainty_coef" "DirSubData=data|flags|uncertainty_data

#' @seealso Currently nothing.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-09-12)
#     original creation 
#   Cove Sturtevant (2019-09-30)
#     re-structured inputs to be more human readable
#     added argument for output directory 
#   Cove Sturtevant (2019-10-30)
#     added uncertainty_fdas folder
#   Cove Sturtevant (2020-02-17)
#     adjusted naming of uncertainty folder(s)
#   Cove Sturtevant (2020-03-04)
#     refactored to choose which are data folders, which are copied over, and others populated empty
#     also identify datums only by 'data' folder
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,
                                      NameParaReqd=c("DirIn",
                                                      "DirOut"),
                                      NameParaOptn=c("DirSubCopy",
                                                     "DirSubData"),
                                      log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Pull info about the parent directory
InfoDirBgn <- NEONprocIS.base::def.dir.splt.pach.time(DirBgn)
log$info(base::paste0('Evaluating parent directory: ',base::paste(DirBgn),' for locations with date gaps...')) 

# ---- Survey the dates present for each location ID ----

# Get a full directory listing, finding those matching the format /YYYY/MM/DD/LOCATION-ID and parsing out each date and location-id
dirAll <- base::list.dirs(path=DirBgn,recursive=TRUE,full.names=FALSE)
dirAllSub <- base::gregexpr(pattern='[/]',text=dirAll) # Find the subdirectories (indicated by / in the path)
mtchDir <- base::unlist(base::lapply(dirAllSub,FUN=function(idxDir){base::length(idxDir) == 3})) # Find paths with embedded date and named location at final directory
dirEval <- dirAll[mtchDir] # Directories we are going to evaluate further, in the format of /YYYY/MM/DD/LOCATION-ID
timeDirEval <- base::strptime(x=base::substr(x=dirEval,start=1,stop=10),format='%Y/%m/%d',tz='GMT') # Data date for each directory path
idLocDirEval <- base::substr(x=dirEval,start=12,stop=nchar(dirEval)) # LOCATION-ID for each directory path

# Error check our list
if(base::sum(base::is.na(timeDirEval)) > 0){
  log$fatal(base::paste0('Cannot interpret data dates for directory path(s): ',base::paste(base::paste0(DirBgn,'/',dirEval[base::is.na(timeDirEval)]),collapse=','))) 
  stop()
}

# For each location id, find the missing dates 
idLoc <- base::unique(idLocDirEval)
for(idxIdLoc in idLoc){
  
  log$info(base::paste0('Evaluating date gaps for location-ID: ',idxIdLoc)) 
  
  # What are the existing dates for this location ID
  timeIdxLoc <- timeDirEval[idLocDirEval == idxIdLoc]
  timeIdxLocNumc <- base::as.numeric(timeIdxLoc) # numeric representation

  # Symbolically link the existing directories for this location-ID
  rptCopyExst <- base::lapply(timeIdxLoc,FUN=function(idxTimeExst){
    
    dirTime <- base::paste0(base::format(idxTimeExst,'%Y'),'/',base::format(idxTimeExst,'%m'),'/',base::format(idxTimeExst,'%d'))
    dirSrc <- base::paste0(DirBgn,'/',dirTime,'/',idxIdLoc)
    dirDest <- base::paste0(DirOut,InfoDirBgn$dirRepo,'/',dirTime)
    NEONprocIS.base::def.dir.copy.symb(DirSrc=dirSrc,DirDest=dirDest,log=log)
    
  })
  log$info(base::paste0('Copied existing dates for location-ID: ',idxIdLoc)) 
  log$debug(base::paste0('Copied existing dates: ',base::format(timeIdxLoc,format='%Y-%m-%d'),' for location-ID: ',idxIdLoc)) 
  
  
  # What should be the dates if there were no gaps?
  timeIdxLocExpc <- base::seq.POSIXt(from=base::min(timeIdxLoc),to=base::max(timeIdxLoc),by='day')
  timeIdxLocExpcNumc <- base::as.numeric(timeIdxLocExpc) # numeric representation

  # Find missing dates
  timeMissNumc <- base::setdiff(timeIdxLocExpcNumc,timeIdxLocNumc)
  timeMiss <- base::subset(x=timeIdxLocExpc,subset=timeIdxLocExpcNumc %in% timeMissNumc)

  # Move on if there are no gaps (nice!)
  if(base::length(timeMiss) == 0){
    log$info(base::paste0('Nice! No date gaps for location-ID: ',idxIdLoc)) 
    next()
  } else {
    log$info(base::paste0('Filling ',base::length(timeMiss), ' date gaps for location-ID: ',idxIdLoc)) 
    log$debug(base::paste0('Filling date gaps: ',base::paste(base::format(x=timeMiss,format='%Y-%m-%d'),collapse=','), ' for location-ID: ',idxIdLoc)) 
  }
  
  # Fill each gap with the directory structure from the last available date
  timeMiss <- base::sort(timeMiss)
  timeMissDiff <- base::diff(timeMiss)
  base::units(timeMissDiff) <- 'days'
  timeMissDiff <- base::as.numeric(timeMissDiff)
  timeMissDiff <- c(1,timeMissDiff) # Add for the first missing day
  timeGf <- timeMiss[1]-as.difftime(tim=1,units='days') # Initialize the first date to fill with
  for(idxMiss in 1:base::length(timeMiss)){
    
    idxTimeMiss <- timeMiss[idxMiss]
    
    # Determine the date to fill with. If this missing date is more than 1 day after the last missing date, then we need to reset the fill date. Otherwise, 
    # the gap is continuous and we can use the previous fill date
    if(timeMissDiff[idxMiss] != 1){
      timeGf <- idxTimeMiss-as.difftime(tim=1,units='days') # Initialize the first date to fill with
    }
    
    # Fill the gap with folders to simply copy over from the date prior to the gap
    dirSrcBase <- base::paste0(DirBgn,'/',base::format(timeGf,'%Y'),'/',base::format(timeGf,'%m'),'/',base::format(timeGf,'%d'),'/',idxIdLoc)
    dirSub <- base::dir(dirSrcBase)
    dirSubCopy <- base::intersect(dirSub,Para$DirSubCopy)
    dirDest <- base::paste0(DirOut,InfoDirBgn$dirRepo,'/',base::format(idxTimeMiss,'%Y'),'/',base::format(idxTimeMiss,'%m'),'/',base::format(idxTimeMiss,'%d'),'/',idxIdLoc)
    if(length(dirSubCopy) > 0){
      dirSrc <- base::paste0(dirSrcBase,'/',dirSubCopy)
      NEONprocIS.base::def.dir.copy.symb(DirSrc=dirSrc,DirDest=dirDest,log=log)
    }
    
    # Now create all other folders
    dirAdd <- base::setdiff(dirSub,dirSubCopy)
    NEONprocIS.base::def.dir.crea(DirBgn=dirDest,DirSub=dirAdd)
    
    # Populate the data folders with an empty data file by opening the file from the last gap and using its schema
    dirSubData <- base::intersect(dirSub,Para$DirSubData)
    for(idxDirSubData in dirSubData){
      # Source and destination directories
      dirSrcData <- base::paste0(dirSrcBase,'/',idxDirSubData)
      dirDestData <- base::paste0(dirDest,'/',idxDirSubData)
      
      # Construct file names
      fileDataSrc <- base::dir(base::paste0(dirSrcBase,'/',idxDirSubData)) # filename of the data file in the source directory
      fileDataOut <- base::sub(pattern=base::format(timeGf,'%Y-%m-%d'),replacement=base::format(idxTimeMiss,'%Y-%m-%d'),x=fileDataSrc) # replace the date in the filename with the missing day
      
      # Open the file before the gap
      data <- NEONprocIS.base::def.read.avro.deve(NameFile=base::paste0(dirSrcData,'/',fileDataSrc),NameLib='/ravro.so')
      schm <- base::attr(data,'schema')
      rptDataOut <- NEONprocIS.base::def.wrte.avro.deve(data=base::data.frame(),NameFile=base::paste0(dirDestData,'/',fileDataOut),Schm=schm,NameLib='/ravro.so')
      
    }
    

    log$debug(base::paste0('Filled date gap: ',base::paste(base::format(x=idxTimeMiss,format='%Y-%m-%d'),collapse=','), ' for location-ID: ',idxIdLoc)) 
    
  }
}

