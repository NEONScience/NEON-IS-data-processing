##############################################################################################
#' @title Calibration filtering module for NEON IS data processing

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} 

#' @description Workflow. Filter the directory of calibration files for the most recent calibration(s) applicable 
#' to the data dates. Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' This script is run at the command line with 2 or 3 arguments. Each argument must be a string in the format
#' "Para=value", where "Para" is the intended parameter name and "value" is the value of the parameter. The 
#' arguments are: 
#' 
#' 1. "DirIn=value", where value is the input path, structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which 
#' indicates the 4-digit year, 2-digit month, and 2-digit day. 
#' 
#' Nested within this path are the folders:
#'         /data 
#'         /calibration/STREAM 
#' The data folder holds a single daily data file corresponding to the yyyy/mm/dd in the input path. 
#' The STREAM folder(s) may be any name and there may be any number of STREAM folders at this level,
#' each containing the calibration files applicable to STREAM.
#'  
#' For example:
#' Input path = /scratch/pfs/proc_group/soilprt/27134/2019/01/01 with nested folders:
#'    /data 
#'    /calibration/soilPRTResistance 
#'    /calibration/heaterVoltage 
#'    
#' Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the parameter will be assigned from the system environment 
#' variable matching the value string.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 3. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by pipes, at the same level as the 
#' calibration folder in the input path that are to be copied with a symbolic link to the output path.

#' @return A directory of filtered calibration files and additional desired subfolders symbolically linked in directory DirOut, 
#' where DirOut replaces the input directory structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains the 
#' child directory structure of the input path. 


#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.cal.filt.R "DirIn=/pfs/proc_group/2019/01/01/prt/27134" "DirOut=/pfs/out" "DirSubCopy=data"

#' @seealso \code{\link[NEONprocIS.base]{def.log.init}}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-03-12)
#     original creation
#   Cove Sturtevant (2019-04-30)
#     add looping through datums
#   Cove Sturtevant (2019-05-08)
#     add hierarchical logging
#   Cove Sturtevant (2019-05-21)
#     updated call to newly created NEONprocIS.cal package
#   Cove Sturtevant (2019-09-12)
#     added reading of input path from environment variable
#     simplified fatal errors to not stifle the R error message
#   Cove Sturtevant (2019-09-24)
#     structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Options
base::options(digits.secs = 3)

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut"),NameParaOptn="DirSubCopy",log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,c('calibration'))) # Make sure we don't symbolically link the calibration folder
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c('calibration',DirSubCopy))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths. We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

# Process each file path
for(idxDirIn in DirIn){

  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Get directory listing of input directory. Expect subdirectories for data and calibration(s)
  DirCal <- base::paste0(idxDirIn,'/calibration')
  var <- base::dir(DirCal,include.dirs=TRUE) # data streams with calibrations
  
  # Create the base output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    base::suppressWarnings(NEONprocIS.base::def.copy.dir.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut))
    log$info(base::paste0('Unmodified subdirectories ',base::paste0(DirSubCopy,collapse=','),' of ',idxDirIn, ' copied to ',idxDirOut))
  }
  
  # The time frame of the data is one day, and this day is indicated in the directory structure.
  if(base::is.null(InfoDirIn$time)){
    # Generate error and stop execution
    log$fatal(base::paste0('Cannot interpret data date from input directory structure: ',InfoDirIn$dirRepo)) 
    base::stop() 
  }
  timeBgn <-  InfoDirIn$time
  timeEnd <- InfoDirIn$time + base::as.difftime(1,units='days')
  
  
  # For each data stream, filter the calibration files for the most recent applicable file(s) over the data date range
  for(idxVar in var){
    
    # Create the output directory for calibrations
    DirOutCalVar <- base::paste0(idxDirOut,'/calibration/',idxVar)
    base::dir.create(DirOutCalVar,recursive=TRUE)
    
    # Directory listing of cal files for this data stream
    DirCalVar <- base::paste0(DirCal,'/',idxVar)
    fileCal <- base::dir(DirCalVar)
    
    #Read in each calibration file, saving the valid start/end dates & certificate number
    metaCal <- base::lapply(fileCal,FUN=function(idxFile){
      cal <- NEONprocIS.cal::def.read.cal.xml(NameFile=base::paste0(DirCalVar,'/',idxFile),Vrbs=TRUE)
      rpt <- base::data.frame(file=idxFile,timeValiBgn=cal$timeVali$StartTime,timeValiEnd=cal$timeVali$EndTime,
                              id=base::as.numeric(cal$file$StreamCalVal$CertificateNumber),stringsAsFactors=FALSE)
    })
    metaCal <- base::Reduce(f=base::rbind,x=metaCal)
    
    # Which calibration files have valid ranges at or before our data range? 
    use <- metaCal$timeValiBgn <= timeEnd
    if(base::sum(use) == 0){
      # We don't have any applicable files, issue warning and skip
      log$warn(base::paste0('No calibration files have valid ranges at or before data date in cal directory ',DirCalVar))
      next
    } else {
      # Save the usable ones
      metaCal <- metaCal[use,] 
    }
    
    # Of the expired cals, keep only the one with the latest valid date. This has the effect of bringing the list
    # down to one file if all cals are expired. If we have expired and unexpired cals, we'll only consider the last expired.
    setSort <- base::sort(base::as.numeric(metaCal$timeValiEnd),decreasing=TRUE,index.return=TRUE)$ix
    metaCal <- metaCal[setSort,]
    setCalExpi <- base::which(metaCal$timeValiEnd < timeBgn)
    metaCal$expi <- FALSE # Add a field for expired cals
    metaCal$expi[setCalExpi]<- TRUE
    if(base::length(setCalExpi)>1){
      metaCal <- metaCal[1:setCalExpi[1],]
    }
    
    # If we have more than 1 file with a valid cal during our data range, we need to do more checking
    numCal <- base::nrow(metaCal)
    if(numCal > 1){
      
      # For the expired cal (if any), move it's end date to the data start date
      metaCal$timeValiEnd[metaCal$expi] <- timeBgn
      
      # If the valid range for a cal file extends past the data range, truncate it 
      metaCal$timeValiBgn[metaCal$timeValiBgn < timeBgn] <- timeBgn
      metaCal$timeValiEnd[metaCal$timeValiEnd > timeEnd] <- timeEnd
      
      # If we have a valid cal that covers the very first data point, rid ourselves of the expired cal
      if(base::min(metaCal$timeValiBgn[!metaCal$expi]) <= timeBgn){
        metaCal <- metaCal[!metaCal$expi,]
        numCal <- base::nrow(metaCal)
      }
      
      # Order the cals by id (most recent is the largest number)
      setSort <- base::sort(metaCal$id,decreasing=TRUE,index.return=TRUE)$ix
      metaCal <- metaCal[setSort,]
      
      # Go through each calibration file below the most recent, starting with the last. 
      # If it is complete covered in time by the cals above it, it can be removed.
      for(idxCal in base::seq.int(from=numCal,to=2)){
        
        # There's a possibility from above that we only have one cal left. If so, break out of this for loop
        if(idxCal == 1){
          break
        }
        
        # Pull this cal's metadata out
        metaIdx <- metaCal[idxCal,]
        
        # Form the set of Cal's above it
        metaCalEval <- metaCal[1:(idxCal-1),]
        
        # Order the cals above it by start date
        setSort <- base::sort(base::as.numeric(metaCalEval$timeValiBgn),index.return=TRUE)$ix
        metaCalEval <- metaCalEval[setSort,]
        
        # Run through each cal above it, 
        for(idxCalEval in 1:(idxCal-1)){
          # Pull out the more recent one we are comparing against
          metaEvalIdx <- metaCalEval[idxCalEval,]
          
          if(metaIdx$timeValiBgn < metaEvalIdx$timeValiBgn){
            # Stop, we have reached a point where there is a gap to the next start date of a more recent cal
            break
          } else {
            # Update the start date to the end date of the more recent cal (if it's greater)
            metaIdx$timeValiBgn <- base::max(metaIdx$timeValiBgn,metaEvalIdx$timeValiEnd)
          }
        }
        
        # Do we have a start date that is at or after our end date? If so, we have more recent calibrations that cover 
        # this cal, and can therefore get rid of it
        if(metaIdx$timeValiBgn >= metaIdx$timeValiEnd){
          metaCal <- metaCal[-idxCal,]
        }
        
      } # End loop around additional (older) calibration files
      
    } # End if statement for multiple cal files
    
    log$info(base::paste0(base::nrow(metaCal), ' calibration file(s) saved to filtered cal directory ',DirOutCalVar))
    
    # We are left with a filtered cal list. Let's copy the files in that list over to the output directory
    base::system(base::paste0('ln -s ',DirCalVar,'/',metaCal$file,' ',DirOutCalVar, collapse=' && '))
    
  } # End loop around cal streams
    
} # End loop around file paths
