##############################################################################################
#' @title Calibration filtering module for NEON IS data processing

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} 

#' @description Workflow. Filter the directory of calibration files for those that apply to the 
#' data dates. 
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
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.

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
#   Cove Sturtevant (2020-02-10)
#     pulled out major code functionality into functions
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
    
    #Get metadata for all the calibration files in the directory, saving the valid start/end dates & certificate number
    metaCal <- NEONprocIS.cal::def.cal.meta(fileCal=base::paste0(DirCalVar,'/',fileCal),log=log)
    
    # Determine the calibrations that apply for this day
    calSlct <- NEONprocIS.cal::def.cal.slct(metaCal=metaCal,TimeBgn=timeBgn,TimeEnd=timeEnd,log=log)
    fileCalSlct <- base::setdiff(base::unique(calSlct$file),'NA')
    numFileCalSlct <- base::length(fileCalSlct)
    
    log$info(base::paste0(numFileCalSlct, ' calibration file(s) saved to filtered cal directory ',DirOutCalVar))
    
    # We are left with a filtered cal list. Let's copy the files in that list over to the output directory
    if(numFileCalSlct > 0){
      base::system(base::paste0('ln -s ',DirCalVar,'/',fileCalSlct,' ',DirOutCalVar, collapse=' && '))
    }
    
  } # End loop around cal streams
    
} # End loop around file paths
