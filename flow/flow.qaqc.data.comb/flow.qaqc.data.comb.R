##############################################################################################
#' @title Combines the filtered data output resulting from multiple quality tests

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Combines the filtered data output resulting from multiple quality tests in 
#' which each test or set of tests saved a file of the filtered data. Filtering involved turning
#' data values to NA according to the specifications of the test. This code assumes the remaining
#' data values are identical among files, simply that the locations of NA values are different. The 
#' result will be a single file with NA indices representing the union of all files.
#' 
#' This script is run at the command line with 3+ arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder. 
#' Files to be combined are located in the terminal director(ies), as specified in the dirComb below. Note that all terminal
#' directories must be at the same level.
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' 3. "DirSubCombData=value", where the value is the name of subfolders holding timeseries files 
#' (e.g. data) which are to be merged, separated by pipes (|). These additional subdirectories must be at the same 
#' level in the directory structure. Within each subfolder are timeseries files, arbitratily named. The timeseries
#' length and name & number of columns must be identical among the files. The only difference will be the specific
#' indices of data values that are NA. 
#' 
#' 5. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by pipes, at 
#' the same level as the folders specified in DirSubCombData that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return A repository structure in DirOut, where DirOut replaces the input directory 
#' structure up to #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure 
#' of the input path. A single merged file will replace the originals in each of the subdirectories
#' specified in the DirSubCombData argument. The indices of NA values in the data of the merged file will represent
#' the union of all NA indices encountered in the input files. The merged file will be written with the same schema 
#' as returned from reading the input file(s). All other subdirectories indicated in DirSubCopy will be carried through 
#' unmodified.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.qaqc.data.comb.R 'DirIn=/pfs/tempAirSingle_qaqc_data_group/prt/2018/01/01' 'DirOut=/pfs/out' 'DirSubCombData=data' 'DirSubCopy=location'
#' 
#' @seealso None
#' 
# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-12-20)
#     original creation
#   Cove Sturtevant (2020-04-28)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
##############################################################################################
options(digits.secs = 3)
library(foreach)
library(doParallel)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Use environment variable to specify how many cores to run on
numCoreUse <- base::as.numeric(Sys.getenv('PARALLELIZATION_INTERNAL'))
numCoreAvail <- parallel::detectCores()
if (base::is.na(numCoreUse)){
  numCoreUse <- 1
} 
if(numCoreUse > numCoreAvail){
  numCoreUse <- numCoreAvail
}
log$debug(paste0(numCoreUse, ' of ',numCoreAvail, ' available cores will be used for internal parallelization.'))

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut"),
                                      NameParaOptn=c("DirSubCombData","DirSubCopy"),log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve optional data directories to merge timeseries files in
DirSubCombData <- Para$DirSubCombData
log$debug(base::paste0('Subdirectories to combine timeseries files within: ',base::paste0(DirSubCombData,collapse=',')))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,DirSubCombData))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c(DirSubCombData,DirSubCopy))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths. We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Gather info about the input directory and formulate the parent output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  
  # Copy with a symbolic link the desired subfolders we aren't modifying
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut,log=log)
  }  
  
  # For each data directory, merge the data within based on the union of NA indices of the individual files
  for (idxDirSubCombData in DirSubCombData){
    
    # Create the output directory
    base::suppressWarnings(base::dir.create( base::paste0(idxDirOut,'/',idxDirSubCombData),recursive=TRUE))

    # Get a file listing
    fileData <- base::dir(base::paste0(idxDirIn,'/',idxDirSubCombData))
    
    dataOut <- NULL # Initialize output
    
    # Go through each sensor file, grabbing data installed at the location
    for(idxFileData in fileData){
      
      nameFileData <- base::paste0(idxDirIn,'/',idxDirSubCombData,'/',idxFileData) # Full path to file
      
      # Open the data file
      data  <- base::try(NEONprocIS.base::def.read.parq(NameFile=nameFileData,log=log),silent=FALSE)
      if(base::class(data) == 'try-error'){
        # Generate error and stop execution
        log$error(base::paste0('File: ', nameFileData, ' is unreadable.')) 
        stop()
      }
      
      # If we haven't saved any data. Initialize with this file
      if(base::is.null(dataOut)){
        dataOut <- data
      }
      
      # Error check to ensure the data size and columns are the same
      if(base::nrow(dataOut) != base::nrow(data)){
        # Generate error and stop execution
        log$error(base::paste0('File: ', nameFileData, ' contains a different number of rows (', base::nrow(data),
                               ') compared to previously read files (', base::nrow(dataOut),
                               '). The data sizes should be identical across all files in the directory.')) 
        stop()
      }
      if(all.equal(base::names(dataOut),base::names(data)) != TRUE){
        # Generate error and stop execution
        log$error(base::paste0('File: ', nameFileData, ' contains different columns (', base::paste0(base::names(data),collapse=','),
                               ') compared to previously read files (', base::paste0(base::names(dataOut),collapse=','),
                               '). The columns should be identical across all files in the directory.')) 
        stop()
      }
      
      # Union the NA indices between any existing output data and this input file
      dataOut[base::is.na(data)] <- NA
        
    } # End loop around sensor files
      
    # Write the output

    # Choose the shortest file name
    fileDataOut <- fileData[base::nchar(fileData) == base::min(nchar(fileData))][1]
    nameFileDataOut <- base::paste0(idxDirOut,'/',idxDirSubCombData,'/',fileDataOut) # Full path to output file
      
    # Read the schema from the input data file
    SchmOut <- base::attr(dataOut,'schema')
    rptDataOut <- base::try(NEONprocIS.base::def.wrte.parq(data=dataOut,NameFile=nameFileDataOut,NameFileSchm=NULL,Schm=SchmOut,log=log),silent=TRUE)
    if(base::class(rptDataOut) == 'try-error'){
      log$error(base::paste0('Cannot write merged file ', nameFileDataOut,'. ',attr(rptDataOut,"condition"))) 
      stop()
    } else {
      if(base::is.null(SchmOut)){
        log$debug(base::paste0('Schema for writing output file ',nameFileDataOut, 'could not be read from input file. Using output data frame to auto-create schema.'))
      } else {
        log$debug(base::paste0('Same schema as input data files used to write output file ',nameFileDataOut))
      }
      log$info(base::paste0('Merged timeseries data written successfully in ',nameFileDataOut))
    }
    
  } # End loop around data directories
  
  return()
} # End loop around datums
