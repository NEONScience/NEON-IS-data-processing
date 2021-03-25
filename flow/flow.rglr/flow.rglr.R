##############################################################################################
#' @title Regularization module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Bin data to generate a regular time sequence of observations.
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read regularization frequency from location file (if not in input parameters)
#'      Loop through all data files
#'        Regularize data in each file
#'        Write out the regularized data
#'      
#' This script is run at the command line with the following arguments. Each argument must be a
#' string in the format "Para=value", where "Para" is the intended parameter name and "value" is
#' the value of the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the
#' value of the parameter will be assigned from the system environment variable matching the value
#' string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. NOTE: This path must be a
#' parent of the terminal directory where the data to be regularized reside. See argument "DirRglr"
#' below to indicate the terminal directory.
#'
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs', the same name
#' as the terminal directory indicated in argument "DirRglr", or recognizable as the 'yyyy/mm/dd'
#' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained
#' in the folder.
#'
#' For example:
#' Input path = /scratch/pfs/proc_group/soilprt/27134/2019/01/01
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "DirRglr=value", where value is the name of the terminal directory where the data to be
#' regularized resides. This will be one or more child levels away from "DirIn". All files in the
#' terminal directory will be regularized. The value may also be a vector of terminal directories,
#' separated by pipes (|). All terminal directories must be present and at the same directory level.
#' For example, "DirRglr=data|flags" indicates to regularize the data files within each the data
#' and flags directories.
#'
#' 4. "FileSchmRglr=value" (optional), where value is the full path to schema for regularized data output by
#' this workflow. The value may be NA, in which case the output schema will be the same as the input
#' data. The value may be a single file, in which case it will apply to all regularized output, or
#' multiple values in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirRglr and value is the path to the schema file
#' for the regularized output of that directory. Multiple dir:value pairs are separated by pipes (|).
#' For example, "FileSchmRglr=data:/path/to/schemaData.avsc|flags:NA" indicates that the regularized
#' output from the data directory will be written with the schema /path/to/schemaData.avsc and the
#' regularized output from the flags directory will be the same as the input files found in that
#' directory.  NOTE: The choice of inputs RptTimeBin and DropNotNumc will add or remove columns from 
#' the output. Ensure that the output schema reflects the behavior chosen in these parameters. Default
#' value is NA.
#'
#' 5. "FreqRglr=value" (optional), where value is the regularization frequency in Hz. The value may be a single
#' number, in which case it will apply to all terminal directories specified in the DirRglr argument,
#' or multiple values in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirRglr and value is the regularization frequency
#' for the data in that directory. Multiple dir:value pairs are separated by pipes (|). For example,
#' "FreqRglr=data:10|flags:1" indicates that the files in the data directory will be regularized at
#' 10 Hz frequency and the files in the flags directory will be regularized at 1 Hz frequency. If the
#' value is NA (for any/all directories), the regularization frequency for each sensor/location will be
#' retrieved from the "Data Rate" property in the locations file within the locations directory (that
#' must exist at the same level as the terminal directories specified in DirRglr). If any value of
#' FreqRglr is NA, the directory structure must be location-focused, where the parent directory of the
#' terminal directories is named for the named location. Default value is NA.
#'
#' 6. "MethRglr=value" (optional), where value is the regularization method (per the choices in
#' eddy4R.base::def.rglr for input parameter MethRglr). The value may be a single string, in which
#' case it will apply to all terminal directories specified in the DirRglr argument, or multiple values
#' in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirRglr and value is the regularization method
#' for the data in that directory. Multiple dir:value pairs are separated by pipes (|). For example,
#' "MethRglr=data:CybiEc|flags:Cybidflt" indicates that the files in the data directory will be
#' regularized with the "CybiEc" method and files in the flags directory will be regularized with
#' the "CybiDflt" method. Default value is CybiEc, which should be used in most cases. Note that the
#' 'instantaneous' regularization method is 'CybiEcTimeMeas'.
#'
#' 7. "WndwRglr=value" (optional), where value is the windowing method (per the choices in
#' eddy4R.base::def.rglr for input parameter WndwRglr). The value may be a single string, in which
#' case it will apply to all terminal directories specified in the DirRglr argument, or multiple values
#' in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirRglr and value is the the windowing method
#' for the data in that directory. Multiple dir:value pairs are separated by pipes (|). For example,
#' "WndwRglr=data:Trlg|flags:Lead" indicates that the files in the data directory will be
#' regularized with the Trlg windowing method, and files in the flags directory will be regularized with
#' the "Lead" windowing method. Default value is Trlg, and should be used in most cases.
#'
#' 8. "IdxWndw=value" (optional), where value is the index allocation method (per the choices in
#' eddy4R.base::def.rglr for input parameter IdxWndw).  The value may be a single string, in which
#' case it will apply to all terminal directories specified in the DirRglr argument, or multiple values
#' in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirRglr and value is the the index allocation method
#' for the data in that directory. Multiple dir:value pairs are separated by pipes (|). For example,
#' "IdxWndw=data:IdxWndwMin|flags:Cntr" indicates that the files in the data directory will be
#' regularized with the IdxWndwMin index allocation method, and files in the flags directory will be
#' regularized with the "Cntr" index allocation method. Default value is IdxWndwMin, and should be used
#' in most cases.
#' 
#' 9. "RptTimeWndw=value" (optional), where value is logical TRUE or FALSE (default), and pertains to the 
#' choices in eddy4R.base::def.rglr for input parameter RptTimeWndw. TRUE will output
#' two additional columns at the end of the output data file for the start and end times of the time windows
#' used in the regularization. Note that the output variable readout_time will be included in the output
#' regardless of the choice made here, and will probably match the start time of the bin unless 
#' MethRglr=CybiEcTimeMeas. The value in RptTimeWndw=value may be a single string, in which
#' case it will apply to all terminal directories specified in the DirRglr argument, or multiple values
#' in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirRglr and value is the selection of RptTimeWndw
#' for the data in that directory. Multiple dir:value pairs are separated by pipes (|). For example,
#' "RptTimeWndw=data:TRUE|flags:FALSE" indicates that the regularization windows will be added to the output
#' files in the data directory, but will not be added to the output in the flags directory. Note that TRUE
#' is typically used for instantaneous L1 output so that the start and end times of the regularization bins
#' are used in place of the start and end times of the averaging intervals. The default names for the 
#' additional output columns are timeWndwBgn and timeWndwEnd, corresponding to the start (inclusive) and end 
#' (exclusive) times of the regularization bin for each output record. These may be renamed using the 
#' schema provided in argument FileSchmRglr.
#' 
#' 10. "DropNotNumc=value" (optional), where value is logical TRUE (default) or FALSE, and pertains to the 
#' choices in eddy4R.base::def.rglr for input parameter DropNotNumc. TRUE will drop
#' all non-numeric columns prior to the regularization (except for readout_time). Dropped columns will 
#' not be included in the output. The value may be a single string, in which
#' case it will apply to all terminal directories specified in the DirRglr argument, or multiple values
#' in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirRglr and value is the selection of DropNotNumc
#' for the data in that directory. Multiple dir:value pairs are separated by pipes (|). For example,
#' "DropNotNumc=data:TRUE|flags:FALSE" indicates that the non numeric columns will be dropped in the output 
#' files in the data directory, but non-numeric columns will be retained (and regularized) for data in the 
#' flags directory. Ensure that any schemas provided for the output files account for the choice(s) made
#' here (i.e. there may be fewer columns if DropNotNumc=TRUE)
#'
#' 11. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the regularization folder in the input path that are to be copied with a
#' symbolic link to the output path.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return Regularized data output in Parquet format in DirOut, where DirOut directory
#' replaces BASE_REPO but otherwise retains the child directory structure of the input path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.rglr.R "DirIn=/pfs/prt_calibration/prt/2019/01/01" "DirOut=/pfs/out" "DirRglr=data" "FileSchmRglr=/pfs/avro_schemas/data_regularized.avsc" "FreqRglr=0.1" "MethRglr=CybiEc" "WndwRglr=Trlg" "IdxWndw=IdxWndwMin"
#'
#' Using environment variables for input directory and output file schema
#' Sys.setenv(DIR_IN='/pfs/prt_calibration/prt/2019/01/01')
#' Sys.setenv(FILE_SCHEMA_RGLR='/pfs/avro_schemas/data_regularized.avsc')
#' Rscript flow.rglr.R "DirIn=$DIR_IN" "DirOut=/pfs/out" "DirRglr=data" "FileSchmRglr=$FILE_SCHEMA_RGLR" "FreqRglr=0.1" "MethRglr=CybiEc" "WndwRglr=Trlg" "IdxWndw=IdxWndwMin"


#' @seealso \code{\link[eddy4R.base]{def.rglr}}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-02-16)
#     original creation with read/write in csv until AVRO R-package available
#   Cove Sturtevant (2019-02-20)
#     generalized script to accept command line arguments for input/output directory
#       and regularization frequency
#   Cove Sturtevant (2019-03-21)
#     Read/write in AVRO format.
#     Output to same directory structure as input path.
#   Cove Sturtevant (2019-05-01)
#     add looping through datums, and added some additional logging
#   Cove Sturtevant (2019-05-10)
#     add hierarchical logging
#   Cove Sturtevant (2019-05-15)
#     Made detection of repos to process more flexible, and adjusted associated input(s)
#   Cove Sturtevant (2019-09-13)
#     added reading of input path from environment variables
#     simplified fatal errors
#     adjusted the arguments to indicate the output schema for regularized data
#   Cove Sturtevant (2019-09-30)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#     allow regularizing of more than one directory, each with the same or separate regularization options
#   Cove Sturtevant (2019-10-01)
#     return regularized data as the same class it came in as
#   Cove Sturtevant (2020-02-18)
#     make option to look for regularization frequency within the location file
#     add instantaneous regularization option (just a parameter input to eddy4R.base::def.rglr)
#     pulled out some code into functions
#   Cove Sturtevant (2020-04-15)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2021-02-02)
#     add option to include the start and end times for the bins in the output
#     add option to regularize non-numeric columns instead of dropping from output
#     fix bug causing incorrect parameter assignment if regularization directories aren't 
#        all in the same order in the input arguments
#   Cove Sturtevant (2021-03-03)
#     Applied internal parallelization
##############################################################################################
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
arg <- base::commandArgs(trailingOnly = TRUE)

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", "DirOut", "DirRglr"),
    NameParaOptn = c(
      "FileSchmRglr",
      "FreqRglr",
      "MethRglr",
      "WndwRglr",
      "IdxWndw",
      "DirSubCopy",
      "RptTimeWndw",
      "DropNotNumc"
    ),
    ValuParaOptn = base::list(
      FileSchmRglr = "NA",
      FreqRglr = NA,
      MethRglr = "CybiEc",
      WndwRglr = "Trlg",
      IdxWndw = "IdxWndwMin",
      RptTimeWndw=FALSE,
      DropNotNumc=TRUE
    ),
    TypePara = base::list(
      FreqRglr = "numeric"
    ),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(base::paste0(
  'Terminal Directories to regularize: ',
  base::paste0(Para$DirRglr, collapse = ',')
))

# Retrieve output schema(s)
log$debug(base::paste0(
  'Output schema(s) for regularized data: ',
  base::paste0(Para$FileSchmRglr, collapse = ',')
))
SchmRglr <-
  NEONprocIS.base::def.vect.pars.pair(
    vect = Para$FileSchmRglr,
    KeyExp = Para$DirRglr,
    ValuDflt = 'NA',
    NameCol = c('DirRglr', 'FileSchmRglr'),
    log = log
  )

# Read in the schema(s)
SchmRglr$SchmRglr <- NA
for (idxSchmRglr in 1:base::length(Para$DirRglr)) {
  if (SchmRglr$FileSchmRglr[idxSchmRglr] != 'NA') {
    SchmRglr$SchmRglr[idxSchmRglr] <-
      base::paste0(base::readLines(SchmRglr$FileSchmRglr[idxSchmRglr]),
                   collapse = '')
  }
}
SchmRglr <- SchmRglr[base::match(Para$DirRglr, SchmRglr$DirRglr),]

# Retrieve regularization frequency
log$debug(base::paste0(
  'Regularization frequency(ies): ',
  base::paste0(Para$FreqRglr, collapse = ',')
))
FreqRglr <-
  NEONprocIS.base::def.vect.pars.pair(
    vect = Para$FreqRglr,
    KeyExp = Para$DirRglr,
    ValuDflt = base::as.numeric(NA),
    NameCol = c('DirRglr', 'FreqRglr'),
    Type = c('character', 'numeric'),
    log = log
  )
FreqRglr <- FreqRglr[base::match(Para$DirRglr, FreqRglr$DirRglr),]

# Retrieve regularization method
log$debug(base::paste0(
  'Regularization method(s): ',
  base::paste0(Para$MethRglr, collapse = ',')
))
MethRglr <-
  NEONprocIS.base::def.vect.pars.pair(
    vect = Para$MethRglr,
    KeyExp = Para$DirRglr,
    ValuDflt = "CybiEc",
    NameCol = c('DirRglr', 'MethRglr'),
    log = log
  )
MethRglr <- MethRglr[base::match(Para$DirRglr, MethRglr$DirRglr),]

# Retrieve windowing parameter
log$debug(base::paste0(
  'Windowing parameter(s): ',
  base::paste0(Para$WndwRglr, collapse = ',')
))
WndwRglr <-
  NEONprocIS.base::def.vect.pars.pair(
    vect = Para$WndwRglr,
    KeyExp = Para$DirRglr,
    ValuDflt = "Trlg",
    NameCol = c('DirRglr', 'WndwRglr'),
    log = log
  )
WndwRglr <- WndwRglr[base::match(Para$DirRglr, WndwRglr$DirRglr),]

# Retrieve Index allocation method
log$debug(base::paste0(
  'Index allocation method(s): ',
  base::paste0(Para$IdxWndw, collapse = ',')
))
IdxWndw <-
  NEONprocIS.base::def.vect.pars.pair(
    vect = Para$IdxWndw,
    KeyExp = Para$DirRglr,
    ValuDflt = "Trlg",
    NameCol = c('DirRglr', 'IdxWndw'),
    log = log
  )
IdxWndw <- IdxWndw[base::match(Para$DirRglr, IdxWndw$DirRglr),]

# Retrieve choice to output regularization windows
log$debug(base::paste0(
  'Choice(s) to report regularization windows in the output: ',
  base::paste0(Para$RptTimeWndw, collapse = ',')
))
RptTimeWndw <-
  NEONprocIS.base::def.vect.pars.pair(
    vect = Para$RptTimeWndw,
    KeyExp = Para$DirRglr,
    ValuDflt = FALSE,
    NameCol = c('DirRglr', 'RptTimeWndw'),
    Type = c('character', 'logical'),
    log = log
  )
RptTimeWndw <- RptTimeWndw[base::match(Para$DirRglr, RptTimeWndw$DirRglr),]

# Retrieve choice to drop non-numeric columns from output
log$debug(base::paste0(
  'Choice(s) to drop non-numeric columns from output: ',
  base::paste0(Para$DropNotNumc, collapse = ',')
))
DropNotNumc <-
  NEONprocIS.base::def.vect.pars.pair(
    vect = Para$DropNotNumc,
    KeyExp = Para$DirRglr,
    ValuDflt = TRUE,
    NameCol = c('DirRglr', 'DropNotNumc'),
    Type = c('character', 'logical'),
    log = log
  )
DropNotNumc <- DropNotNumc[base::match(Para$DirRglr, DropNotNumc$DirRglr),]

# Group all the regularization parameters into a single data frame
ParaRglr = base::cbind(
  SchmRglr, 
  FreqRglr['FreqRglr'], 
  MethRglr['MethRglr'], 
  WndwRglr['WndwRglr'], 
  IdxWndw['IdxWndw'],
  RptTimeWndw['RptTimeWndw'],
  DropNotNumc['DropNotNumc'])
expcLoc <-
  base::any(base::is.na(ParaRglr$FreqRglr)) # Do we need location info?

# Retrieve optional subdirectories to copy over
DirSubCopy <-
  base::unique(base::setdiff(Para$DirSubCopy, Para$DirRglr))
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# Determine expected subdirectories of each input path
if (expcLoc) {
  # At least one regularization frequency will be obtained from the location file
  nameDirSub <- base::as.list(c(Para$DirRglr, 'location'))
} else {
  # No location directory needed
  nameDirSub <- base::as.list(c(Para$DirRglr))
}
log$debug(base::paste0(
  'Expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub = nameDirSub,
                              log = log)

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing datum path: ', idxDirIn))
  
  # Gather info about the input directory (including date) and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-
    InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  idxDirOut <- base::paste0(Para$DirOut, InfoDirIn$dirRepo)
  idxDirLoc <- base::paste0(idxDirIn, '/location')
  fileLoc <- base::dir(idxDirLoc)
  numFileLoc <- base::length(fileLoc)
  
  # Error check - quit if we need locations and they aren't there
  if (expcLoc && numFileLoc == 0) {
    log$error(base::paste0('No location data found in ', DirLoc))
    stop()
  } else if (numFileLoc > 1) {
    fileLoc <- fileLoc[1]
    log$warn(base::paste0(
      'There is more than location file in ',
      idxDirLoc,
      '. Using ',
      fileLoc
    ))
  } 
  
  # Read regularization frequency from location file if expected
  if (expcLoc){
    # Grab the named location from the directory structure
    nameLoc <-
      utils::tail(InfoDirIn$dirSplt, 1) # Location identifier from directory path
    
    # Find the location we're looking for in the locations file
    nameFileLoc <- base::paste0(idxDirLoc, '/', fileLoc)
    locMeta <-
      NEONprocIS.base::def.loc.meta(
        NameFile = nameFileLoc,
        NameLoc = nameLoc,
        TimeBgn = timeBgn,
        TimeEnd = timeEnd,
        log = log
      )
    FreqRglrIdxLoc <- base::as.numeric(locMeta$dataRate[1])
    
    # Error check
    if (base::is.na(FreqRglrIdxLoc)) {
      log$error(
        base::paste0(
          'Cannot determine regularization frequency from location file for datum path ',
          idxDirIn
        )
      )
      stop()
    }
    
    log$debug(base::paste0('Regularization frequency: ',FreqRglrIdxLoc, ' Hz read from location file'))
  }
  
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn, '/', DirSubCopy), idxDirOut, log =
                                         log)
  }
  
  # Run through each directory to regularize
  for (idxDirRglr in Para$DirRglr) {
    # Row index to parameter set
    idxRowParaRglr <- base::which(ParaRglr$DirRglr == idxDirRglr)
    
    # Use regularization frequency from the location file if not in input args
    FreqRglrIdx <- ParaRglr$FreqRglr[idxRowParaRglr]
    if (base::is.na(FreqRglrIdx)) {
      # Use the frequency in the locations file instead
      FreqRglrIdx <- FreqRglrIdxLoc
    }
    log$debug(base::paste0('Regularization frequency to be used for ',idxDirRglr,' directory: ',FreqRglrIdx, ' Hz'))
    
    
    # Get directory listing of input directory
    idxDirInRglr <-  base::paste0(idxDirIn, '/', idxDirRglr)
    fileData <- base::dir(idxDirInRglr)
    if (base::length(fileData) > 1) {
      log$warn(
        base::paste0(
          'There is more than one data file in path: ',
          idxDirIn,
          '... Regularizing them all!'
        )
      )
    }
    
    # Create output directory
    idxDirOutRglr <- base::paste0(idxDirOut, '/', idxDirRglr)
    base::dir.create(idxDirOutRglr, recursive = TRUE)
    
    # Regularize each file
    for (idxFileData in fileData) {
      # Load data
      fileIn <- base::paste0(idxDirInRglr, '/', idxFileData)
      data  <-
        base::try(NEONprocIS.base::def.read.parq(NameFile = fileIn,
                                                 log = log),
                  silent = FALSE)
      if (base::any(base::class(data) == 'try-error')) {
        log$error(base::paste0('File ', fileIn, ' is unreadable.'))
        stop()
      }
      nameVarIn <- base::names(data)
      
      # Pull out the time variable
      if (!('readout_time' %in% nameVarIn)) {
        log$error(
          base::paste0(
            'Variable "readout_time" is required, but cannot be found in file: ',
            fileIn
          )
        )
        stop()
      }
      
      # Regularize the data
      BgnRglr <- base::as.POSIXlt(timeBgn)
      EndRglr <- base::as.POSIXlt(timeEnd)
      idxTime <- base::which(nameVarIn == 'readout_time')
      dataRglr <-
        eddy4R.base::def.rglr(
          timeMeas = base::as.POSIXlt(data$readout_time),
          dataMeas = base::subset(data, select = -idxTime),
          BgnRglr = BgnRglr,
          EndRglr = EndRglr,
          FreqRglr = FreqRglrIdx,
          MethRglr = ParaRglr$MethRglr[idxRowParaRglr],
          WndwRglr = ParaRglr$WndwRglr[idxRowParaRglr],
          IdxWndw = ParaRglr$IdxWndw[idxRowParaRglr],
          DropNotNumc = ParaRglr$DropNotNumc[idxRowParaRglr],
          RptTimeWndw = ParaRglr$RptTimeWndw[idxRowParaRglr]
        )
      
      # Make sure we return the regularized data as the same class it came in with
      for (idxVarRglr in base::names(dataRglr$dataRglr)) {
        base::class(dataRglr$dataRglr[[idxVarRglr]]) <-
          base::class(data[[idxVarRglr]])
      }
      
      # Add the readout time back in, and potentially the bin start and end times
      rpt <-
        base::data.frame(readout_time = dataRglr$timeRglr,
                         stringsAsFactors = FALSE)
      rpt <- base::cbind(rpt, dataRglr$dataRglr)
      
      # Match the original column order (minus any variables we dropped)
      setColOrd <- base::match(nameVarIn,base::names(rpt))
      rpt <- rpt[,setColOrd[!is.na(setColOrd)]]
      
      # Tack on the time window start and end times to the end of the data frame
      if(ParaRglr$RptTimeWndw[idxRowParaRglr] == TRUE){
        rpt <- base::cbind(rpt, dataRglr$timeWndw)
      }
      
      # Remove any data points outside this day
      rpt <-
        rpt[rpt$readout_time >= BgnRglr & rpt$readout_time < EndRglr, ]
      
      # select output schema
      if (base::is.na(ParaRglr$SchmRglr[idxRowParaRglr])) {
        # use the output data to generate a schema
        idxSchmRglr <- base::attr(rpt, 'schema')
      } else {
        idxSchmRglr <- ParaRglr$SchmRglr[idxRowParaRglr]
      }
      
      # Write the output
      fileOut <- base::paste0(idxDirOutRglr, '/', idxFileData)
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = rpt,
          NameFile = fileOut,
          NameFileSchm = NULL,
          Schm = idxSchmRglr
        ),
        silent = TRUE)
      if (base::class(rptWrte) == 'try-error') {
        log$error(base::paste0(
          'Cannot write regularized data in file ',
          fileOut,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Regularized data written successfully in file: ',
          fileOut
        ))
      }
      
      
    } # End loop around files to regularize
  } # End loop around directories to regularize
  
  return()
} # End loop around datum paths
