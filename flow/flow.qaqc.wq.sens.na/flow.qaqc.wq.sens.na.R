##############################################################################################
#' @title Verify completeness of water quality sensor records.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Verify completeness of water quality records. Originally known as the
#' 'buoy NA test'. This module opens the data files for all sensors marked as a data product group
#' and checks that all timestamps are present across data files and no data values are NA. For any
#' records failing the check, missing timestamps are filled and data values for ALL sensors turned
#' to NA.
#'
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum (sensor group):
#'      Check for missing or corrupt records across the sensor set set
#'      Create sensorNA flag indicating missing/corrupt records
#'      Fill in missing/corrupt records with NA (-1 for existing flags)
#'      Write out the adjusted data and flags for each sensor
#'
#' This script is run at the command line with the following arguments. Each argument must be a
#' string in the format "Para=value", where "Para" is the intended parameter name and "value" is
#' the value of the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the
#' value of the parameter will be assigned from the system environment variable matching the value
#' string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. NOTE: This path must be at the
#' GROUP_ID or higher (see below).
#'
#' The input data repository is structured: #/pfs/BASE_REPO/#/yyyy/mm/dd/#/GROUP_ID/SOURCE_TYPE/NAME_LOC/,
#' where # indicates any number of parent and child directories of any name, so long as they are not
#' 'pfs', the same name as the other data directores indicated in argument "DirSubAply", or recognizable as
#' the 'yyyy/mm/dd' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data
#' contained in the folder. BASE_REPO is the repository name. GROUP_ID is the unique identifier for the
#' group of sensors that make up one instance of the data product. The sensor data found in the child
#' directories of GROUP_ID are considered one suite of sensor data for which all sensors must have non-NA
#' records at the same timestamps. Within the GROUP_ID folder, each sensor type (SOURCE_TYPE) has its own
#' child directory, and individual sensor locations for that sensor type, each with their own unique NAME_LOC
#' identifier (NAME_LOC), are futher child directories of SOURCE_TYPE. Each NAME_LOC directory must contain
#' a child directory called 'data', within which the calibrated data at native resolution is found in a
#' single file. There may exist other subfolders at the same level of 'data' and these will be copied through
#' to the output repository untouched unless they are intended to be modified as indicated in argument DirAply
#' (see that argument for details).
#'
#' An example input argument for DirIn:
#' "DirIn = /scratch/pfs/waterQuality_group/2019/01/01/group001/"
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "FileSchmQf=value" (optional), where value is the full path to avro schema for the single quality flag
#' output by this workflow. If not input, the schema will be created automatically.The default column naming
#' (and order) is "readout_time", "qfCmpl".
#'
#' 4. "DirTypeSrc=value", where value contains the exact names of the minimum SOURCE_TYPE directories
#' that are always expected to be found within the GROUP_ID folder (see argument DirIn above). This is how
#' the code will identify sensor groups to process. Separate multiple names with pipes (|).
#' For example, "DirTypeSrc=exo2|exoconductivity|exodossolvedoxygen" indicates that when these directory
#' names are found at the same level, the parent of these directories is the GROUP_ID directory and all
#' data found within the child directories of GROUP_ID will be evaluated together for timestamps consistency
#' and non-NA values. Note that more SOURCE_TYPE directories than indicated in DirTypeSrc may be found at the
#' same level as those that are indicated, allowing some flexibility in identifying the GROUP_ID directory
#' when some SOURCE_TYPEs exist in some groups but not others.
#'
#' 5. "DirSubAply=value" (optional), where value contains the name of subdirectories, separated by pipes (|),
#' at the same level as the 'data' directory (see argument DirIn above) for which missing records should be
#' inserted and values turned to NA as those identified in the data (in the data directory). Note that files
#' in DirSubAply are not evaluated, simply the failure action is applied. Do not include the directory 'flags'
#' here. The failure action will automatically be applied to data files in the flags directory, with the
#' exception that flag values will be turned to -1.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A repository structure in DirOut, where DirOut replaces the input directory structure up to
#' #/pfs/BASE_REPO (see inputs above) but otherwise retains the child directory structure of the input path.
#' Data files in the repository will all contain consistent timestamps and values turned to NA for records in
#' which any sensor data file was missing the timestamps or contained NA values. The same applies to files within
#' the DirSubAply directories and the flags directory, with the exception that flag values are turned to -1 for
#' records that failed the check. A new file will be added to the flags directory with the results of the check
#' (0=pass,1=fail)
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.qaqc.wq.cmpl.R "DirIn='/scratch/pfs/waterQuality_related_location_group/exo/2019/01/01" "DirOut=/scratch/pfs/out" "FileSchmQf=/scratch/pfs/avro_schemas/dp0p/flags_wq_sensor_NA.avsc" "DirTypeSrc=exo2|exoconductivity|exodissolvedoxygen|exophorp|exototalalgae|exoturbidity" "DirSubAply=uncertainty_data"

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-03-02)
#     original creation
#   Cove Sturtevant (2020-04-28)
#     switch read/write data from avro to parquet
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
    NameParaReqd = c("DirIn", "DirOut", "DirTypeSrc"),
    NameParaOptn = c("FileSchmQf", "DirSubAply"),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))

# Retrieve output flag schema
log$debug(base::paste0('Output schema for flags: ', Para$FileSchmQf))

# Read in the schema
if (!base::is.null(Para$FileSchmQf)) {
  SchmQf <-
    base::paste0(base::readLines(Para$FileSchmQf), collapse = '')
} else {
  SchmQf <- NULL
}


log$debug(base::paste0(
  'Expected SOURCE_TYPE directories of each datum path: ',
  base::paste0(Para$DirTypeSrc, collapse = ',')
))

log$debug(
  base::paste0(
    'Additional subdirectories (besides data and flags) to apply failure outcome: ',
    base::paste0(Para$DirSubAply, collapse = ',')
  )
)
dirMod <- c('data', 'flags', Para$DirSubAply)



# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(
    DirBgn = Para$DirIn,
    nameDirSub = Para$DirTypeSrc,
    log = log
  )

# Process each datum
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing datum path: ', idxDirIn))
  
  # Determine the paths to the NAME_LOC directories (i.e. all the sensors) of this group
  dirLocName <- NEONprocIS.base::def.dir.in(DirBgn = idxDirIn,
                                            nameDirSub = 'data',
                                            log = log)
  
  # Determine all the files within the data directories
  fileData <-
    base::list.files(base::paste0(dirLocName, '/data'), full.names = TRUE)
  
  # Find inconsistent or corrupt records
  chkRcd <-
    base::try(NEONprocIS.base::def.rcd.miss.na(fileData = fileData, log =
                                                      log),
              silent = FALSE)
  if (base::class(data) == 'try-error') {
    base::stop()
  }
  timeBad <- chkRcd$timeBad
  timeAll <- chkRcd$timeAll
  
  # Create the flags
  qf <-
    base::data.frame(readout_time = timeAll$readout_time, qfSensNa = 0)
  qf$qfSensNa[timeAll$readout_time %in% timeBad$readout_time] <- 1
  class(qf$qfSensNa) <- 'integer'
  
  
  # Go back through the data and NA-out missing or bad timestamps
  for (idxDirLocName in dirLocName) {
    # Gather info about the input directory (including date)
    InfoDirLocName <-
      NEONprocIS.base::def.dir.splt.pach.time(idxDirLocName)
    dirLocNameOut <-
      base::paste0(Para$DirOut, InfoDirLocName$dirRepo)
    
    # What subdirectories are in this named location path
    dirSub <- base::dir(idxDirLocName)
    
    # Pull the first data file
    fileData <-
      base::list.files(base::paste0(idxDirLocName, '/data'), full.names = FALSE)[1]
    
    # Start with flags file, this always gets written
    dirOutQf <- base::paste0(dirLocNameOut, '/flags')
    NEONprocIS.base::def.dir.crea(DirBgn = dirLocNameOut,
                                  DirSub = 'flags',
                                  log = log)
    NameFileOutQf <-
      NEONprocIS.base::def.file.name.out(nameFileIn = fileData, sufx = '_flagsSensNa')
    NameFileOutQf <- base::paste0(dirOutQf, '/', NameFileOutQf)
    rptQfCal <-
      NEONprocIS.base::def.wrte.parq(
        data = qf,
        NameFile = NameFileOutQf,
        NameFileSchm = NULL,
        Schm = SchmQf
      )
    if (rptQfCal == 0) {
      log$info(base::paste0('Sensor NA flags written successfully in ', NameFileOutQf))
    }
    
    # Copy over any other directories that are not impacted by the test
    dirSubCopy <-
      base::setdiff(dirSub, c('data', 'flags', Para$DirSubAply))
    NEONprocIS.base::def.dir.copy.symb(DirSrc = base::paste0(idxDirLocName, '/', dirSubCopy),
                                       DirDest = dirLocNameOut,
                                       log=log)
    
    # Now handle directories potentially impacted by the test.
    if (base::nrow(timeBad) == 0) {
      # No bad records were found. Copy over directories unmodified.
      dirSubCopy <- c('data', Para$DirSubAply)
      NEONprocIS.base::def.dir.copy.symb(
        DirSrc = base::paste0(idxDirLocName, '/', dirSubCopy),
        DirDest = dirLocNameOut,
        log=log
      )
      
      # Copy over any other flags files in the flags directory
      if ('flags' %in% dirSub) {
        dirQf <- base::paste0(idxDirLocName, '/flags')
        fileQf <- base::list.files(dirQf)
        
        base::system(base::paste0('ln -s ',
                                  dirQf,
                                  '/',
                                  fileQf,
                                  ' ',
                                  dirOutQf,
                                  collapse = ' && '))
        
      }
    } else {
      # Bad records found. Need to NA-out bad records in the following directories
      for (idxDirMod in base::intersect(dirMod, dirSub)) {
        # Create the output directory
        idxDirOut <- base::paste0(dirLocNameOut, '/', idxDirMod)
        NEONprocIS.base::def.dir.crea(DirBgn = dirLocNameOut,
                                      DirSub = idxDirMod,
                                      log = log)
        
        fileMod <-
          base::list.files(base::paste0(idxDirLocName, '/', idxDirMod))
        
        for (idxFileMod in fileMod) {

          # Open file and error check
          nameFileMod <-
            base::paste0(idxDirLocName, '/', idxDirMod, '/', idxFileMod)
          data  <-
            base::try(NEONprocIS.base::def.read.parq(NameFile = nameFileMod,
                                                     log = log),
                      silent = FALSE)
          
          if (base::class(data) == 'try-error') {
            # Generate error and stop execution
            log$error(base::paste0('File ', nameFileMod, ' is unreadable.'))
            base::stop()
          }
          chkCol <-
            NEONprocIS.base::def.validate.dataframe(
              dfIn = data,
              TestNa = FALSE,
              TestNumc = FALSE,
              TestNameCol = 'readout_time',
              log = log
            )
          if (!chkCol) {
            stop()
          }
          
          # Choose what value to fill with
          if (idxDirMod == 'flags') {
            # Fill flags
            valuBad <- -1
          } else {
            valuBad <- NA
          }
          
          # Fix bad records
          dataMod <-
            NEONprocIS.base::def.rcd.fix.miss.na(
              data = data,
              timeBad = timeBad,
              valuBad = valuBad,
              log = log
            )
          
          
          # Write it out
          fileOut <- base::paste0(idxDirOut, '/', fileMod)
          rptWrte <-
            base::try(NEONprocIS.base::def.wrte.parq(
              data = dataMod,
              NameFile = fileOut,
              NameFileSchm = NULL,
              Schm = base::attr(fileMod, 'schema')
            ),
            silent = TRUE)
          if (base::class(rptWrte) == 'try-error') {
            log$error(base::paste0(
              'Cannot write file ',
              fileOut,
              '. ',
              attr(rptWrte, "condition")
            ))
            stop()
          } else {
            log$info(base::paste0(
              'Adjusted records written successfully to file: ',
              fileOut
            ))
          }
          
          
        } # End loop around files to modify
        
      } # End loop around directories impacted by the test
      
    } # End if statement for bad records found
    
  } # End second pass through sensor locations
  
  return()
} # End loop around datums to process
