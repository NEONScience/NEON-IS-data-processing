##############################################################################################
#' @title Merge the contents of multiple data files that share a common time variable

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Merge the contents of multiple data files that share a common time
#' variable but different data columns. Note that if the same column name (other than the
#' time variable) is found in more than one input file, only the first intance will be retained
#' for output. Any missing timestamps among the files will be filled
#' with NA values for the affected columns. Optionally select and/or rearrange columns for
#' output.
#'
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Read in and combine all the files for each input datum
#'      Select/arrange columns for output
#'      Write out the combined data file
#'
#' This script is run at the command line with the following arguments. Each argument must be a string
#' in the format "Para=value", where "Para" is the intended parameter name and "value" is the value of
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are:
#'
#' 1. "DirIn=value", where value is the path to the input data directory. NOTE: This path must be a
#' parent of the terminal directory where the data to be combined resides. See argument "DirComb"
#' below to indicate the terminal directory.
#'
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs', the same name
#' as the terminal directory indicated in argument "DirComb", or recognizable as the 'yyyy/mm/dd'
#' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained
#' in the folder.
#'
#' For example:
#' Input path = /scratch/pfs/proc_group/soilprt/27134/2019/01/01
#'
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' 3. "DirComb=value", where value is the name(s) of the terminal directories, separated by pipes,
#' where the data to be combined resides. This will be one or more child levels away from "DirIn".
#' All files in the terminal directories will be combined into a single file. The value may also be
#' a vector of terminal directories, separated by pipes (|). All terminal directories must be present
#' and at the same directory level. For example, "DirComb=data|flags" indicates to combine all the
#' files within the data and flags directories into a single file.
#'
#' 4. "NameDirCombOut=value", where value is the name of the output directory that will be created to
#' hold the combined file. It may be the same as one of DirComb, but note that the original directory
#' may be be copied through to the output in argument DirSubCopy.
#'
#' 5. "NameFileSufx=value" (optional), where value is a character suffix to add to the output
#' file name (before any extension). For example, if the shortest file name found in the input files is 
#' "prt_CFGLOC12345_2019-01-01.avro", and the input argument is "NameFileSufx=_stats_100", then the 
#' output file will be "prt_CFGLOC12345_2019-01-01_stats_100.avro". Default is no suffix.
#'  
#' 6. "NameVarTime=value", where value is the name of the time variable common across all
#' files. Note that any missing timestamps among the files will be filled with NA values.
#'
#' 7. "FileSchmComb=value" (optional), where value is the full path to schema for combined data output by
#' this workflow. If not input, the schema will be constructed from the output data frame.
#'
#' 8. "ColKeep=value" (optional), value contains the names, in desired order, of the input columns
#' that should be copied over to the combined output file. The column names indicated here must be a
#' full or partial set of the union of the column names found in the input files. Use the output
#' schema in argument FileSchmComb to rename them as desired. Note that column names may be listed
#' more than once here. In that case the same data will be duplicated in the indicated columns, but
#' the second and greater instance will have an index appended to the end of the column name.
#' If this argument is omitted, all columns found in the input files for each directory will be included
#' in the output file in the order they are encountered in the input files.
#'
#' 9. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by
#' pipes, at the same level as the flags folder in the input path that are to be copied with a
#' symbolic link to the output path.
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.
#'
#' @return A single file containined the merged data in DirOut, where DirOut replaces BASE_REPO but
#' otherwise retains the child directory structure of the input path. The file name will be the same
#' as the shortest file name found in the input files, with '_combined' added as suffix prior to the
#' file extension.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-03-12)
#     original creation
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
    NameParaReqd = c("DirIn", "DirOut", "DirComb", "NameDirCombOut", "NameVarTime"),
    NameParaOptn = c("FileSchmComb",
                     "ColKeep",
                     "DirSubCopy",
                     "NameFileSufx"),
    log = log
  )


# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))
log$debug(
  base::paste0(
    'All files found in the following directories will be combined: ',
    base::paste0(Para$DirComb, collapse = ',')
  )
)
log$debug(
  base::paste0(
    'A single combined data file will be populated in the directory: ',
    Para$NameDirCombOut
  )
)
log$debug(base::paste0('Common time variable expected in all files: ', Para$NameVarTime))

# Read in the output schema
log$debug(base::paste0(
  'Output schema: ',
  base::paste0(Para$FileSchmComb, collapse = ',')
))
if (base::is.null(Para$FileSchmComb) || Para$FileSchmComb == 'NA') {
  SchmComb <- NULL
} else {
  SchmComb <-
    base::paste0(base::readLines(Para$FileSchmComb), collapse = '')
  
  # Parse the avro schema for output variable names
  nameVarSchmComb <- NEONprocIS.base::def.schm.avro.pars(Schm=SchmComb,log=log)$var$name
}

# Echo more arguments
log$debug(
  base::paste0(
    'Input columns (and their order) to populate in the combined output file (all if empty): ',
    base::paste0(Para$ColKeep, collapse = ',')
  )
)


# Retrieve optional subdirectories to copy over
DirSubCopy <-
  base::unique(base::setdiff(Para$DirSubCopy, Para$NameDirCombOut))
log$debug(base::paste0(
  'Additional subdirectories to copy: ',
  base::paste0(DirSubCopy, collapse = ',')
))

# What are the expected subdirectories of each input path
log$debug(base::paste0(
  'Minimum expected subdirectories of each datum path: ',
  base::paste0(Para$DirComb, collapse = ',')
))

# Find all the input paths (datums). We will process each one.
DirIn <-
  NEONprocIS.base::def.dir.in(DirBgn = Para$DirIn,
                              nameDirSub =  Para$DirComb,
                              log = log)


# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  log$info(base::paste0('Processing path to datum: ', idxDirIn))
  
  # Get directory listing of input director(ies). We will combine these files.
  file <-
    base::list.files(base::paste0(idxDirIn, '/', Para$DirComb))
  filePath <-
    base::list.files(base::paste0(idxDirIn, '/', Para$DirComb), full.names =
                       TRUE)
  
  # Gather info about the input directory (including date) and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  idxDirOut <- base::paste0(Para$DirOut, InfoDirIn$dirRepo)
  idxDirOutComb <- base::paste0(idxDirOut, '/', Para$NameDirCombOut)
  NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                DirSub = Para$NameDirCombOut,
                                log = log)
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn, '/', DirSubCopy),
                                       idxDirOut, log = log)
  }
  
  # Combine data files
  data <- NULL
  data <-
    NEONprocIS.base::def.file.comb.ts(file = filePath,
                                           nameVarTime = Para$NameVarTime,
                                           log = log)
  
  # Take stock of the combined data
  nameCol <- base::names(data)
  log$debug(base::paste0(
    'Columns found in the combined data files: ',
    base::paste0(nameCol, collapse = ',')
  ))
  
  # Filter and re-order the output columns
  if (!base::is.null(Para$ColKeep)) {
    # Check whether the desired columns to keep are found in the combined data
    chkCol <- Para$ColKeep %in% nameCol
    if (base::any(!chkCol)) {
      log$error(
        base::paste0(
          'Columns: ',
          base::paste0(nameCol[!chkCol], collapse = ','),
          'were not found in the input data. Check ColKeep input argument.'
        )
      )
      stop()
    }
    
    # Reorder and filter the output columns
    data <- data[Para$ColKeep]
    
    # Turn any periods in the column names to underscores
    base::names(data) <- base::sub(pattern='[.]',replacement='_',x=base::names(data))

    
    if(base::is.null(SchmComb)){
      log$debug(base::paste0(
        'Filtered and re-ordered output columns : ',
        base::paste0(base::names(data), collapse = ',')
      ))
    } else {
      log$debug(base::paste0(
        'Filtered and re-ordered input columns: ',
        base::paste0(Para$ColKeep, collapse = ','),
        ' will be respectively output with column names: ',
        base::paste0(nameVarSchmComb, collapse = ',')
      ))      
    }
    
  }
  
  # Write out the file. Take the shortest file name and tag on the suffix.
  fileBase <-
    file[base::nchar(file) == base::min(base::nchar(file))][1]
  fileOut <-
    NEONprocIS.base::def.file.name.out(nameFileIn = fileBase,
                                       sufx = Para$NameFileSufx,
                                       log = log)
  nameFileOut <- base::paste0(idxDirOutComb, '/', fileOut)
  
  rptWrte <-
    base::try(NEONprocIS.base::def.wrte.parq(
      data = data,
      NameFile = nameFileOut,
      NameFileSchm = NULL,
      Schm = SchmComb,
      log=log
    ),
    silent = TRUE)
  if (base::class(rptWrte) == 'try-error') {
    log$error(base::paste0(
      'Cannot write combined file ',
      nameFileOut,
      '. ',
      attr(rptWrte, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0('Combined data written successfully in file: ',
                          nameFileOut))
  }
  
  return()
} # End loop around datum paths
