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
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

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


source("./data_comb.R")
calibration_conversion(DirIn = Para$DirIn,,
                       DirOut = Para$DirOut,
                       DirComb = Para$DirComb,
                       NameDirCombOut = Para$NameDirCombOut,
                       NameVarTime = Para$NameVarTime,
                       FileSchmComb = Para$FileSchmComb,
                       ColKeep = Para$ColKeep,
                       DirSubCopy = Para$DirSubCopy,
                       NameFileSufx = Para$NameFileSufx)
