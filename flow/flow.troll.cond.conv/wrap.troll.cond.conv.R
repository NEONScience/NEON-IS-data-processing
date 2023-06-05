##############################################################################################
#' @title Workflow for Missing Temp Flag and Conductivity Conversion

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Wrapper function. Flags conductivity for the Aqua Troll 200 when temperature stream is missing. 
#' Calculates specific conductance when temperature stream is available.
#'
#' @param DirIn Character value. The path to parent directory where the flags exist. 
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /aquatroll200/yyyy/mm/dd/SENSOR/data
#'         /aquatroll200/yyyy/mm/dd/SENSOR/flags
#'         /aquatroll200/yyyy/mm/dd/SENSOR/uncertainty_coef
#'         /aquatroll200/yyyy/mm/dd/SENSOR/uncertainty_data
#'
#' @param DirOut Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmData (Optional).  A json-formatted schema where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA. 
#' 
#' @param SchmQf (optional) A json-formatted character string containing the schema for the calibration flags output
#' by this function. If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). 
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is).

#' @param log (optional) A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return 
#' Corrected conductivity data and associated flags for missing temperature data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' SchmData <- "~/R/NEON-IS-avro-schemas/dp0p/aquatroll200_cond_corrected.avsc"
#' SchmQf <- "~/R/NEON-IS-avro-schemas/dp0p/flags_troll_specific_temp.avsc"
#' 

#' wrap.troll.cond.conv(DirIn="/pfs/tempSoil_pre_statistics_group/prt/2020/01/02/CFGLOC12345",
#'                DirOutBase="/pfs/out",
#'                SchmData=SchmData,
#'                SchmQf=SchmQf
#'                )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2023-03-03)
#     Convert flow script to wrapper function
##############################################################################################
wrap.troll.cond.conv <- function(DirIn,
                         DirOut,
                         SchmData,
                         SchmQf
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 

  
} # End function

  