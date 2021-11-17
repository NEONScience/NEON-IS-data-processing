##############################################################################################
#' @title Unit test for Basic QA/QC (plausibility) module for NEON IS data processing

#' @author
#' Mija Choi \email{choim@batelleEcology.org}
#'
#' @description  Wrapper function. Basic QA/QC (plausibility) module for NEON IS data processing. Includes tests for 
#' null, gap, range, step, spike, and persistence. See eddy4R.qaqc package functions for details on each test.
#' General code workflow:
#'      Error-check input parameters
#'      Read regularization frequency from location file if expected
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Loop through all data files
#'        Regularize data in each file
#'        Write out the regularized data
#'
#' @param DirIn Character value. The path to parent directory where the data and thresholds exist. 
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within the parent path must be the folders:
#'         /data 
#'         /threshold
#'         
#' The data folder holds any number of daily data files corresponding to the date in the input 
#' path and surrounding days. Names of data files MUST include the data date in the format %Y-%m-%d 
#' (YYYY-mm-dd). It does not matter where in the filename the date is denoted, so long as it is unambiguous.
#' 
#' The threshold folder holds a single file with QA/QC threshold information.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param ParaTest A named list for each variable/term that quality tests will be applied to. The list element 
#' for each term is another list with the following named vectors: \cr
#' \code{term} Character vector. The name of the term to be tested. Must be present in the input data.
#' \code{test} Character vector. The name of the quality tests to run. Options are: 'null','gap','range','step','spike','persistence'
#' \code{rmv} Logical vector of same length as \code{test}, indicating for each corresponding test whether test failures should 
#' result in data removal (NA values). 
#' 
#' @param SchmDataOut (Optional). A json-formatted character string containing the schema for the output data. May be NULL (default), 
#' in which case the schema will be created automatically from the output data frame with the same variable names 
#' as the input data frame.
#' 
#' @param SchmQf (Optional). A json-formatted character string containing the schema for the output quality flags. May be NULL (default), 
#' in which case the  the variable names will be a camelCase combination of the term, 
#' the test, and the characters "QF", in that order. For example, if the input arguments 5-6 are 
#' "TermTest1=temp:null|range(rmv)" and "TermTest1=resistance:spike|gap" and the argument VarAddFileQf is omitted,
#' the output columns will be readout_time, tempNullQF, tempRangeQF, resistanceSpikeQF, resistanceGapQF, in that order. 
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS. 
#' Otherwise, they will be labeled incorrectly.
#' 
#' @param VarAddFileQf (Optional). Character vector. The names of any variables in the input data file
#' that should be copied over to the output flags files. Do not include readout_time.
#' In normal circumstances there should be none, as output flags files should only contain a timestamp and flags, 
#' but in rare cases additional variables may desired to be included in the flags files (such as source ID, site, 
#' or additional variables in the input file that already act as a quality flag. Defaults to NULL. Note that these will be 
#' tacked on to the end of the output columns produced by the selections in TermTest, and any output schema 
#' should account for this.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log (optional) A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. The data and flags folders will include only the 
#' data/flags for the date indicated in the directory structure. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link.
#' 
#' The flags file will contain a column for the readout time followed by columns for quality flags grouped by 
#' variable/term in the same order as the variables/terms and the tests were provided in the input arguments 
#' (test nested within term), followed by additional variables, if any, specified in argument VarAddFileQf.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' ParaTest <- list(relativeHumidity=list(term='relativeHumidity',
#'                                        test=c("null","gap","range","step","spike","persistence"),
#'                                        rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
#'                                        ),
#'                  temperature=list(term='temperature',
#'                                   test=c("null","gap","range","step","spike","persistence"),
#'                                   rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
#'                                   ),
#'                  dewPoint=list(term='dewPoint',
#'                                test=c("null","gap","range","step","spike","persistence"),
#'                                rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
#'                                )
#'                  )
#' wrap.qaqc.plau(DirIn="~/pfs/relHumidity_padded_timeseries_analyzer/hmp155/2020/01/01/CFGLOC101252",
#'                DirOutBase="~/pfs/out",
#'                ParaTest=ParaTest,
#'                VarAddFileQf='errorState'
#' )
#' @seealso None currently
#' 
# changelog and author contributions / copyrights
#   Mija Choi (2021-11-10)
#     original creation
##############################################################################################
# Define test context
context("\n       | Unit test of Basic QA/QC (plausibility) module for NEON IS data processing \n")

test_that("Unit test of wrap.qaqc.plau.R", {
  source('../../flow.qaqc.plau/wrap.qaqc.plau.R')
  library(stringr)
  
  DirIn = "pfs/padded_timeseries_analyzer/hmp155/2020/01/02/CFGLOC101252"
  DirOutBase = "pfs/out"
  VarAddFileQf='errorState'
  ParaTest <- list(relativeHumidity=list(term='relativeHumidity',
                                         test=c("null","gap","range","step","spike","persistence"),
                                         rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
  ),
  temperature=list(term='temperature',
                   test=c("null","gap","range","step","spike","persistence"),
                   rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
  ),
  dewPoint=list(term='dewPoint',
                test=c("null","gap","range","step","spike","persistence"),
                rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
  )
  )
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  # Test 1 
  
  # remove the test output symbolic link
  DirSrc = 'CFGLOC101252'
  cmdLs <- base::paste0('ls ', base::paste0(DirSrc))
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  returned_wrap_qaqc_plau <- wrap.qaqc.plau(DirIn=DirIn,
                 DirOutBase=DirOutBase,
                 ParaTest=ParaTest,
                 VarAddFileQf=VarAddFileQf
  )
  
  
})

