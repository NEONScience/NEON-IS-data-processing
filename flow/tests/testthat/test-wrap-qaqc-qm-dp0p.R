##############################################################################################
#' @title Unit test for Instantaneous quality metrics module for NEON IS data processing.

#' @author
#' Mija Choi \email{choim@batelleEcology.org}
#'
#' @description Wrapper function. Aggregates quality flags to produce alpha, beta, and final quality flags
#' for each L0' (instantaneous) record. The alpha flag is 1 when any of a set of selected
#' flags have a value of 1 (fail). The beta flag is 1 when any of a set of selected
#' flags cannot be evaluated (have a value of -1). If either the alpha flag or beta flag are raised,
#' the final quality is raised (value of 1). Multiple sets of alpha, beta, and final quality flag
#' may be created, as indicated in the input arguments.
#' General code workflow:
#'      Error-check input parameters
#'      Read in and combine flags files
#'      For each aggregation interval...
#'         Create output directories and copy (by symbolic link) unmodified components
#'         Read in and combine all the flags files in the flags directory of each input datum
#'         Force any flags to particular values based on values of other flags (as specified in input arguments)
#'         Create alpha, beta, and final quality flags for each group as specified in the input arguments
#'         Write out the entire flag set, including all input flags and the alpha, beta, and final QFs
#'
#' @param DirIn Character value. The path to parent directory where the flags exist.
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#'
#' Nested within this path are (at a minimum) the folder:
#'         /flags
#'
#' The flags folder holds any number of daily files holding quality flags. All files will be combined. Ensure
#' there are no overlapping column names between the files other than "readout_time", otherwise only one of the
#' columns will be retained. Note that the "readout_time" variable must exist in all files. Any non-matching
#' timestamps among files will result in NA values for columns that do not have this timestamp.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' @param ParaGrp (optional) A named list of lists, each sublist representing the specifications for computing a group of
#' alpha QF, beta QF, and final QF. List names at the top level are used as a prefix to the output alpha, beta, and final QFs.
#' If no group name/prefix is desired, use a zero-character string (i.e. ""). Each sublist has 3 named elements:\cr
#' \code{qfAlph}: Character vector. The exact names of the quality flags that should be used in the computation of the alpha
#' QF for the group. Note that a flag may be specified in multiple groups (i.e. will feed into multiple final quality flags).\cr
#' \code{qfBeta}: Character vector. The exact names of the quality flags that should be used in the computation of the beta
#' QF for the group. \cr
#' \code{qfBetaIgnr}: Optional (may be omitted). Character vector. The exact names of the quality flags that, if any of their values equals 1,
#' the beta QF flag for the indicated group is set to 0. \cr
#' #' Note that a flag may be specified in multiple groups (i.e. will feed into multiple final quality flags). Note that the original
#' quality flags found in the input files will be output. The ParaGrp argument simply dictates what QFs feed into each set of
#' alpha/beta/final quality flags. If this argument is NULL (default), all quality flags in the file(s) will be used to compute
#' a single set of alpha, beta, and final quality flags.
#'
#' @param ParaForc (optional) A list of lists, each sublist representing the specifications for forcing one or more quality flags
#' (excluding alpha, beta, and final QFs) to a value if the value of a "driver" flag equals a particular value.
#' Each sublist has 3 named elements:\cr
#' \code{qfDrve} Character value. The name of the driver flag \cr
#' \code{valuDrve} Numeric value. The "driver value" of the driver flag which activates the force \cr
#' \code{qfForc} Character value. The name(s) of the "forced" flag(s) \cr
#' \code{valuForc} Numeric value. The "forced" value \cr
#' For example, if tempRangeQF and tempPersistenceQF should be set to -1 when sensorNAQF is 1, then the argument is
#' ParaForc <- list(list(qfDrve="sensorNAQF",valuDrve=1,qfForc=c("tempRangeQF","tempPersistenceQF"),valuForc=-1)).\cr
#' Note that the logic described here occurs before the alpha, beta, and final quality flags are calculated, and
#' forcing actions will occur in the order found in this argument.
#'
#' @param VarTimeBgn (Optinoal). The name of the variable in the input files that signifies the
#' start time of the instantaneous measurement. Defaults to 'readout_time'. This variable be reported in the output as
#' startDateTime and will not be treated as a flag. If it is not found in any of the input files, startDateTime will be
#' populated with the values in readout_time.
#'
#' @param VarTimeEnd (Optinoal). The name of the variable in the input files that signifies the
#' end time of the instantaneous measurement. Defaults to 'readout_time'. This variable be reported in the output as
#' endDateTime and will not be treated as a flag. If it is not found in any of the input files, endDateTime will be
#' populated with the values in readout_time.
#'
#' @param Tmi (Optional). Character value. A 3-character index specifying the NEON timing index to include
#' in the output file name. Default is "000".
#'
#' @param SchmQm (Optional).  A json-formatted character string containing the schema for the output quality
#' metrics file. If this input is not provided, the output schema for the will be auto-generated from
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE FLAGS
#' IN THE SORTED FILE LIST. See output information below for details.
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the
#' output path (i.e. not combined but carried through as-is).

#' @param log (optional) A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Resultant quality flags, including the alpha, beta, and final quality flags output in DirOutBase, where the
#' terminal directory of DirOutBase replaces BASE_REPO but otherwise retains the child directory structure of the input
#' path. Directory 'quality_metrics' will automatically populated in the output directory with the output file. Any
#' other folders specified in argument DirSubCopy will be copied over unmodified with a symbolic link.
#'
#' If no output schema is provided for the quality flags, the variable names for the alpha, beta, and final QFs will
#' be a combination of the group name and AlphaQF, BetaQF, or FinalQF. The order of the outputs will be all quality flags in
#' the same order they were found in the sorted (increasing order) input file list, followed by the alpha QF, betaQF, and
#' finalQF for each group in the same order as the ParaGrp argument. Additionally, the first two columns of the output file
#' will contain the start and end times for the measurement, labeled "startDateTime" and "endDateTime",
#' respectively, and populated with the variables indicated in input arguments VarTimeBgn and VarTimeEnd.
#' Example column ordering: Say there are two input files named outflagsA.parquet and outflagsB.parquet, where
#' outflagsA.parquet contains flag tempValidCalQF, RHValidCalQF and outflagsB.parquet contains flags tempRangeQF, RHRangeQF
#' and the grouping arguments are "GrpQfAlpha1=temp:tempRangeQf|tempValidCalQF", "GrpQfAlpha2=RH:RHRangeQf|RHValidCalQF",
#' "GrpQfBeta1=temp:tempRangeQf", "GrpQfBeta2=RH:tempRangeQf".
#' The ordering of the output columns will be startDateTime, endDateTime, tempValidCalQF, RHValidCalQF, tempValidCalQF,
#' RHValidCalQF, tempAlphaQF, tempBetaQF, tempFinalQF, RHAlphaQF, RHBetaQF, RHFinalQF. The names (but not the order) of the
#' output columns may be changed by providing an output schema in argument FileSchmQm. However, ENSURE THAT ANY PROVIDED
#' OUTPUT SCHEMA MATCHES THIS COLUMN ORDERING. Otherwise, column names will not pertain to the flag in the column. Best practice
#' is to first run the code without the schema argument so that the default column naming is output. Then craft the schema
#' based on the known column ordering.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON Algorithm Theoretical Basis Document: Quality Flags and Quality Metrics for TIS Data Products (NEON.DOC.001113) \cr
#' Smith, D.E., Metzger, S., and Taylor, J.R.: A transparent and transferable framework for tracking quality information in
#' large datasets. PLoS ONE, 9(11), e112249.doi:10.1371/journal.pone.0112249, 2014. \cr

#' @keywords Currently none
#'
#' @seealso None currently
#'
# changelog and author contributions / copyrights
#   Mija Choi (2021-12-06)
#     original creation
##############################################################################################
# Define test context
context("\n       | Unit test of Basic QA/QC (plausibility) module for NEON IS data processing \n")

test_that("Unit test of wrap.qaqc.qm.dp0p.R", {
  source('../../flow.qaqc.qm.dp0p/wrap.qaqc.qm.dp0p.R')
  library(stringr)
  
  DirIn = "pfs/relHumidity_flags/hmp155/2020/01/01/CFGLOC101252"
  DirOutBase = "pfs/out"
  ParaGrp <- list(
    relativeHumidity = list(
      name = 'relativeHumidity',
      qf = c(
        "relativeHumidityNullQF",
        "relativeHumidityGapQF",
        "relativeHumidityRangeQF",
        "relativeHumidityStepQF",
        "relativeHumiditySpikeQF",
        "relativeHumidityPersistenceQF",
        "errorStateQF"
      )
    ),
    temperature = list(
      name = 'temperature',
      qf = c(
        "temperatureNullQF",
        "temperatureGapQF",
        "temperatureRangeQF",
        "temperatureStepQF",
        "temperatureSpikeQF",
        "temperaturePersistenceQF",
        "errorStateQF"
      )
    ),
    dewPoint = list(
      name = 'dewPoint',
      qf = c(
        "dewPointNullQF",
        "dewPointGapQF",
        "dewPointRangeQF",
        "dewPointStepQF",
        "dewPointSpikeQF",
        "dewPointPersistenceQF",
        "errorStateQF"
      )
    )
  )
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirSubCopy = "dirSub"
  
  # Test 1 Happy path
  returned_qm_dp0p <- wrap.qaqc.qm.dp0p(DirIn = DirIn,
                                        DirOutBase = DirOutBase,
                                        DirSubCopy=DirSubCopy,
                                        ParaGrp = ParaGrp)
  
  DirSrc = 'CFGLOC101252'
  cmdLs <- base::paste0('ls ', base::paste0(DirSrc))
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  
  # Test 2 with non zero values
  naDirIn = "pfs/relHumidity_flags/hmp155_NA/2020/01/01/CFGLOC101252"
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  returned_qm_dp0p <- wrap.qaqc.qm.dp0p(DirIn = naDirIn,
                                        DirOutBase = DirOutBase,
                                        ParaGrp = ParaGrp)
  
})
