##############################################################################################
#' @title Unit test of basic statistics and uncertainty module for NEON IS data processing.

#' @author
#' Mija Choi \email{choim@batelleEcology.org}
#'
#' @description Wrapper function. Basic statistics module for NEON IS data processing. Computes one or more
#' of the following statistics: mean, median, minimum, maximum, sum, variance, standard
#' deviation, standard error, number of points, skewness, kurtosis, median absolute deviation (mad),
#' expanded uncertainty
#'
#' General code workflow:
#'      Error-check input parameters
#'      Create output time sequence for each aggregation interval
#'      Read in and uncertainty coefficients and uncertainty data, if applicable
#'      For each aggregation interval:
#'         For each time bin of each aggregation interval:
#'            Compute the desired statistics for each term
#'         Write the output file for the aggregation inteval
#'
#' @param DirIn Character value. The path to parent directory where the flags exist.
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where indicates any number of
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#'
#' Nested within this path are (at a minimum) the folder:
#'         /data
#'
#' The data folder holds any number of daily data files for which statistics will be computed. If expUncert
#' is output (see options in ParaStat), information in folders 'uncertainty_coef' and/or 'uncertainty_data'
#' will be passed into the specified uncertainty function. Note that there can only be one file in the
#' uncertainty_data directory, so it must contain all applicable uncertainty data.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' @param WndwAgr Difftime vector. The aggregation interval(s) for which to compute statistics.
#' Note that a separate file will be output for each aggregation interval.
#' It is assumed that the expected length of the input data is one day.
#' The aggregation interval must be an equal divisor of one day.
#'
#' @param ParaStat A list of lists, one sublist for each term for which to compute statistics. Each sublist contains
#' the following named elements:
#' \code{term}: Character value. The term for which to compute statistics. \cr
#' \code{stat}: Character vector. The exact names of the statistics to compute for the term. Statistic options are
#' (exact names):
#' mean, median, minimum, maximum, sum, variance, stdDev, stdEr, numPts, expUncert, skewness, kurtosis, mad
#' \code{funcUcrt}: Character value (optional). The name of the function in the NEONprocIS.stat package
#' to compute the expanded uncertainty statistic (expUncert). Look in the NEONprocIS.stat package for
#' available functions to use, or create your own so long as it accepts the same inputs and outputs data
#' in the same format. Note that any uncertainty coefficients and/or  L0' uncertainty data in the uncertainty_coef
#' and uncertainty_data folders, respectively, of DirIn will be passed into the uncertainty function for use there.
#' If expUncert is not in the stat list for the term, this list element may be omitted.
#'
#' @param SchmQm (Optional).  A json-formatted character string containing the schema for the output statistics
#' file. If this input is not provided, the output schema for the will be auto-generated from
#' the output data frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE ORDER OF THE TERMS
#' IN THE ParaStat ARGUMENT (stats nested within term). See output information below for details.
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at
#' the same level as the data folder in the input path that are to be copied with a symbolic link to the
#' output path (i.e. carried through as-is).

#' @param log (optional) A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return
#' Statistics for each aggregation interval output in Parquet format in DirOutBase, where the terminal
#' directory of DirOutBase replaces BASE_REPO but otherwise retains the child directory structure of the input
#' path. Directory 'stats' will automatically populated in the output directory, where the files
#' for each aggregation interval will be placed. Any other folders specified in argument DirSubCopy will be
#' copied over unmodified with a symbolic link.
#' If no output schema is provided for the statistics, the variable names will be a combination of
#' the term and statistic, in the order they were provided in ParaStat. Additionally, the first two columns
#' of the stats file will contain the start and end times for the aggregation interval, labeled
#' "startDateTime" and "endDateTime", respectively.
#' The statistics are calculated for readout times in the interval [startDateTime endDateTime), with an open
#' brack on the right (i.e. inclusive of the startDateTime but exclusive of the endDateTime). The remaining
#' columns present the chosen statistics for each term. For example, if
#' ParaStat = list(temp=list(term="temp",stat=c("mean","median")),pres=list(term="pres",stat=c("minimum","maximum"))),
#' the output columns will be startDateTime, endDateTime, tempMean, tempMedian,presMinimum, presMaximum, in that order.
#' The names of the output columns may be replaced by providing an output
#' schema in argument FileSchmStat. However, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE STATS MATCHES THE
#' ORDERING OF THE INPUT ARGUMENTS. Otherwise, column names will not pertain to the statistics in the column.
#' The best way to ensure that the output schema matches the expected column ordering is to do a trial run
#' without inputting an output schema. The default output column names will be used, which can then serve as a
#' guide for crafting the output schema.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @seealso None currently
#'
#'changelog and author contributions / copyrights
#'  Mija Choi (2022-06-22)
#'    original creation
##############################################################################################
#'Define test context
context("\n       | Unit test of basic statistics and uncertainty module for NEON IS data processing \n")

test_that("Unit test of wrap.stat.basc.R", {
  source('../../flow.stat.basc/wrap.stat.basc.R')
  library(stringr)
  
  ParaStat = list(
      temp = list(
        term = "temp",
        stat = c("mean", "median", "expUncert"),
        funcUcrt = "wrap.ucrt.dp01.cal.cnst.fdas.rstc"
        ),
      pressure = list(
        term = "temp",
        stat = c("minimum", "maximum")
        ))
  
  ParaStat_1 = list(
    temp = list(
      term = "temp",
      stat = c("mean", "median", "expUncert"),
      funcUcrt = "wrap.ucrt.dp01.cal.cnst.fdas.rstc"
    ))
  
  ParaStat_skewness = list(
    temp = list(
      term = "temp",
      stat = c("mean", "median", "expUncert", "skewness"),
      funcUcrt = "wrap.ucrt.dp01.cal.cnst.fdas.rstc"
      ))
  
  ParaStat_kurtosis = list(
    temp = list(
      term = "temp",
      stat = c("mean", "median", "expUncert", "kurtosis"),
      funcUcrt = "wrap.ucrt.dp01.cal.cnst.fdas.rstc"
    ))
  
  DirIn = "pfs/tempSoil_pre_statistics_group/2020/01/02/CFGLOC101777"
  DirOutBase = "pfs/out"
  
  #1 Test 1
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  wrap.stat.basc(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat
  )
  
  #2 Test 2, stat has skewness
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  wrap.stat.basc(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat_skewness
  )
  
  #3 Test 3, stat has kurtosis
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  wrap.stat.basc(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat_kurtosis
  )
  
  #4 Test 4, There a folder for uncertainty data? zero uncertainty data files in path
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirIn_noData = "pfs/tempSoil_pre_statistics_group_noUncert/2020/01/02/CFGLOC101777"
  try(wrap.stat.basc(
    DirIn = DirIn_noData,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat
  ),silent = TRUE)
  
  #5 Test 5, There a folder for uncertainty coef? zero uncertainty coef files in path
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirIn_noData = "pfs/tempSoil_pre_statistics_group_noUcrtCoef/2020/01/02/CFGLOC101777"
  try(wrap.stat.basc(
    DirIn = DirIn_noData,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat
  ),silent = TRUE)
  #6 Test 6, data unreadable
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirIn_badData = "pfs/tempSoil_pre_statistics_group_wrongData/2020/01/02/CFGLOC101777"
  try(wrap.stat.basc(
    DirIn = DirIn_badData,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat
  ),silent = TRUE)
  
  #7 Test 7, no uncertainty_data directory
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirIn_badData = "pfs/tempSoil_pre_statistics_group_noUncrtDir/2020/01/02/CFGLOC101777"
  try(wrap.stat.basc(
    DirIn = DirIn_badData,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat
  ),silent = TRUE)
  
  #8 Test 8, Open the uncertainty coef file, unreadable
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirIn_badData = "pfs/tempSoil_pre_statistics_group_wrongUcrtCoef/2020/01/02/CFGLOC101777"
  try(wrap.stat.basc(
    DirIn = DirIn_badData,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat
  ),silent = TRUE)
  #9 Test 9, Open the uncertainty data file, unreadable
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirIn_badData = "pfs/tempSoil_pre_statistics_group_wrongUcrtData/2020/01/02/CFGLOC101777"
  try(wrap.stat.basc(
    DirIn = DirIn_badData,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat
  ),silent = TRUE)
  
  #10 Test 10, the stats has unexpected one
  
  stat_incorrect = c("mean", "median", "Uncert")
  ParaStat_incorrect = ParaStat
  ParaStat_incorrect$temp$stat = stat_incorrect
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  try(wrap.stat.basc(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat_incorrect
  ),silent = TRUE)
  
  #11 Test 11, the subfolder is stats
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirSubCopy = 'stats'
  try(wrap.stat.basc(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat,
    DirSubCopy = DirSubCopy
  ),silent = TRUE)
  
  #12 Test 12, the subfolder is nonStats
  
  if (dir.exists(DirOutBase)) {unlink(DirOutBase, recursive = TRUE)}
  
  DirSubCopy = 'nonSstats'
  try(wrap.stat.basc(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    WndwAgr = as.difftime(c(1, 30), units = 'mins'),
    ParaStat = ParaStat,
    DirSubCopy = DirSubCopy
  ),silent = TRUE)
  
})
