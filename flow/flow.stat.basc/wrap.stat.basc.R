##############################################################################################
#' @title Basic statistics and uncertainty module for NEON IS data processing.

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

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
#'         Bin the data by time and compute all requested statistics in a single
#'         data.table grouped aggregation. Vectorize expanded-uncertainty computation
#'         across all bins for known FDAS/cal.cnst variants; fall back to a per-bin
#'         call for unknown uncertainty functions.
#'         Write the output file for the aggregation interval.
#'
#' @param DirIn Character value. The path to parent directory where the flags exist.
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of
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
#' @param SchmStat (Optional).  A json-formatted character string containing the schema for the output statistics
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

#' @examples
#' # Not run
#' ParaStat = list(
#'                 temp=list(
#'                           term="temp",
#'                           stat=c("mean","median","expUncert"),
#'                           funcUcrt="wrap.ucrt.dp01.cal.cnst.fdas.rstc"
#'                           ),
#'                 pres=list(
#'                           term="pres",
#'                           stat=c("minimum","maximum")
#'                           )
#'                 )
#' wrap.stat.basc(DirIn="/pfs/tempSoil_pre_statistics_group/prt/2020/01/02/CFGLOC12345",
#'                DirOutBase="/pfs/out",
#'                WndwAgr=as.difftime(c(1,30),units='mins'),
#'                ParaStat=ParaStat
#'                )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2022-06-16)
#     Convert flow script to wrapper function
#   Cove Sturtevant (2025-04-29)
#     Force sum of all-NA data to be NA (instead of 0)
#   Teresa Burlingame (2026-06-15)
#     Refactor inner per-bin loop to data.table grouped aggregation.
#     Vectorize expUncert for cal.cnst.fdas.rstc/volt and cal.cnst variants;
#     keep per-bin fallback for other funcUcrt values. Pass Dict=FALSE on
#     parquet write. Fix pre-existing class()==string check on write result.
##############################################################################################

# Internal helper: pre-extract scalar uncertainty coefficients for the full day so the
# per-bin lapply scans of the ucrtCoef list happen once per file instead of once per bin.
.ucrtCoefScalar <- function(ucrtCoef, var, fn, timeBgn, timeEnd) {
  # Returns a list with the scalars needed by the named funcUcrt, or NULL fields when missing.
  pickMax <- function(name) {
    mtch <- vapply(ucrtCoef, function(c) {
      isTRUE(c$term == var) && isTRUE(c$Name == name) &&
        !is.null(c$start_date) && !is.null(c$end_date) &&
        c$start_date < timeEnd && c$end_date > timeBgn
    }, logical(1))
    if (sum(mtch) == 0) return(NA_real_)
    max(as.numeric(unlist(lapply(ucrtCoef[mtch], function(c) c$Value))))
  }
  fdasType <- if (fn == "wrap.ucrt.dp01.cal.cnst.fdas.rstc") "R"
              else if (fn == "wrap.ucrt.dp01.cal.cnst.fdas.volt") "V"
              else NA_character_
  out <- list(A3 = pickMax("U_CVALA3"))
  if (!is.na(fdasType)) {
    out$Fdas3 <- pickMax(paste0("U_CVAL", fdasType, "3"))
    out$Fdas4 <- pickMax(paste0("U_CVAL", fdasType, "4"))
    out$TypeFdas <- fdasType
  }
  out
}

# Internal helper: vectorized fdas expUncert across all bins for one variable.
# Mirrors wrap.ucrt.dp01.cal.cnst.fdas.rstc/volt: combines se (natural variation),
# A3 (constant cal uncertainty), and FDAS uncertainty from max-ucrtComb row per bin.
# Returns a numeric vector indexed by bin index (1..length(timeSeq)), NA where no bin data.
.expUncertFdasVec <- function(dataDT, ucrtDataDT, var, coefs, timeBrk, statsLong) {
  nBin <- length(timeBrk) - 1L
  out <- rep(NA_real_, nBin)
  if (is.null(ucrtDataDT) || nrow(ucrtDataDT) == 0L) return(out)
  colRaw  <- paste0(var, "_raw")
  colDerv <- paste0(var, "_dervCal")
  colUcrt <- paste0(var, "_ucrtComb")
  needCols <- c("readout_time", colRaw, colDerv, colUcrt)
  if (!all(needCols %in% names(ucrtDataDT))) return(out)
  # Match the original masking: rows where the corresponding data value is NA contribute nothing.
  # The original aligns by row index within a bin; we align by readout_time across the full series.
  dataKeep <- dataDT[, .(readout_time, .dataVal = get(var))]
  uTmp <- merge(ucrtDataDT[, c("readout_time", ".bin", colRaw, colDerv, colUcrt), with = FALSE],
                dataKeep, by = "readout_time", all.x = TRUE)
  uTmp[is.na(.dataVal), (colUcrt) := NA_real_]
  uTmp <- uTmp[!is.na(.bin) & !is.na(get(colUcrt))]
  if (nrow(uTmp) == 0L) return(out)
  # For each bin, pick the row with max ucrtComb
  data.table::setorderv(uTmp, c(".bin", colUcrt), order = c(1L, -1L))
  uMax <- uTmp[, .SD[1L], by = .bin, .SDcols = c(colRaw, colDerv, colUcrt)]
  setnames(uMax, c(colRaw, colDerv, colUcrt), c(".raw", ".dervCal", ".ucrtComb"))
  # Pull per-bin stdEr from the already-computed statsLong for this var
  seDT <- statsLong[term == var, .(.bin, stdEr)]
  m <- merge(uMax, seDT, by = ".bin", all.x = TRUE)
  ucrtFdas <- if (is.na(coefs$Fdas3) || is.na(coefs$Fdas4)) {
    rep(NA_real_, nrow(m))
  } else {
    abs(m$.dervCal) * (coefs$Fdas3 * m$.raw + coefs$Fdas4)
  }
  # 2 * sqrt(se^2 + A3^2 + Fdas^2). NA in any component propagates to NA.
  expU <- 2 * sqrt(m$stdEr^2 + coefs$A3^2 + ucrtFdas^2)
  out[m$.bin] <- expU
  out
}

# Internal helper: vectorized cal.cnst expUncert across all bins for one variable.
# Mirrors wrap.ucrt.dp01.cal.cnst: 2 * sqrt(se^2 + A3^2). No ucrtData needed.
.expUncertCalCnstVec <- function(var, coefs, statsLong, nBin) {
  out <- rep(NA_real_, nBin)
  seDT <- statsLong[term == var, .(.bin, stdEr)]
  if (nrow(seDT) == 0L) return(out)
  expU <- 2 * sqrt(seDT$stdEr^2 + coefs$A3^2)
  out[seDT$.bin] <- expU
  out
}

# Internal helper: per-bin fallback for unknown funcUcrt variants. Calls the original
# NEONprocIS.stat::wrap.ucrt.dp01 once per bin with the bin's data slice.
.expUncertFallback <- function(dataDT, ucrtDataDT, FuncUcrt, var, ucrtCoef, nBin, log) {
  out <- rep(NA_real_, nBin)
  binsUsed <- unique(dataDT$.bin)
  binsUsed <- binsUsed[!is.na(binsUsed)]
  FuncUcrtVar <- FuncUcrt[FuncUcrt$var == var, , drop = FALSE]
  for (b in binsUsed) {
    dSlice <- as.data.frame(dataDT[.bin == b])
    uSlice <- if (!is.null(ucrtDataDT)) as.data.frame(ucrtDataDT[.bin == b]) else NULL
    res <- try(NEONprocIS.stat::wrap.ucrt.dp01(data = dSlice, FuncUcrt = FuncUcrtVar,
                                               ucrtCoef = ucrtCoef, ucrtData = uSlice,
                                               log = log), silent = TRUE)
    if (!inherits(res, "try-error") && !is.null(res[[var]])) out[b] <- res[[var]]
  }
  out
}

wrap.stat.basc <- function(DirIn,
                         DirOutBase,
                         WndwAgr,
                         ParaStat,
                         SchmStat=NULL,
                         DirSubCopy=NULL,
                         log=NULL
){

  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  }

  # Error check the chosen statistics
  stat <- base::unique(base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$stat})))
  StatAvail <- c("mean", "median", "minimum", "maximum", "sum", "variance", "stdDev", "stdEr", "numPts", "skewness", "kurtosis", "mad", "expUncert")
  chkStat <- stat %in% StatAvail
  if(base::sum(!chkStat) > 0){
    log$fatal(base::paste0('Statistic(s): ',
                           base::paste0(stat[!chkStat],collapse=","),
                           ' are unrecognized for computation by the basic stats module. Acceptable statistic choices are ',
                           base::paste0(StatAvail,collapse=",")))
    stop()
  }

  # Pull out all terms and stats we will compute
  termComp <- base::unlist(base::lapply(ParaStat,FUN=function(idx){idx$term}))

  # Compile the full set of output statistics, in the order in which they will be output (for initializing the output)
  nameColOut <- base::unlist(base::lapply(
    ParaStat,FUN=function(idx){
      base::paste0(idx$term,
                  base::paste(
                    base::toupper(
                      base::substr(idx$stat,1,1)),
                    base::substr(idx$stat,
                                 2,
                                 base::nchar(idx$stat)),
                    sep=""
                    )
      )
      }
    )
  )

  # Put the uncertainty functions into a data frame
  FuncUcrt <- base::do.call(base::rbind,
                            base::lapply(ParaStat,
                                         FUN=function(idx){
                                           if(!base::is.null(idx$funcUcrt)){
                                             return(base::data.frame(var=idx$term,
                                                                     FuncUcrt=idx$funcUcrt,
                                                                     stringsAsFactors=FALSE))
                                           }else{
                                             return(NULL)
                                           }
                                         }
                            )
  )

  # Create the binning for each aggregation interval
  timeBgnDiff <- list()
  timeEndDiff <- list()
  for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
    timeBinDiff <- NEONprocIS.base::def.time.bin.diff(WndwBin=WndwAgr[idxWndwAgr],
                                                      WndwTime=base::as.difftime(1,units='days'),
                                                      log=log)

    timeBgnDiff[[idxWndwAgr]] <- timeBinDiff$timeBgnDiff # Add to timeBgn of each day to represent the starting time sequence
    timeEndDiff[[idxWndwAgr]] <- timeBinDiff$timeEndDiff # Add to timeBgn of each day to represent the end time sequence
  } # End loop around aggregation intervals

  # Get directory listing of input directory. Expect subdirectories for data
  dirData <- base::paste0(DirIn,'/data')
  fileData <- base::sort(base::dir(dirData))
  log$info(base::paste0('Computing statistics for ', base::length(fileData),' input files (separately).'))

  # Gather info about the input directory (including date) and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  dirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  dirOutStat <- base::paste0(dirOut,'/stats')
  base::dir.create(dirOutStat,recursive=TRUE)

  # Copy with a symbolic link the desired subfolders
  if(base::length(DirSubCopy) > 0){
    if('stats' %in% DirSubCopy){
      LnkSubObj <- TRUE
    } else {
      LnkSubObj <- FALSE
    }
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,'/',DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=LnkSubObj,
                                       log=log)
  }

  # Are we computing uncertainty? If so, load the uncertainty coefficients file (there should be only 1)
  ucrtData <- NULL
  ucrtCoef <- base::list()
  if("expUncert" %in% stat){
    dirUcrtCoef <- base::paste0(DirIn,'/uncertainty_coef')

    fileUcrt <- base::dir(dirUcrtCoef)
    if(base::length(fileUcrt) != 1){
      log$warn(base::paste0("There are either zero or more than one uncertainty coefficient files in path: ",dirUcrtCoef,"... Uncertainty coefs will not be read in. This is fine if the uncertainty function doesn't need it, but you should check..."))
      ucrtCoef <- base::list()
    } else {
      nameFileUcrt <- base::paste0(dirUcrtCoef,'/',fileUcrt) # Full path to file

      # Open the uncertainty file
      ucrtCoef  <- base::try(rjson::fromJSON(file=nameFileUcrt,simplify=TRUE),silent=FALSE)
      if(base::class(ucrtCoef) == 'try-error'){
        # Generate error and stop execution
        log$error(base::paste0('File: ', nameFileUcrt, ' is unreadable.'))
        stop()
      }
      # Turn times to POSIX
      ucrtCoef <- base::lapply(ucrtCoef,FUN=function(idxUcrt){
        idxUcrt$start_date <- base::strptime(idxUcrt$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        idxUcrt$end_date <- base::strptime(idxUcrt$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        return(idxUcrt)
      })
      log$debug(base::paste0('Successfully read uncertainty coefficients from file: ',nameFileUcrt))
    }

    # Is there a folder for uncertainty data? If so, read it in.
    nameVarDataUcrt <- NULL
    if('uncertainty_data' %in% base::dir(DirIn)){
      dirUcrtData <- base::paste0(DirIn,'/uncertainty_data')
      log$info(base::paste0('Detected uncertainty data folder',dirUcrtData,'.'))

      fileUcrtData <- base::dir(dirUcrtData)
      if(base::length(fileUcrtData) != 1){
        log$warn(base::paste0("There are either zero or more than one uncertainty data files in path: ",dirUcrtCoef,"... Uncertainty data will not be read in. This is fine if the uncertainty function doesn't need it, but you should check..."))
      } else {
        nameFileUcrtData <- base::paste0(dirUcrtData,'/',fileUcrtData) # Full path to file

        # Open the uncertainty data file
        ucrtData  <- base::try(NEONprocIS.base::def.read.parq(NameFile=nameFileUcrtData,log=log),silent=FALSE)
        if(base::any(base::class(ucrtData) == 'try-error')){
          log$error(base::paste0('File ', fileUcrtData,' is unreadable.'))
          stop()
        } else {
          log$debug(base::paste0('Successfully read uncertainty data in file: ',fileUcrtData))
        }

      }
    } else {
      log$debug(base::paste0("No L0' uncertainty data detected in : ",DirIn))
    }# End if statement around uncertainty data
  } # End if statement around expUncert

  # Pre-compute which optional stats are requested (invariant across files)
  needSkew <- "skewness" %in% stat
  needKurt <- "kurtosis" %in% stat
  needMedn <- "median"   %in% stat
  needMad  <- "mad"      %in% stat

  # Pre-extract scalar uncertainty coefficients per (var, funcUcrt). The lapply scan
  # over ucrtCoef is O(20) per coefficient; running it 1488 times per bin was the
  # dominant cost in the original code. Constant coefficients for a day - extract once.
  ucrtCoefByVar <- list()
  if ("expUncert" %in% stat && !is.null(FuncUcrt) && nrow(FuncUcrt) > 0) {
    for (k in seq_len(nrow(FuncUcrt))) {
      var <- FuncUcrt$var[k]
      fn  <- FuncUcrt$FuncUcrt[k]
      ucrtCoefByVar[[var]] <- .ucrtCoefScalar(ucrtCoef, var, fn, timeBgn, timeEnd)
    }
  }

  # Run through each data file
  for(idxFileData in fileData){

    # Load in data file in parquet format into data frame 'data'.
    fileIn <- base::paste0(dirData,'/',idxFileData)
    data  <- base::try(NEONprocIS.base::def.read.parq(NameFile=fileIn,log=log),silent=FALSE)
    if(base::any(base::class(data) == 'try-error')){
      log$error(base::paste0('File ', fileIn,' is unreadable.'))
      stop()
    } else {
      log$debug(base::paste0('Successfully read in data file: ',fileIn))
    }

    # Validate the data
    valiData <-
      NEONprocIS.base::def.validate.dataframe(dfIn = data,
                                              TestNameCol = base::unique(c(
                                                'readout_time', termComp
                                              )),
                                              log = log)
    if (!valiData) {
      base::stop()
    }

    # Switch to data.table semantics for the compute kernel. Calls below mutate
    # `data` by reference; cleanup happens at the end of each WndwAgr iteration.
    data.table::setDT(data)
    if (!is.null(ucrtData)) data.table::setDT(ucrtData)

    # Run through each aggregation interval
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){

      log$debug(base::paste0('Computing stats for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))

      # Create start and end time sequences
      timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
      timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
      timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
      nBin <- length(timeAgrBgn)

      # Initialize the output (wide form, matches original schema/order)
      rpt <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
      rpt[,3:(base::length(nameColOut)+2)] <- base::as.numeric(NA)
      base::names(rpt)[3:(base::length(nameColOut)+2)] <- nameColOut

      # Allocate data points to aggregation windows (as a column on the data.table)
      data[, .bin := base::.bincode(base::as.numeric(readout_time), timeBrk,
                                    right=FALSE, include.lowest=FALSE)]
      if (!is.null(ucrtData)) {
        ucrtData[, .bin := base::.bincode(base::as.numeric(readout_time), timeBrk,
                                          right=FALSE, include.lowest=FALSE)]
      }

      # Build long-form (bin, term, val) view for grouped aggregation over all terms at once.
      termCols <- base::intersect(termComp, base::names(data))
      dataLong <- data.table::melt(
        data[!is.na(.bin), c(".bin", termCols), with = FALSE],
        id.vars = ".bin", measure.vars = termCols,
        variable.name = "term", value.name = "val",
        variable.factor = FALSE
      )

      # Single-pass grouped aggregation. R's stats::var (and dispatched data.table
      # group var) is the same shifted two-pass algorithm as the original code -
      # bit-identical results for sufficiently-populated bins.
      statsLong <- dataLong[, {
        n   <- base::sum(!is.na(val))
        mn  <- if (n > 0L) base::mean(val, na.rm = TRUE) else NA_real_
        vr  <- if (n > 1L) stats::var(val, na.rm = TRUE) else NA_real_
        sdv <- base::sqrt(vr)
        se  <- sdv / base::sqrt(n)
        mnv <- if (n > 0L) base::suppressWarnings(base::min(val, na.rm = TRUE)) else NA_real_
        mxv <- if (n > 0L) base::suppressWarnings(base::max(val, na.rm = TRUE)) else NA_real_
        smv <- if (n == 0L) NA_real_ else base::sum(val, na.rm = TRUE)
        mdv <- if (needMedn && n > 0L) stats::median(val, na.rm = TRUE) else NA_real_
        madv <- if (needMad  && n > 0L) stats::mad(val, constant = 1, na.rm = TRUE) else NA_real_
        skv <- if (needSkew && n > 0L && !is.na(sdv) && sdv > 0) {
          base::sum((val - mn)^3, na.rm = TRUE) / sdv^3 / n
        } else NA_real_
        kuv <- if (needKurt && n > 0L && !is.na(sdv) && sdv > 0) {
          base::sum((val - mn)^4, na.rm = TRUE) / sdv^4 / n
        } else NA_real_
        .(numPts = as.integer(n), mean = mn, median = mdv,
          minimum = mnv, maximum = mxv, sum = smv,
          variance = vr, stdDev = sdv, stdEr = se,
          skewness = skv, kurtosis = kuv, mad = madv)
      }, by = .(.bin, term)]
      data.table::setkey(statsLong, .bin, term)

      # Inf/-Inf from all-NA min/max with na.rm: replace with NA (preserves orig behavior)
      statsLong[is.infinite(minimum), minimum := NA_real_]
      statsLong[is.infinite(maximum), maximum := NA_real_]

      # Compute expUncert per variable (vectorized when funcUcrt is known)
      expUncertByVar <- list()
      if ("expUncert" %in% stat && !is.null(FuncUcrt)) {
        for (k in base::seq_len(base::nrow(FuncUcrt))) {
          var <- FuncUcrt$var[k]
          fn  <- FuncUcrt$FuncUcrt[k]
          coefs <- ucrtCoefByVar[[var]]
          if (fn %in% c("wrap.ucrt.dp01.cal.cnst.fdas.rstc",
                        "wrap.ucrt.dp01.cal.cnst.fdas.volt")) {
            expUncertByVar[[var]] <- .expUncertFdasVec(data, ucrtData, var, coefs, timeBrk, statsLong)
          } else if (fn == "wrap.ucrt.dp01.cal.cnst") {
            expUncertByVar[[var]] <- .expUncertCalCnstVec(var, coefs, statsLong, nBin)
          } else {
            log$debug(base::paste0("expUncert funcUcrt '", fn,
                                   "' not vectorized; using per-bin fallback for var '", var, "'."))
            expUncertByVar[[var]] <- .expUncertFallback(data, ucrtData, FuncUcrt, var,
                                                       ucrtCoef, nBin, log)
          }
        }
      }

      # Fill rpt from statsLong + expUncertByVar, preserving the ParaStat ordering
      for (paraIdx in base::seq_along(ParaStat)) {
        termNm <- ParaStat[[paraIdx]]$term
        rowsT <- statsLong[term == termNm]
        for (s in ParaStat[[paraIdx]]$stat) {
          colName <- base::paste0(termNm, base::paste0(base::toupper(base::substr(s,1,1)),
                                                       base::substr(s,2,base::nchar(s))))
          if (s == "expUncert") {
            vals <- expUncertByVar[[termNm]]
            if (!is.null(vals)) rpt[, colName] <- vals
          } else if (s == "numPts") {
            rpt[rowsT$.bin, colName] <- as.integer(rowsT[[s]])
          } else {
            rpt[rowsT$.bin, colName] <- rowsT[[s]]
          }
        }
      }

      # Belt-and-suspenders: ensure no Inf leaks through (matches original lines 425-426)
      rpt[rpt==Inf] <- NA
      rpt[rpt==-Inf] <- NA


      # Write out the file for this aggregation interval
      tmi <- base::gsub(
        pattern=' ',
        replacement='0',
        x=base::format(
          base::as.character(WndwAgr[idxWndwAgr]),
          width=3,
          justify='right'))
      NameFileOutStat <-
        NEONprocIS.base::def.file.name.out(nameFileIn = idxFileData,
                                           prfx = base::paste0(dirOutStat, '/'),
                                           sufx = base::paste0('_basicStats_',tmi)
        )
      rptWrte <- base::try(NEONprocIS.base::def.wrte.parq(data=rpt,
                                                          NameFile=NameFileOutStat,
                                                          NameFileSchm=NULL,
                                                          Schm=SchmStat,
                                                          Dict=FALSE),
                           silent=TRUE)
      if(base::inherits(rptWrte, 'try-error')){
        log$error(base::paste0('Cannot write basic statistics file ', NameFileOutStat,'. ',attr(rptWrte,"condition")))
        stop()
      } else {
        log$info(base::paste0('Basic statistics written successfully in file: ',NameFileOutStat))
      }

      # Cleanup: drop the temporary .bin column so setDT-by-reference doesn't
      # leak it into the next aggregation window (or the next file via foreach).
      data[, .bin := NULL]
      if (!is.null(ucrtData)) ucrtData[, .bin := NULL]

    } # End loop around aggregation intervals

  } # End loop around files

  return()

} # End function


