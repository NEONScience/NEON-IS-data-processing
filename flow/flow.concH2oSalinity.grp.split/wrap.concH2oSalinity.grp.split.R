##############################################################################################
#' @title  Select and rename depth-specific columns from EnviroSCAN soil salinity group split data
#' (wrap.concH2oSalinity.grp.split)

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description For a single datum (CFGLOC source location directory produced by group_path_split),
#' reads the sibling group JSON to determine HOR and VER for this location. The VER encodes the
#' depth position (e.g., VER "503" = depth 3 of 8). The function then reads each stats and
#' quality_metrics parquet file, selects only the columns pertaining to that depth
#' (e.g., columns containing "Depth03"), renames them to drop the depth indicator
#' (e.g., VSICDepth03Mean -> VSICMean), and writes the output. Columns representing the physical
#' depth measurement itself (depth##SoilMoisture) are excluded from output.
#'
#' @param DirIn Character value. The input path to a single source location directory, structured as:
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/group-name/source-id
#' where source-id is the CFGLOC identifier. Nested within this path are (at a minimum):
#'         /stats            - parquet files with multi-depth statistics
#'         /quality_metrics  - parquet files with multi-depth quality metrics
#' A sibling /group directory at the group-name level must contain <source-id>.json with HOR/VER.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion
#' of DirIn.
#'
#' @param DirSubCopy (optional) Character vector. Names of additional subfolders at the same level
#' as stats/quality_metrics to copy via symbolic link to the output path.
#'
#' @param SchmStats (optional) A json-formatted character string containing the avro schema for the
#' depth-renamed stats output parquet files. If NULL, schema is inferred from the data.
#'
#' @param SchmQm (optional) A json-formatted character string containing the avro schema for the
#' depth-renamed quality_metrics output parquet files. If NULL, schema is inferred from the data.
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init. Defaults to NULL,
#' in which case the logger will be created within the function.
#'
#' @return A repository in DirOutBase containing depth-renamed stats and quality_metrics parquet
#' files, where DirOutBase replaces BASE_REPO of DirIn but otherwise retains the child directory
#' structure of the input path.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # NOT RUN
#' DirIn <- '/home/tburlingame/pfs/concH2oSoilSalinity_group_path_split/2025/10/17/conc-h2o-soil-salinity-split_GRSM004503/CFGLOC105332'
#' DirOutBase <- '/home/tburlingame/pfs/out_concH2oSalinity'
#' wrap.concH2oSalinity.grp.split(DirIn, DirOutBase)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame (2026-04-22)
#     original creation
#   Teresa Burlingame (2026-04-22)
#     renamed grp.comb -> grp.split, added SchmStats and SchmQm parameters
##############################################################################################
wrap.concH2oSalinity.grp.split <- function(DirIn,
                                           DirOutBase,
                                           SchmStats = NULL,
                                           SchmQm = NULL,
                                           DirSubCopy = NULL,
                                           log = NULL) {

  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Gather info about the input directory (extracts date and repo path components)
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn, log = log)

  # The CFGLOC name is the terminal directory of DirIn
  nameCfgloc <- base::basename(DirIn)

  # The group-level directory is the parent of the CFGLOC directory
  DirGroup <- base::dirname(DirIn)

  # Read the group JSON for this source location
  fileGrpJson <- fs::path(DirGroup, 'group', paste0(nameCfgloc, '.json'))
  if (!base::file.exists(fileGrpJson)) {
    log$fatal(base::paste0('Group JSON not found: ', fileGrpJson))
    stop()
  }

  grpJson <- base::tryCatch(
    rjson::fromJSON(file = fileGrpJson, simplify = TRUE),
    error = function(e) {
      log$fatal(base::paste0('Cannot read group JSON: ', fileGrpJson, ' Error: ', e$message))
      stop()
    }
  )

  # Extract HOR and VER from the group JSON features
  feat <- grpJson$features[[1]]
  HOR <- feat$HOR
  VER <- feat$VER
  site <- feat$site

  log$info(base::paste0('Group info - site: ', site, '  HOR: ', HOR, '  VER: ', VER))

  # Derive the depth index from VER (e.g., VER "503" -> depth 3 -> "Depth03")
  # VER values follow the convention 5XX where XX is the depth position (01-08)
  depthIdx <- as.integer(VER) - 500
  if (depthIdx < 1 || depthIdx > 8) {
    log$fatal(base::paste0('Unexpected VER value "', VER, '": computed depth index ', depthIdx,
                           ' is outside expected range 1-8.'))
    stop()
  }
  depthStr <- base::sprintf('Depth%02d', depthIdx)
  log$info(base::paste0('Will extract columns for ', depthStr, ' (VER=', VER, ')'))

  # Define input subdirectory paths
  dirInStats <- fs::path(DirIn, 'stats')
  dirInQm    <- fs::path(DirIn, 'quality_metrics')

  # Set up output directories. Stats and quality_metrics are written under the
  # CFGLOC-level directory (preserving the full input path structure), but the
  # group JSON is written one level up, directly under the group directory
  dirOut    <- fs::path(DirOutBase, InfoDirIn$dirRepo)
  dirOutStats <- fs::path(dirOut, 'stats')
  dirOutQm    <- fs::path(dirOut, 'quality_metrics')
  dirOutGroup <- fs::path(base::dirname(dirOut), 'group')

  # Create the CFGLOC-level data directories. 'location' holds the synthesized
  # per-VER CFGLOC JSON consumed by pub_files (sensor positions), sibling to
  # stats and quality_metrics so it lands under DATA_TYPE_INDEX in consolidate.
  NEONprocIS.base::def.dir.crea(
    DirBgn = dirOut,
    DirSub = c('stats', 'quality_metrics', 'location'),
    log = log
  )
  # Create the group directory one level up (above the CFGLOC identity)
  NEONprocIS.base::def.dir.crea(
    DirBgn = base::dirname(dirOut),
    DirSub = c('group'),
    log = log
  )

  # Copy additional subdirectories via symbolic link if requested
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy, c('stats', 'quality_metrics', 'group')))
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(
      DirSrc  = fs::path(DirIn, DirSubCopy),
      DirDest = dirOut,
      LnkSubObj = FALSE,
      log = log
    )
  }

  # Helper: select depth-specific columns from a data frame and rename them.
  # Keeps startDateTime, endDateTime, all columns containing depthStr, and
  # drops any depth##SoilMoisture columns (raw depth metadata).
  selectRenameCols <- function(df, depthStr) {
    allCols <- base::names(df)

    # Identify depth-specific columns (match the target depth)
    depthCols <- base::grep(depthStr, allCols, value = TRUE, fixed = TRUE)

    # Drop the depth##SoilMoisture physical depth columns - not needed in output
    depthCols <- depthCols[!base::grepl('SoilMoisture', depthCols)]

    keepCols <- c('startDateTime', 'endDateTime', depthCols)
    # Only keep columns that actually exist (guard against schema drift)
    keepCols <- base::intersect(keepCols, allCols)

    df <- df[, keepCols, drop = FALSE]

    # Rename: strip the depth indicator string (e.g., "Depth03") from all column names
    base::names(df) <- base::gsub(depthStr, '', base::names(df), fixed = TRUE)

    return(df)
  }

  # Helper: process all parquet files in a source directory, applying column selection/rename
  processParquetDir <- function(dirIn, dirOut, depthStr, label, Schm) {
    files <- base::list.files(dirIn, pattern = '\\.parquet$', full.names = FALSE, recursive = FALSE)

    if (base::length(files) == 0) {
      log$warn(base::paste0('No parquet files found in ', label, ' directory: ', dirIn))
      return(invisible(NULL))
    }

    for (f in files) {
      fileIn  <- fs::path(dirIn, f)
      fileOut <- fs::path(dirOut, f)

      df <- base::tryCatch(
        arrow::read_parquet(fileIn, as_data_frame = TRUE),
        error = function(e) {
          log$error(base::paste0('Cannot read parquet file: ', fileIn, ' Error: ', e$message))
          stop()
        }
      )

      df <- selectRenameCols(df, depthStr)

      rptOut <- base::try(
        NEONprocIS.base::def.wrte.parq(
          data = df,
          NameFile = base::as.character(fileOut),
          Schm = Schm
        ),
        silent = TRUE
      )
      if (base::class(rptOut)[1] == 'try-error') {
        log$error(base::paste0('Cannot write parquet file: ', fileOut, '. ', attr(rptOut, 'condition')))
        stop()
      }

      log$debug(base::paste0('Wrote ', label, ' file: ', fileOut))
    }
  }

  # Process stats files
  processParquetDir(dirInStats, dirOutStats, depthStr, 'stats', SchmStats)

  # Process quality_metrics files
  processParquetDir(dirInQm, dirOutQm, depthStr, 'quality_metrics', SchmQm)

  # Synthesize the per-VER CFGLOC location JSON consumed by pub_files. Depth
  # for this VER lives in the stats INPUT as depth##SoilMoistureMean (added as
  # a passthrough TermStat in stats_group_and_compute since cal-driven depth
  # varies within a day when there are mid-day cal changes or sensor swaps).
  # Read from dirInStats because selectRenameCols drops the column from output
  # (case-sensitive grep won't match lowercase 'd'). Emit a CFGLOC-shaped JSON
  # marked override_source=enviroscan so pub_files' is_enviroscan_sensor branch
  # differentiates the 8 rows by JSON contents.
  #
  # Segmentation (5-min bins, "both neighbors match" persistence rule):
  #   - Prefer the 5-min stats parquet (_005.parquet), else shortest bin.
  #   - A bin is 'confirmed' if both temporal neighbors share its value
  #     (edge bins fall back to single-neighbor match; single-bin day: non-NA).
  #   - Unconfirmed bins define the ambiguous gap between adjacent segments.
  #   - Adjacent segments meet at the midpoint of that gap (no positions gap).
  #   - First segment starts at dayStart; last segment ends at dayEnd; pub_files
  #     merges day-adjacent same-(HOR,VER,depth) features into contiguous rows.
  depthColName <- base::sprintf('depth%02dSoilMoistureMean', depthIdx)

  statsFiles <- base::list.files(dirInStats, pattern = '\\.parquet$',
                                 full.names = TRUE, recursive = FALSE)

  # Prefer explicit 5-min files by conventional NEON naming (..._005.parquet);
  # fall back to the file with the shortest median bin duration.
  fileFinest <- NULL
  if (base::length(statsFiles) > 0) {
    filesByName <- statsFiles[base::grepl('_005\\.parquet$', statsFiles)]
    if (base::length(filesByName) > 0) {
      fileFinest <- filesByName[1]
    } else {
      minBin <- Inf
      for (f in statsFiles) {
        dfTry <- base::tryCatch(arrow::read_parquet(f, as_data_frame = TRUE),
                                error = function(e) NULL)
        if (!base::is.null(dfTry) &&
            all(c('startDateTime','endDateTime') %in% base::names(dfTry)) &&
            base::nrow(dfTry) >= 1) {
          bin <- stats::median(as.numeric(difftime(dfTry$endDateTime,
                                                   dfTry$startDateTime,
                                                   units = 'secs')), na.rm = TRUE)
          if (!is.na(bin) && bin < minBin) { minBin <- bin; fileFinest <- f }
        }
      }
    }
  }

  features <- base::list()

  if (!base::is.null(fileFinest)) {
    dfStats <- base::tryCatch(
      arrow::read_parquet(fileFinest, as_data_frame = TRUE),
      error = function(e) NULL
    )
    if (!base::is.null(dfStats) &&
        depthColName %in% base::names(dfStats) &&
        all(c('startDateTime','endDateTime') %in% base::names(dfStats)) &&
        base::nrow(dfStats) > 0) {
      ord <- base::order(dfStats$startDateTime)
      dfStats <- dfStats[ord, , drop = FALSE]
      # Round to 4 decimals for equality comparisons (cal depths are ~cm).
      vR <- base::round(base::as.numeric(dfStats[[depthColName]]), 4)
      s  <- dfStats$startDateTime
      e  <- dfStats$endDateTime
      n  <- base::length(vR)

      confirmed <- base::rep(FALSE, n)
      if (n == 1) {
        confirmed[1] <- !base::is.na(vR[1])
      } else {
        for (i in seq_len(n)) {
          vi <- vR[i]
          if (base::is.na(vi)) next
          if (i == 1) {
            confirmed[i] <- !base::is.na(vR[2]) && (vR[2] == vi)
          } else if (i == n) {
            confirmed[i] <- !base::is.na(vR[i-1]) && (vR[i-1] == vi)
          } else {
            confirmed[i] <- !base::is.na(vR[i-1]) && !base::is.na(vR[i+1]) &&
                            (vR[i-1] == vi) && (vR[i+1] == vi)
          }
        }
      }

      # Walk confirmed bins into runs of matching value.
      segs <- base::list()
      curVal <- NA_real_; curBinStart <- NA; curBinEnd <- NA
      for (i in seq_len(n)) {
        if (!confirmed[i]) next
        if (base::is.na(curVal)) {
          curVal <- vR[i]; curBinStart <- s[i]; curBinEnd <- e[i]
        } else if (vR[i] == curVal) {
          curBinEnd <- e[i]
        } else {
          segs[[length(segs) + 1]] <- base::list(value = curVal,
                                                 binStart = curBinStart,
                                                 binEnd   = curBinEnd)
          curVal <- vR[i]; curBinStart <- s[i]; curBinEnd <- e[i]
        }
      }
      if (!base::is.na(curVal)) {
        segs[[length(segs) + 1]] <- base::list(value = curVal,
                                               binStart = curBinStart,
                                               binEnd   = curBinEnd)
      }

      # Day-scoped bounds (from datum path, not data — full-day coverage even
      # if the parquet has NA bins at day edges). InfoDirIn$time is UTC midnight.
      dayStart <- InfoDirIn$time
      base::attr(dayStart, 'tzone') <- 'UTC'
      dayEnd <- dayStart + 86400

      nSeg <- base::length(segs)
      crossovers <- base::vector('list', base::max(0, nSeg - 1))
      if (nSeg >= 2) {
        for (k in seq_len(nSeg - 1)) {
          a <- segs[[k]]; b <- segs[[k + 1]]
          gapSec <- base::as.numeric(difftime(b$binStart, a$binEnd, units = 'secs'))
          crossovers[[k]] <- a$binEnd + gapSec / 2
        }
      }

      isoFmt <- function(x) {
        base::attr(x, 'tzone') <- 'UTC'
        base::format(x, '%Y-%m-%dT%H:%M:%SZ')
      }

      for (k in seq_len(nSeg)) {
        segK <- segs[[k]]
        segStart <- if (k == 1)    dayStart          else crossovers[[k - 1]]
        segEnd   <- if (k == nSeg) dayEnd            else crossovers[[k]]
        features[[length(features) + 1]] <- base::list(
          type = 'Feature',
          geometry = NA,
          properties = base::list(name = nameCfgloc, site = site),
          HOR = HOR,
          VER = VER,
          depth = segK$value,
          positionStartDateTime = isoFmt(segStart),
          positionEndDateTime   = isoFmt(segEnd)
        )
      }
    }
  }

  if (base::length(features) == 0) {
    log$warn(base::paste0(
      'No confirmed depth segments found in column ', depthColName,
      ' for ', nameCfgloc, ' (VER=', VER,
      '). Skipping location JSON emit; sensor_positions will not include this row.'
    ))
  } else {
    locJson <- base::list(
      type = 'FeatureCollection',
      override_source = 'enviroscan',
      features = features
    )
    fileOutLocJson <- fs::path(dirOut, 'location', paste0(nameCfgloc, '.json'))
    base::writeLines(rjson::toJSON(locJson, indent = 2), con = fileOutLocJson)
    log$info(base::paste0(
      'Wrote synthesized location JSON: ', fileOutLocJson,
      ' (HOR=', HOR, ' VER=', VER, ' segments=', base::length(features), ')'
    ))
  }


  # Link only this datum's group JSON into the output group/ directory.
  fileOutGrpJson <- fs::path(dirOutGroup, paste0(nameCfgloc, '.json'))
  if (!base::file.exists(fileOutGrpJson)) {
    linkOk <- base::file.symlink(from = fileGrpJson, to = fileOutGrpJson)
    if (!isTRUE(linkOk)) {
      log$warn(base::paste0('Could not create symbolic link for group JSON: ',
                            fileGrpJson, ' -> ', fileOutGrpJson,
                            '. Falling back to file copy.'))
      copyOk <- base::file.copy(from = fileGrpJson, to = fileOutGrpJson, overwrite = TRUE)
      if (!isTRUE(copyOk)) {
        log$fatal(base::paste0('Could not copy group JSON to output: ', fileOutGrpJson))
        stop()
      }
    }
  }

  log$info(base::paste0('Completed processing for ', nameCfgloc, ' (', site, ' HOR=', HOR, ' VER=', VER, ')'))

  return(invisible(NULL))
}
