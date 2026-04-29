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

  # Set up output directory, preserving the full path structure under DirOutBase
  dirOut    <- fs::path(DirOutBase, InfoDirIn$dirRepo)
  dirOutStats <- fs::path(dirOut, 'stats')
  dirOutQm    <- fs::path(dirOut, 'quality_metrics')

  NEONprocIS.base::def.dir.crea(
    DirBgn = dirOut,
    DirSub = c('stats', 'quality_metrics'),
    log = log
  )

  # Copy additional subdirectories via symbolic link if requested
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy, c('stats', 'quality_metrics')))
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

  log$info(base::paste0('Completed processing for ', nameCfgloc, ' (', site, ' HOR=', HOR, ' VER=', VER, ')'))

  return(invisible(NULL))
}
