##############################################################################################
#' @title Unit test for Threshold selection module for NEON IS data processing.

#' @author
#' Mija Choi \email{choim@batelleEcology.org}
#'
#' @description Wrapper function. Select thresholds for each processing day and location for sets of term and context(s)
#' among all available thresholds.
#' The choice of constraint/threshold to use is determined by moving up the following hierarchy
#' from finer to coarser constraints until one applies. Thus, the finest applicable level of constraint
#' is chosen. Threshold selection order is as follows (1 being the finest possible constraint):
#' 6. Realm, annual
#' 5. Realm, seasonal
#' 4. Site-specific, annual
#' 3. Site-specific, seasonal
#' 2. Sensor-spefific, annual
#' 1. Sensor-specific, seasonal
#'
#'
#' @param DirIn Character value. The input path to the data from a single sensor or location, structured as follows:
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/id, where # indicates any number of parent and child directories
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates
#' the 4-digit year, 2-digit month, and' 2-digit day. The id is the unique identifier of the sensor or location. \cr
#'
#' Nested within this path are the folders:
#'         /threshold
#'         /location
#' The location folder holds at least 1 json file with location data/properties specific to the location
#' identifier in the directory path. If there is more than one file in this directory, only the first will
#' be used, since the properties of the named location (i.e. site) should be the same across files.
#' The threshold folder contains a json file with available thresholds from which the threshold selection
#' order will be used to choose from.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn.
#'
#' @param thshRaw List of thresholds to select from, as produced by reading a json formatted threshold file with
#' rjson::fromJSON (see example)
#'
#' @param thshPosx The list of thresholds thshRaw after passing it through NEONprocIS.qaqc::def.read.thsh.qaqc.list
#' in order to turn the timestamps to POSIXct (see example)
#'
#' @param ParaThsh A list of lists, each specifying a combination of term and context(s) to select thresholds for
#' the day and location. Applicable thresholds will be selected separately for each sub-list and combined when written to file.
#' Each sub-list contains two named elements:\cr
#' \code{Term} Character value. The term (i.e. variable) to select thresholds for. For example: 'temp'. \cr
#' \code{Ctxt} Character vector. Any number of contexts that apply to the term in order to define its application.
#' For example, c('soil','deep') would indicate to select thresholds for term 'temp' matching both contexts 'soil', and
#' 'deep'. Enter NULL if there are no applicable contexts, meaning that the term stands alone.
#' Note that AND logic applies within each sub-list (matching term and all contexts).
#'
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return A directory structure created in DirOutBase, where the terminal directory of DirOutBase
#' replaces BASE_REPO of DirIn but otherwise retains the child directory structure of the input path. The filtered
#' thresholds containing the thresholds applicable to each location, day, and sets of term and context(s)
#' will be written to thresholds.json file in the same format as input thshRaw and placed in an additional subdirectory
#' called 'thresholds' at the same level as the
#' location directory. Be sure to specify "DirSubCopy=location" to pass the location directory through
#' to the output (along with any other desired subdirectories to copy over. Otherwise, they will be dropped.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' # Open and parse the thresholds file (with all available thresholds)
#' FileThsh <- "~/pfs/tempSoil_threshold_filter/thresholds.json"
#' thshRaw <- rjson::fromJSON(file=FileThsh,simplify=TRUE),silent=FALSE)
#' thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.list(listThsh=thshRaw) # This turns dates to POSIXct, which is required
#' ParaThsh <- list(list(Term='temp',Ctxt='soil'))
#' wrap.thsh.slct(DirIn="~/pfs/tempSoil_locations/prt/2020/01/01/CFGLOC12345",
#'                DirOutBase="~/pfs/out",
#'                thsh=thsh,
#'                ParaThsh=ParaThsh,
#'                DirSubCopy='location'
#' )

# changelog and author contributions / copyrights
#   Mija Choi (2022-01-11)
#     original creation
##############################################################################################
# Define test context
context("\n       | Unit test of Quality metrics module for NEON IS data processing \n")

test_that("Unit test of wrap.thsh.slct.R", {
  source('../../flow.thsh.slct/wrap.thsh.slct.R')
  library(stringr)
  
  FileThsh <- "pfs/thresholds.json"
  thshRaw <- rjson::fromJSON(file = FileThsh, simplify = TRUE)
  # This turns dates to POSIXct, which is required
  thshPosx <-
    NEONprocIS.qaqc::def.read.thsh.qaqc.list(listThsh = thshRaw)
  ParaThsh <-
    list(list(
      Term = 'relativeHumidity',
      Ctxt = c('relative-humidity', 'terrestrial')
    ))
  
  DirIn = "pfs/hmp_locations/2020/01/02/CFGLOC101252"
  
  DirOutBase = "pfs/out"
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirSrc = "CFGLOC101252"
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  # Test 1, Happy path
  wrap.thsh.slct(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    thshRaw = thshRaw,
    thshPosx = thshPosx,
    ParaThsh = ParaThsh,
    DirSubCopy = 'location'
  )
  
  # Test 1.a, thshPosx = NULL
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  try(wrap.thsh.slct(
    DirIn = DirIn,
    DirOutBase = DirOutBase,
    thshRaw = thshRaw,
    thshPosx = NULL,
    ParaThsh = ParaThsh,
    DirSubCopy = 'location'
  ), silent = TRUE)
  #
  # Test 2 no location files
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn_nofiles = "pfs/hmp_locations_nofiles/prt/10312"
  try(wrap.thsh.slct(
    DirIn = DirIn_nofiles,
    DirOutBase = DirOutBase,
    thshRaw = thshRaw,
    thshPosx = thshPosx,
    ParaThsh = ParaThsh
  ),
  silent = TRUE)
  
  DirSrc = "10312"
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  # Test 2.a no location files
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn_noLoc = "pfs/hmp_locations_noLocs/2020/01/02/10312"
  try(wrap.thsh.slct(
    DirIn = DirIn_noLoc,
    DirOutBase = DirOutBase,
    thshRaw = thshRaw,
    thshPosx = thshPosx,
    ParaThsh = ParaThsh
  ),
  silent = TRUE)
  
  DirSrc = "10312"
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  #
  # Test 2.b year missing in subDir
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn_wrongDir = "pfs/hmp_locations_wrongDir/noYr/01/02/CFGLOC101252"
  try(wrap.thsh.slct(
    DirIn = DirIn_wrongDir,
    DirOutBase = DirOutBase,
    thshRaw = thshRaw,
    thshPosx = thshPosx,
    ParaThsh = ParaThsh
  ),
  silent = TRUE)
  
  DirSrc = "CFGLOC101252"
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
  # Test 3, 2 location files
  
  if (dir.exists(DirOutBase)) {
    unlink(DirOutBase, recursive = TRUE)
  }
  
  DirIn_2Locs = "pfs/hmp_locations_2files/2020/01/02/10267"
  try(wrap.thsh.slct(
    DirIn = DirIn_2Locs,
    DirOutBase = DirOutBase,
    thshRaw = thshRaw,
    thshPosx = thshPosx,
    ParaThsh = ParaThsh
  ),
  silent = TRUE)
  DirSrc = "10267"
  exstDirSrc <- base::unlist(base::lapply(DirSrc, base::dir.exists))
  
  if (exstDirSrc) {
    cmdSymbLink <- base::paste0('rm ', base::paste0(DirSrc))
    rmSymbLink <- base::lapply(cmdSymbLink, base::system)
  }
})
