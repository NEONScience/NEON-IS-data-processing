##############################################################################################
#' @title  Assess soil temperature closest to sensor depths to determine if data should be flagged. 

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. Determine if test is to be run based on thresholds, find closests temperature sensor, 
#' read in data and check if it is below freezing. If the closest sensor is flagged, do an average of the one above and one below 
#' and check to see if the average of the temperatures are less than 1 degree. If one or both are unavailable, flag NA
#' 
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path are (at a minimum) the folders:
#'         /data
#'         /flags

#' The flags folder holds two files containing basic plausibility and calibration quality flags for 
#' the central processing day only. These files should respectively be named in the convention:
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD_flagsPlausibility.parquet
#' All other files in this directory will be ignored.
#'
#' The threshold folder contains a single file named thresholds.json that holds threshold parameters
#' applicable to the smoothing algorithm.
#' 
#' @param DirTemp Character value. The input path to the temperature data that is used to perform the the temperature test.
#'  Location files are used to determine depth of the sensors and pair with the appropriate enviroscan depth.
#'  Then the data is read in to perform tests. 
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param SchmQf (Optional). A json-formatted character string containing the schema for the standard calibration and
#' plausibility QFs as well as the custom QFs tempTestDepth##QF

#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data/flags/threshold folders in the input path that are to be copied with a 
#' symbolic link to the output path (i.e. carried through as-is). Note that the 'stats' and 'flags' directories 
#' are automatically populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the quality flags of the three different enviroscan data streams:"
#' VSIC, VSWCFactory and VSWCSoilSpecific. Standard flags are read in, then a test for frozen soil is performed. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' 
#' "DirIn=/scratch/pfs/concH2oSoilSalinity_analyze_pad_and_qaqc_plau/2025/10/17/conc-h2o-soil-salinity_GRSM001501/",
#' "DirTemp=/scratch/pfs/concH2oSoilSalinity_group_path/2025/10/17/conc-h2o-soil-salinity_GRSM005501/",
#' "DirOut=/scratch/pfs/tb_out",
#' "DirErr=/scratch/pfs/tb_out/errored_datums"
#' wrap.envscn.temp.flags(DirIn,DirOutBase,DirTemp,DirSubCopy)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame  (2025-02-16)
#     Initial creation
##############################################################################################
wrap.envscn.temp.flags <- function(DirIn,
                                   DirOutBase,
                                   DirTemp,
                                   SchmQf = NULL,
                                   DirSubCopy = NULL,
                                   log = NULL
){
  
  # Start logging if not already initialized
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirInData <- fs::path(DirIn,'data')
  dirInQf <- fs::path(DirIn,'flags')
  
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutQf <- fs::path(dirOut,'flags')
  
  #dirOutData <- fs::path(dirOut,'data')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = c('flags'),
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy, c('flags')))
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc = fs::path(DirIn, DirSubCopy),
                                       DirDest = dirOut,
                                       LnkSubObj = FALSE,
                                       log = log)
  }    
  
  # ===== Load and validate thresholds =====
  dirInThrsh <- fs::path(DirIn, 'threshold')
  fileThsh <- base::list.files(dirInThrsh)
  
  if (base::length(fileThsh) == 0) {
    log$error(base::paste0('No threshold files found in ', dirInThrsh))
    stop()
  }
  
  if (base::length(fileThsh) > 1) {
    log$debug(base::paste0('Multiple threshold files found: ', base::paste(fileThsh, collapse = ', ')))
    fileThsh <- fileThsh[1]
    log$info(base::paste0('Using first threshold file: ', fileThsh))
  }
  
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(
    NameFile = fs::path(dirInThrsh, fileThsh)
  )
  
  # Define expected terms and threshold names
  termTest <- c("VSICDepth01", "VSICDepth02", "VSICDepth03", "VSICDepth04", 
                "VSICDepth05", "VSICDepth06", "VSICDepth07", "VSICDepth08")
  tempTestName <- "tempTest"
  
  #############
  #dummy TODO delete after thresholds finalized
  tempTestName <- "Range Threshold Soft Min"
  thshSubset <- thsh[thsh$threshold_name == tempTestName & thsh$term_name %in% termTest, ]
  thshSubset$threshold_name <- "tempTest"
  
  #replace with just   thshSubset <- thsh[thsh$threshold_name == tempTestName & thsh$term_name %in% termTest, ]

  ###############
  
  # Filter thresholds for temperature test
  
  if (base::nrow(thshSubset) == 0) {
    log$error(base::paste0('No "', tempTestName, '" thresholds found for terms: ', 
                           base::paste(termTest, collapse = ', ')))
    stop()
  }
  
  # Create threshold lookup: term_name -> number_value
  thresholdLookup <- stats::setNames(thshSubset$number_value, thshSubset$term_name)
  
  # Validate all expected terms have threshold values
  missingTerms <- termTest[!termTest %in% base::names(thresholdLookup)]
  if (base::length(missingTerms) > 0) {
    log$error(base::paste0('Missing "', tempTestName, '" thresholds for: ', 
                           base::paste(missingTerms, collapse = ', ')))
    stop()
  }
  
  # ===== Load enviroscan data =====
  fileData <- base::list.files(dirInData, pattern = '.parquet', full.names = FALSE)
  
  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  
  # Filter to soil moisture columns and essential metadata
  soilCols <- base::names(data)[base::grepl("SoilMoisture", base::names(data))]
  
  keepStatic <- c("source_id", "site_id", "readout_time")
  keepCols <- c(keepStatic, base::intersect(soilCols, base::names(data)))
  dataSm <- data[, keepCols, drop = FALSE]
  
  # Extract depth keys (e.g., "01", "02", etc.)
  depthKeys <- base::sub("^depth(\\d{2})SoilMoisture$", "\\1", soilCols)
  
  # Initialize temperature test QF columns (default: -1 = test not run)
  qfCols <- base::paste0("tempTestDepth", depthKeys, "QF")
  dataSm[qfCols] <- -1L
  
  # ===== Map thresholds to data column names =====
  # Convert threshold names from "VSICDepth02" to "depth02SoilMoisture"
  base::names(thresholdLookup) <- base::sub(
    "^VSICDepth(\\d{2})$",
    "depth\\1SoilMoisture",
    base::names(thresholdLookup)
  )
  
  # ===== Load temperature sensor metadata =====
  sensorDepthDf <- def.load.temp.sensors(DirTemp = DirTemp, log = log)
  
  # Check if we have any temperature sensors available
  if (nrow(sensorDepthDf) == 0) {
    log$warn("No temperature sensors available. All temperature test flags will remain -1 (test not run).")
    # Skip temperature testing - flags already initialized to -1
  } else {
    # ===== Apply temperature test to each depth =====
    for (col in soilCols) {
    
    depthNum <- base::sub("^depth(\\d{2}).*$", "\\1", col)
    qfName <- base::paste0("tempTestDepth", depthNum, "QF")
    
    # Check if test should be run (threshold value: 0 = skip, 1 = run)
    if (thresholdLookup[[col]] == 0) {
      log$info(base::paste0('Skipping temperature test for ', col, ' (threshold = 0)'))
      dataSm[[qfName]] <- 0L  # Automatic pass
      
    } else if (thresholdLookup[[col]] == 1) {
      log$debug(base::paste0('Running temperature test for ', col))
      
      # Find target depth from maximum sensor depth in the data
      targetDepth <- base::max(dataSm[[col]], na.rm = TRUE)
      
      # Check if target depth is valid
      if (!base::is.finite(targetDepth) || base::is.na(targetDepth)) {
        log$warn(base::paste0('No valid depth data found for ', col, 
                              '. Temperature test flag will remain -1 (test not run).'))
        # Leave flag at -1 (already initialized)
      } else {
        # Find closest temperature sensor
        sensorInfo <- def.find.temp.sensor(
          targetDepth = targetDepth,
          sensorDepthDf = sensorDepthDf,
          log = log
        )
        
        # Check if a matching sensor was found
        if (base::is.null(sensorInfo)) {
          log$warn(base::paste0('No temperature sensor found for ', col,
                                ' at depth ', targetDepth, 'm. Temperature test flag will remain -1 (test not run).'))
          # Leave flag at -1 (already initialized)
        } else {
          # Check if sensor is within acceptable distance (0.25m)
          sensorDepth <- sensorInfo$depth_m
          depthDiff <- base::abs(targetDepth - sensorDepth)
          
          if (depthDiff > 0.25) {
            log$warn(base::paste0('Temperature sensor for ', col, ' is ', 
                                  base::round(depthDiff, 3), 'm away from target depth ',
                                  targetDepth, 'm (max allowed: 0.25m). ',
                                  'Temperature test flag will remain -1 (test not run).'))
            # Leave flag at -1 (already initialized)
          } else {
            # Calculate temperature flags
            tempData <- def.calc.temp.flags(
              sensorInfo = sensorInfo,
              targetDepth = targetDepth,
              log = log
            )
            
            # Apply flags to high-frequency data
            dataSm <- def.apply.temp.flags(
              dataSm = dataSm,
              tempData = tempData,
              qfColName = qfName,
              log = log
            )
          }
        }
      }
    }
    } # End temperature test loop
  } # End check for available sensors
  
  # ===== Merge with existing plausibility flags =====
  qfFlags <- dataSm[, c("readout_time", qfCols)]
  
  fileQfPlau <- base::list.files(dirInQf, pattern = 'Plausibility.parquet', full.names = FALSE)
  
  if (base::length(fileQfPlau) == 0) {
    log$error(base::paste0('No plausibility flag files found in ', dirInQf))
    stop()
  }
  
  qfPlau <- NEONprocIS.base::def.read.parq.ds(
    fileIn = fs::path(dirInQf, fileQfPlau),
    VarTime = 'readout_time',
    RmvDupl = TRUE,
    Df = TRUE,
    log = log
  )
  
  # Combine plausibility flags with temperature test flags
  qfAll <- base::merge(qfPlau, qfFlags, by = "readout_time", all = TRUE)
  
  # ===== Sort columns in standard order =====
  sortedCols <- def.sort.qf.cols(base::names(qfAll))
  qfAll <- qfAll[, sortedCols]
  
  # ===== Write output =====
  nameFileQfOut <- fs::path(dirOutQf, fileQfPlau)
  
  rptWrite <- base::try(
    NEONprocIS.base::def.wrte.parq(
      data = qfAll,
      NameFile = nameFileQfOut,
      log = log
    ),
    silent = TRUE
  )
  
  if ('try-error' %in% base::class(rptWrite)) {
    log$error(base::paste0(
      'Cannot write output to ', nameFileQfOut, '. ',
      base::attr(rptWrite, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0('Wrote updated flags to ', nameFileQfOut))
  }
  
  return()
}

