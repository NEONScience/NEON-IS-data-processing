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
wrap.envscn.temp.flags<- function(DirIn,
                                    DirOutBase,
                                    DirTemp,
                                    SchmQf=NULL,
                                    DirSubCopy=NULL,
                                    log=NULL
){
  
  library(tibble)
  library(dplyr)
  library(purrr)
  library(tidyr)
  library(data.table)
  
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
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('flags')))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  #####Threshold####
  dirInThrsh <- fs::path(DirIn,'threshold')
  fileThsh <- list.files(dirInThrsh)
  
  # Read thresholds (optimized error handling)
  if (base::length(fileThsh) > 1) {
    log$debug(base::paste0('threshold files are ', paste(fileThsh, collapse = ', ')))
    fileThsh <- fileThsh[1]
    log$info(base::paste0('Using first threshold file: ', files$threshold))
  }
  
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(
    NameFile = fs::path(dirInThrsh, fileThsh )
  )
  
  # Verify terms exist (vectorized)
  termTest <- c("VSICDepth01", "VSICDepth02", "VSICDepth03", "VSICDepth04", "VSICDepth05", "VSICDepth06", "VSICDepth07", "VSICDepth08")
  if (!all(termTest %in% thsh$term_name)) {
    log$error(base::paste0('Missing thresholds some of the following not in threshold file: ', termTest))
    stop()
  }
  
  tempTest <- "tempTest"
  
  #############
  #dummy TODO delete after thresholds finalized
  tempTest <- "Range Threshold Soft Min"
  thsh_subset <- thsh[thsh$threshold_name == tempTest, ]
  thsh_subset$threshold_name <- "tempTest"
  ###############
  
  thsh_subset <- thsh_subset[thsh_subset$term_name %in% termTest,]

  threshold_lookup <- setNames(thsh_subset$number_value, thsh_subset$term_name)
  
  # Validate all thresholds exist
  if (any(is.na(threshold_lookup))) {
    log$error("Missing threshold values")
    stop()
  }
  
  
  ############Thresholds End ############
  
  #### data files ###
  
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)

  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  
  # only keep depth variables here for testing. 
  # Build the SoilMoisture column names to keep
  soil_cols <- names(data[grepl(names(data), pattern="SoilMoisture")])
  
  # 2) Pick your always-keep columns (adjust as needed)
  keep_static <- c("source_id", "site_id", "readout_time")
  
  # 3) Keep only those columns (and silently drop any that don’t exist)
  keep_cols <- c(keep_static, intersect(soil_cols, names(data)))
  data_sm <- data[, keep_cols, drop = FALSE]
  
  # Extract the "##" depth keys
  keys <- sub("^depth(\\d{2})SoilMoisture$", "\\1", soil_cols)
  
  # Build the new QF column names
  qf_cols <- paste0("depth", keys, "tempTestQF")
  
  # Add them, initialized to -1
  data_sm[qf_cols] <- -1
  
  ######temperature data and locations##########
  
  
  ###match threshold to column names
  
  # rename names like "VSICDepth02" -> "depth02SoilMoisture"
  names(threshold_lookup) <- sub(
    "^VSICDepth(\\d{2})$",
    "depth\\1SoilMoisture",
    names(threshold_lookup)
  )
  
  
  ####Read in temperature locations. 
  
  filesTemp <- list.files(DirTemp, full.names = TRUE, recursive = TRUE)
  
  filesTempData <- filesTemp[grepl("/temp-soil_[^/]+/data/[^/]+001\\.parquet$", filesTemp)]
  filesTempLocation <- filesTemp[grepl("/temp-soil_[^/]+/location/[^/]*_locations\\.json$", filesTemp)]
  
  
  # Helper to extract the temp-soil ID from a path, e.g., temp-soil_GRSM005501
  extract_ts_id <- function(paths) {
    str_match(paths, "/(temp-soil_[^/]+)/")[, 2]
  }
  
  extract_z_offset <- function(path) {
    tryCatch({
      x <- jsonlite::fromJSON(path, simplifyVector = FALSE)
      z <- x$features[[1]]$properties$locations$features[[1]]$properties$z_offset
      as.numeric(z)
    }, error = function(e) NA_real_)
  }

  # Build a table from location files
  df_locations <- tibble(
    sensor_id  = extract_ts_id(filesTempLocation),
    depth_m    = vapply(filesTempLocation, extract_z_offset, numeric(1)),
    location_path = filesTempLocation
  ) %>%
    distinct(sensor_id, .keep_all = TRUE)
  
  # Build a table from data files
  df_data <- tibble(
    sensor_id = extract_ts_id(filesTempData),
    data_path = filesTempData
  ) %>%
    distinct(sensor_id, .keep_all = TRUE)
  
  # Safe join by key
  sensor_depth_df <- df_locations %>%
    inner_join(df_data, by = "sensor_id") %>%
    select(sensor_id, depth_m, location_path, data_path)
  
 # if theres errors stop
  if (any(is.na(sensor_depth_df$depth_m))) {
    error("Some location files failed to yield z_offset:\n",
            paste(z_offsets_df$location_file[!is.na(z_offsets_df$error)], collapse = "\n"))
    stop()
  }

###########
  
##########

#generate test logic
for(col in soil_cols){

  depth_num <- sub("^depth(\\d{2}).*$", "\\1", col)
  qf_name <- paste0("depth", depth_num, "tempTestQF")
  
  #first check if we are running the test at all
  if(threshold_lookup[[col]]==0){
    log$info(paste0('skipping ', col, 'threshold indicates test not run'))
    #if not running test, data is automatic pass.
    data_sm[[qf_name]] <- 0
    
  }else if (threshold_lookup[[col]]==1){
    
    #find closest matching temp data. 
    # target depth (meters, negative = below surface)
    target_depth <- max(data_sm[[col]], na.rm = T)

    # Find closest (tie-breaker: prefer shallower, i.e., greater/more positive)
    closest_sensor <- sensor_depth_df %>%
      filter(!is.na(depth_m)) %>%
      mutate(abs_diff = abs(depth_m - target_depth)) %>%
      arrange(abs_diff, depth_m, sensor_id) %>%  # stable tie-breaker
      slice(1) %>%
      select(sensor_id, depth_m, data_path, location_path)
    
    closest_depth <- closest_sensor$depth_m
    
    #read in data 
    
    ####todo Null handling? 
    temp_data_close <- NEONprocIS.base::def.read.parq(closest_sensor$data_path)
    
    #make new flag checking data frame
    temp_data <- temp_data_close[,c('startDateTime', 'endDateTime')]
    
    #if no temp data is flagged for closest sensor, continue with test
    if(all(temp_data_close$finalQF==0)){
      temp_data$temp_flag <- NA_integer_
      temp_data$temp_flag <-  as.integer(temp_data_close$soilTempMean < temp_data_close$soilTempExpUncert)
      #convert any remaining NA to -1 
      temp_data$temp_flag[is.na(temp_data$temp_flag)] <- -1
    }else{#if we need one up one down, grab them.

      ### combine results of the closest sensor and the joined neighbors
      #temp_data flags
      #initialize column
      temp_data$temp_flag <- NA_integer_
      #add flag value for non-flagged close data
      
      idx_ok <- !is.na(temp_data_close$finalQF) & temp_data_close$finalQF == 0L
      temp_data$temp_flag[idx_ok] <- as.integer(
        temp_data_close$soilTempMean[idx_ok] < temp_data_close$soilTempExpUncert[idx_ok]
      )
      
      # Ensure POSIXct and convert to data.table
      setDT(temp_data)
      temp_data[, `:=`(
        startDateTime = as.POSIXct(startDateTime, tz = "UTC"),
        endDateTime   = as.POSIXct(endDateTime,   tz = "UTC"),
        temp_flag     = as.integer(temp_flag)
      )]
      
        # Neighbors relative to the closest depth
        #   - "next higher (shallower)" means depth_m > closest_depth (less negative)
        next_higher <- sensor_depth_df %>%
          filter(!is.na(depth_m), depth_m > closest_depth) %>%
          arrange(depth_m, sensor_id) %>%
          slice(1) %>%
          select(sensor_id, depth_m, data_path, location_path)
        
        #   - "next lower (deeper)" means depth_m < closest_depth (more negative)
        #     choose the one closest to the closest depth
        next_lower <- sensor_depth_df %>%
          filter(!is.na(depth_m), depth_m < closest_depth) %>%
          arrange(desc(depth_m), sensor_id) %>%
          slice(1) %>%
          select(sensor_id, depth_m, data_path, location_path)
        
        # 3) Read the parquet files if those neighbors exist
        if (nrow(next_lower) == 1) {
          temp_data_lower  <- NEONprocIS.base::def.read.parq(next_lower$data_path)
        } else {
          temp_data_lower <- NULL
        }
        if (nrow(next_higher) == 1) {
          temp_data_higher <- NEONprocIS.base::def.read.parq(next_higher$data_path)
        } else {
          temp_data_higher <- NULL
        }
        
        #if they both exist, pull in and do check 
        if (!is.null(temp_data_lower) & !is.null(temp_data_higher)){
          has_neighbor_join <- TRUE
          #filter for only finalQF <- 1
          temp_data_lower <- temp_data_lower[temp_data_lower$finalQF<1, ]
          temp_data_higher <- temp_data_higher[temp_data_higher$finalQF<1, ]
          #calculate test
          temp_data_lower$zero_check_low <- temp_data_lower$soilTempMean - temp_data_lower$soilTempExpUncert
          temp_data_higher$zero_check_high <- temp_data_higher$soilTempMean - temp_data_higher$soilTempExpUncert
          #join data
          temp_data_join <- dplyr::full_join(temp_data_lower[,c("startDateTime", "endDateTime","zero_check_low")],
                                             temp_data_higher[,c("startDateTime", "endDateTime","zero_check_high")],
                                             by = c("startDateTime", "endDateTime"))
          
          #test to see if average of 2 temps is < 1 degree
          setDT(temp_data_join)
          
          temp_data_join[, avg_zero_check := rowMeans(cbind(zero_check_low, zero_check_high), na.rm = TRUE)]
          temp_data_join[is.nan(avg_zero_check), avg_zero_check := NA_real_]
          temp_data_join[, zero_check := ifelse(is.na(avg_zero_check), NA_integer_, as.integer(avg_zero_check < 1))]

        ##  If neighbor join exists, merge and coalesce (prefer closest; else neighbor)
        if (is.data.frame(temp_data_join) && nrow(temp_data_join) > 0) {
          setDT(temp_data_join)
          temp_data_join[, `:=`(
            startDateTime = as.POSIXct(startDateTime, tz = "UTC"),
            endDateTime   = as.POSIXct(endDateTime,   tz = "UTC"),
            zero_check    = as.integer(zero_check)  # ensure 0/1/NA
          )]
          
          # Exact equality join on [startDateTime, endDateTime].
          combined <- merge(
            temp_data,
            temp_data_join[, .(startDateTime, endDateTime, zero_check)],
            by = c("startDateTime", "endDateTime"),
            all.x = TRUE,
            sort = FALSE
          )
          
          # Fallback: if temp_flag is NA, use neighbor zero_check
          # fcoalesce is a data.table helper (vector-wise coalesce)
          combined[, temp_flag := fcoalesce(temp_flag, zero_check)]
          combined[, zero_check := NULL]
          
          temp_data <- combined
        }
          
        } #if statement looking for neighbors
        
        #any remaining NA values for temp_check == -1
        temp_data$temp_flag[is.na(temp_data$temp_flag)] <- -1

        }  #end of assigning temp test. 
      
    
    #next apply flag to the appropriate soil moisture flag. 
    
    #ensure we have proper date formats

     # Non-equi join: assign each 10-sec row the flag of the minute interval it falls into
      dt_sm   <- as.data.table(data_sm)
      dt_temp <- as.data.table(temp_data)[, .(startDateTime, endDateTime, temp_flag)]
      
      dt_sm[, `:=`(
        readout_start = as.POSIXct(readout_time, tz = "UTC"),
        readout_end   = as.POSIXct(readout_time, tz = "UTC")
      )]
      
      dt_temp[, `:=`(
        startDateTime = as.POSIXct(startDateTime, tz = "UTC"),
        endDateTime   = as.POSIXct(endDateTime,   tz = "UTC"),
        temp_flag     = as.integer(temp_flag)   # TRUE->1, FALSE->0
      )]
      
      # foverlaps requires a key on y (the interval table)
      setkey(dt_temp, startDateTime, endDateTime)
      
      # Use the point-interval columns you created in x
      joined <- foverlaps(
        x = dt_sm[, .(readout_start, readout_end, .rows = .I)],
        y = dt_temp[, .(startDateTime, endDateTime, temp_flag)],
        by.x = c("readout_start", "readout_end"),
        by.y = c("startDateTime", "endDateTime"),
        type = "within",                 # inclusive [start, end]
        nomatch = NA_integer_
      )
      
      new_qf  <- data_sm[[qf_name]]
      
      # rows that have a matching minute interval
      has_match <- !is.na(joined$temp_flag)
      
      # Overwrite with 1/0 where we matched
      new_qf[ joined$.rows[has_match] ] <- joined$temp_flag[has_match]
      
      # 4) Assign back
      data_sm[[qf_name]] <- new_qf

  }#end logic for running test 
      
  } #end logic looping through all the columns. 
  
  #filter flags to just columns of interest
  qf_flags <- data_sm[,c("readout_time", qf_cols)]
  #Read in flag data
  fileQfPlau <- base::list.files(dirInQf,pattern='Plausibility.parquet',full.names=FALSE)
  
  qfPlau <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInQf,fileQfPlau),
                                              VarTime='readout_time',
                                              RmvDupl=TRUE,
                                              Df=TRUE,
                                              log=log)
  
  qfAll <- dplyr::full_join(qfPlau, qf_flags, by = "readout_time")
  #write out logic. 
  
  ###########pick it up here! write it out, clean it up and continue testing. 

###reorder the variables
# Your column names vector
cols <- names(qfAll)

# Define sorting function
sort_cols <- function(cols) {
  # Keep readout_time first
  readout <- cols[cols == "readout_time"]
  
  # Get all QF columns
  qf_cols <- cols[grepl("QF$", cols)]
  
  # Extract components for sorting
  sort_df <- data.frame(
    col = qf_cols,
    stringsAsFactors = FALSE
  )
  
  # Extract variable type, depth, and QF type
  sort_df$var_type <- ifelse(grepl("^VSIC", sort_df$col), "1_VSIC",
                             ifelse(grepl("^VSWCfactory", sort_df$col), "2_VSWCfactory",
                                    ifelse(grepl("^VSWCsoilSpecific", sort_df$col), "3_VSWCsoilSpecific",
                                           "4_temp")))
  
  # Extract depth number
  sort_df$depth <- as.numeric(gsub(".*Depth(\\d+).*|.*depth(\\d+).*", "\\1\\2", sort_df$col))
  
  # Extract QF type with ordering
  sort_df$qf_type <- ifelse(grepl("NullQF$", sort_df$col), "1_Null",
                            ifelse(grepl("GapQF$", sort_df$col), "2_Gap",
                                   ifelse(grepl("RangeQF$", sort_df$col), "3_Range",
                                          ifelse(grepl("StepQF$", sort_df$col), "4_Step",
                                                 ifelse(grepl("PersistenceQF$", sort_df$col), "5_Persistence",
                                                        ifelse(grepl("SpikeQF$", sort_df$col), "6_Spike",
                                                               "7_TempTest"))))))
  
  # Sort by var_type, then depth, then qf_type
  sort_df <- sort_df[order(sort_df$depth, sort_df$qf_type, sort_df$var_type), ]
  
  # Combine readout_time first, then sorted QF columns
  c(readout, sort_df$col)
}

# Apply sorting
sorted_cols <- sort_cols(cols)

# Reorder your dataframe
qfAll<- qfAll[, sorted_cols]

nameFileQfOutFlag <- fileQfPlau

nameFileQfOutFlag <- fs::path(dirOutQf,nameFileQfOutFlag)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = qfAll,
          NameFile = nameFileQfOutFlag,
          log=log
        ),
        silent = TRUE)
      
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          nameFileQfOutFlag,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Wrote updated flags data to file ',
          nameFileQfOutFlag
        ))
      }

  return()
} #end function

