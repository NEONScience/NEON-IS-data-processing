##############################################################################################
#' @title Create custom flag for lightning rod shading
#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#' 
#' @description Definition function. Using site specific thresholds and location information for site calculate, calculate solar
#' angles and the impact of lightning rod shading radiation pedestal. 
#' 
#' @param flagDf data frame input to append flags to reqs at least var readout_time
#' 
#' @param DirIn list of incoming directories. Used to check for location/threshold information. Function cannot run without both
#' 
#' @param termTest terms to run for shading flag. Looks for thresholds for this term. 
#' 
#' @param shadowSource Which type of shadow is expected. Options include LR Cimel Misc to distinguish between 
#'different types of shading sources from different directions. 
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return data frame of cmp22 with appended column for shadowQF indicating that lightning rod was shading sensor (0 = no shading, 1 = shading)
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN

#' @seealso Currently none

# changelog and author contributions / copyrights
#  Teresa Burlingame (2025-09-15)
#     Initial creation
#  Teresa Burlingame (2025-09-25)
#     fix logic output and add handling for Az near North.
#   Teresa Burlingame (2025-09-29)
#     Testing removal of time buffer and testing thresholds being NA to trigger skipping of the flag. 
##############################################################################################
def.rad.shadow.flags <- function(DirIn, 
                                 flagDf,
                                 termTest = NULL,
                                 shadowSource = NULL,
                                 log = NULL){
  if(base::is.null(log)){
      log <- NEONprocIS.base::def.log.init()
    }   
  #use calculated azimuths to verify shading on one year of data, adjust as needed to finalize column for future threshold in pachy module
  library(magrittr)
  library(suncalc)
  library(lubridate)
  library(dplyr)
  library(data.table)

  if(!"readout_time" %in% names(flagDf)){
    log$error("readout_time not in flagDF variable. Invalid configuration.")
    stop()
  } 
  
  #store names before transformation 
  original_flagDf_cols <- names(flagDf)
  
  #read in thresholds for shading
  dirInThsh <- fs::path(DirIn,'threshold')
  fileThsh <- base::dir(dirInThsh)
  
  # Read thresholds (optimized error handling)
  if (base::length(fileThsh) > 1) {
    log$debug(base::paste0('threshold files are ', paste(fileThsh, collapse = ', ')))
    fileThsh <- fileThsh[1]
    log$info(base::paste0('Using first threshold file: ', fileThsh))
  }
  
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(
    NameFile = fs::path(dirInThsh, fileThsh)
  )
  
  ######## dummy threshold stuff here
  
  thsh$threshold_name[thsh$threshold_name == "Max SigmaTest"] <- "AzimuthLR"
  thsh$threshold_name[thsh$threshold_name == "StdDev multiplier for Min Sigma"] <- "LengthLR"
  thsh$threshold_name[thsh$threshold_name == "Max Sigma Test"] <- "Length_correctorLR"
  thsh$threshold_name[thsh$threshold_name == "StdDev multiplier for Max Sigma"] <- "AltitudeLR"
  thsh$threshold_name[thsh$threshold_name == "Delta Test window step - points"] <- "Obstruction_heightLR"

  ########
  
 #verify termTest exists, shadowSource exists and termTest is in thresholds. 
  if (is.null(termTest) || is.na(termTest)) {
    log$info("No terms to test for thresholds")
    stop()
  }
  
  if (is.null(shadowSource) || is.na(shadowSource)) {
    log$info("No shadowSource Provided")
    stop()
  }
  
  if (!any(termTest %in% thsh$term_name)) {
    log$error(base::paste0('Missing threshold term: ', termTest))
    stop()
  }
  
  #extract only term of interest/available in thresholds.
  
  # Extract thresholds using vectorized lookup
  thsh_subset <- thsh[thsh$term_name %in% termTest, ]
  
  threshold_lookup <- setNames(thsh_subset$number_value, thsh_subset$threshold_name)

  # Read location data
    dirInLoc <- fs::path(DirIn,'location')
    fileLoc <-base::dir(dirInLoc)
    
    if (length(fileLoc) == 0) {
      log$error(base::paste0('No location data in ', dirInLoc))
      stop()
    }
    
    #need locations file for lat/long
    if (any(grepl(fileLoc, pattern = "locations.json"))) {
      log$info(base::paste0('grabbing locations file metadata ', fileLoc[grepl(fileLoc, pattern = "locations.json")]))
      fileLoc <- fileLoc[grepl(fileLoc, pattern = "locations.json")]
    } else {
      log$error(base::paste0('No location data in cannot calculate shadows without lat/long ', dirInLoc))
      stop()
    }
    
    loc <-NEONprocIS.base::def.loc.geo.hist(NameFile = fs::path(dirInLoc, fileLoc))
    
    #checks on loc file to ensure we have just one lat/long. Since it's tower lat/long it should not change regardless of location history. 
    if (length(loc) > 1 ){
      log$debug('more than one CFGLOC loc.')
      cfgloc_item <- loc[[1]]  # Gets the first CFGLOC item regardless of name
      log$info(base::paste0('Using first loc file: ', names(loc[1])))
    } else if (length(loc) == 0 || is.null(loc) || is.na(loc)) {
      log$error('No location data available')
      stop()
    } else {
      cfgloc_item <- loc[[1]]  # Gets the first CFGLOC item regardless of name
    }
    
    # Extract lat/long from the first (and likely only) CFGLOC item
    lat_tow <- as.numeric(cfgloc_item[[1]]$reference_location[[1]]$LatTow)
    lon_tow <- as.numeric(cfgloc_item[[1]]$reference_location[[1]]$LonTow)
    
    #limit total options of shadow sources. Filter for expected. 
    shadow_sources <- c("LR", "Cimel", "Misc")
    shadow_sources <- intersect(shadow_sources, shadowSource) # Only those present

    #if no shadow source set flag to -1.
    if(is.null(shadow_sources) || all(is.na(shadow_sources))){
      log$info("No shadow sources, setting flag to -1")
      flagDf$shadowQF <- -1 
      return(flagDf)
    }
    
    dt_flag <- as.data.table(flagDf)
    dt_flag[, shadowQF := 0L]
    
    #NA handling for missing thresholds 
    get_threshold <- function(prefix) {
      key <- paste0(prefix, src)
      if (key %in% names(threshold_lookup)) {
        threshold_lookup[[key]]
      } else {
        NA
      }
    }
    
    #cycle through all shadow sources. If any are incomplete it skips to the next one and does not evaluate. 

    for(src in shadow_sources) {
      
      thresholds <- list(
        Azimuth = get_threshold("Azimuth"),
        Length = get_threshold("Length"),
        Length_corrector = get_threshold("Length_corrector"),
        Altitude = get_threshold("Altitude"),
        Height = get_threshold("Obstruction_height")
      )
      
      # Validate thresholds
      if (any(is.na(unlist(thresholds)))) {
        log$info(paste0("Missing threshold values for ", src, " skipping source."))
        next()
      }
      
      # Extract values
      az <- as.numeric(thresholds$Azimuth)
      len <- as.numeric(thresholds$Length)
      len_corrector <- as.numeric(thresholds$Length_corrector)
      alt <- as.numeric(thresholds$Altitude)
      height <- as.numeric(thresholds$Height)
      
      # Buffer logic (as above)
      #removing time threshold to shorten window. 
      # buffer <- case_when(
      #   len < 3 ~ 15,
      #   len >= 3 & len < 4 ~ 10,
      #   len >= 4 ~ 5,
      #   TRUE ~ NA_real_
      # )
      
      deg_buffer <- case_when(
        len < 3 ~ 5,
        len >= 3 ~ 4,
        TRUE ~ NA_real_
      )
      df_solar_pos <- data.frame(date = flagDf$readout_time)
      df_solar_pos$lat = lat_tow
      df_solar_pos$lon =  lon_tow
      #data frame by date
      #suncalc altitude 0 = horizon, 1.57 = directly over head
      #suncalc azimuth 0 = south 2.35 = NW
      df_solar_pos <- suncalc::getSunlightPosition(data = df_solar_pos)
      
      #convert to degrees and rotate so it lines up more logically. 
      df_solar_pos$deg_az <- round((df_solar_pos$azimuth*(180/pi)) + 180)
      df_solar_pos$deg_alt <- (df_solar_pos$altitude*(180/pi)) 
      
      rod_height <- height
      df_solar_pos[[paste0("shadow_length_", src)]] <- rod_height / tan(df_solar_pos$altitude)
      
      # Create adjusted azimuth ranges
      az_min <- (az - deg_buffer + 360) %% 360
      az_max <- (az + deg_buffer) %% 360
      
      if (az_min > az_max) {
        # Wraps around 0°/360° - need OR condition
        df_shadow <- df_solar_pos %>%
          filter(
            (deg_az >= az_min | deg_az <= az_max) &
              deg_alt >= alt &
              .data[[paste0("shadow_length_", src)]] >= len * len_corrector
          )
      } else {
        # Normal range - use AND condition  
        df_shadow <- df_solar_pos %>%
          filter(
            (deg_az >= az_min & deg_az <= az_max) &
              deg_alt >= alt &
              .data[[paste0("shadow_length_", src)]] >= len * len_corrector
          )
      }
      # Convert to data.table and create time windows
      # plus or minus one minute of flag catch. 
      dt_shadow <- as.data.table(df_shadow)
      dt_shadow[, `:=`(
        time_start = date - 60,
        time_end = date + 60
      )]
      
      # Sort by time_start
      setorder(dt_shadow, time_start)
      
      # Merge overlapping intervals
      dt_shadow[, group := cumsum(c(TRUE, time_start[-1] > shift(time_end)[-1]))]
      dt_shadow_merged <- dt_shadow[, .(
        time_start = min(time_start),
        time_end = max(time_end)
      ), by = group]
      
      # Now use the merged windows for flagging
      setkey(dt_shadow_merged, time_start, time_end)
      dt_flag[, shadowQF := shadowQF | (readout_time %inrange% list(dt_shadow_merged$time_start, dt_shadow_merged$time_end))]
      dt_flag[, shadowQF := as.integer(shadowQF)]
      
    }
    
  #convert back to DF
  flagDf <- as.data.frame(dt_flag)
  flagDf$shadowQF[is.na(flagDf$shadowQF)] <- -1
  
  return(flagDf)
}

