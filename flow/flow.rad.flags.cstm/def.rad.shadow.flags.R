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
##############################################################################################
###### TODO look into making more efficient (does it need to be one sec, does it need to be TF then 0/1 etc)
#### Add a -1 scenario? After finished if val is not 0/1 make -1? 

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
  thsh$threshold_name[thsh$threshold_name == "Despiking Method"] <- "Obstruction_heightLR"

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
    
    shadow_sources <- c("LR", "Cimel", "Misc")
    shadow_sources <- intersect(shadow_sources, shadowSource) # Only those present
    
    dt_flag <- as.data.table(flagDf)
    dt_flag[, shadowQF := 0L]
    
    for(src in shadow_sources) {
      thresholds <- list(
        Azimuth = threshold_lookup[[paste0("Azimuth", src)]],
        Length = threshold_lookup[[paste0("Length", src)]],
        Length_corrector = threshold_lookup[[paste0("Length_corrector", src)]],
        Altitude = threshold_lookup[[paste0("Altitude", src)]],
        Height = threshold_lookup[[paste0("Obstruction_height", src)]]
      )
      
      # Validate thresholds
      if (any(is.na(unlist(thresholds)))) {
        log$error(paste0("Missing threshold values for ", src))
        stop()
      }
      
      # Extract values
      az <- as.numeric(thresholds$Azimuth)
      len <- as.numeric(thresholds$Length)
      len_corrector <- as.numeric(thresholds$Length_corrector)
      alt <- as.numeric(thresholds$Altitude)
      height <- as.numeric(thresholds$Height)
      
      # Buffer logic (as above)
      buffer <- case_when(
        len < 3 ~ 15,
        len >= 3 & len < 4 ~ 10,
        len >= 4 ~ 5,
        TRUE ~ NA_real_
      )
      
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
      
      # Filter for shadows for this source
      df_shadow <- df_solar_pos %>%
        filter(
          (deg_az >= az - deg_buffer & deg_az <= az + deg_buffer) &
            deg_alt >= alt &
            .data[[paste0("shadow_length_", src)]] >= len * len_corrector
        )
      
      # Convert to data.table and create time windows
      dt_shadow <- as.data.table(df_shadow)
      dt_shadow[, `:=`(
        time_start = date - buffer * 60,
        time_end = date + buffer * 60
      )]
      setkey(dt_shadow, time_start, time_end)
      
      # Overlap join - flag
      dt_flag[, shadowQF := shadowQF | as.integer(
        readout_time %inrange% list(dt_shadow$time_start, dt_shadow$time_end)
      )]
    }
    
  #convert back to DF
  flagDf <- as.data.frame(dt_flag)
  is.na(flagDf$shadowQF) <- -1
  
  return(flagDf)
}

