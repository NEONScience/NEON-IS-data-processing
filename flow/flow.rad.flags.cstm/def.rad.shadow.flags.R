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
                                 log = NULL){
  if(base::is.null(log)){
      log <- NEONprocIS.base::def.log.init()
    }   
  #use calculated azimuths to verify shading on one year of data, adjust as needed to finalize column for future threshold in pachy module
  library(magrittr)
  library(suncalc)
  library(lubridate)
  library(dplyr)
  
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
  
  thsh$threshold_name[thsh$threshold_name == "Max SigmaTest"] <- "Azimuth"
  thsh$threshold_name[thsh$threshold_name == "StdDev multiplier for Min Sigma"] <- "Length"
  thsh$threshold_name[thsh$threshold_name == "Max Sigma Test"] <- "Length_corrector"
  thsh$threshold_name[thsh$threshold_name == "StdDev multiplier for Max Sigma"] <- "Altitude"
  
  ########
  
  # Verify terms exist (vectorized)
  #TODO open up to more terms for SPN1/other sensors?
  #input parameter! tailor to be clear that it's in one parameter. 
  # look at stats flow termTest1 etc
  termTest <- "shortwaveRadiation"
  if (!termTest %in% thsh$term_name) {
    log$error(base::paste0('Missing threshold term: ', termTest))
    stop()
  }
  
  # Extract thresholds using vectorized lookup
  thsh_subset <- thsh[thsh$term_name == termTest,]
  
  threshold_lookup <- setNames(thsh_subset$number_value, thsh_subset$threshold_name)
  
  # Pre-allocate threshold variables
  
  #TODO to customize consider threshold names including the shading item in question eg LR, Cimel, NRSS project?
  thresholds <- list(
    Azimuth = threshold_lookup["Azimuth"],
    Length = threshold_lookup["Length"],
    Length_corrector = threshold_lookup["Length_corrector"],
    Altitude = threshold_lookup["Altitude"]
  )
  
  # Validate all thresholds exist
  if (any(is.na(thresholds))) {
    log$info("Missing threshold values")
    stop()
  }
  #azimuth, altitude, length, length corrector 
  
  az <- as.numeric(thresholds$Azimuth)
  len <- as.numeric(thresholds$Length)
  len_corrector <- as.numeric(thresholds$Length_corrector)
  alt <-as.numeric(thresholds$Altitude)
  
  #######TODO test on site with blanks to see if REALM fills in
  
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
  
  #create a time buffer and degree buffer for when the sensors are closer to the lightning rod so that the flagging persists longer
  
  buffer <- case_when(
    len < 3 ~ 15,
    len >= 3 & len < 4 ~ 10,
    len >= 4 ~ 5,
    TRUE ~ NA_real_  # default case
  )
  
  deg_buffer <-  case_when(
    len < 3 ~ 5,
    len >= 3 ~ 4,
    TRUE ~ NA_real_  # default case
  )
  
  #use readout_time of data to add lat/long and azimuth calculations . 
  
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
  
  ######## TODO? how to make more customizable? what if I want thresholds for cimel in the future? Or NRSS projects
  
  #2.6m higher than radiation pedestal
  df_solar_pos$shadow_length_LR <-  2.6/tan(df_solar_pos$altitude) #3m rod - ~0.4m spn1 height
  
  #consider buffer to be also programatic based on shadow length. 
  df_shadow<- df_solar_pos %>% dplyr::filter((deg_az >=az - deg_buffer & deg_az <= az + deg_buffer) & deg_alt >= alt & shadow_length_LR >= len*len_corrector) 
  
  # Check for proximity to df_shadow dates (Flag 1 potential)
  #buffer based on distance to sensor. 
  flagDf <- flagDf %>%
    mutate(
      # Create a temporary boolean for lightning shading from df_shadow
      shadowQF = sapply(readout_time, function(ts_time) {
        any(abs(difftime(ts_time, df_shadow$date, units = "mins")) <= buffer)
      })
    ) %>% 
    mutate(shadowQF = as.integer(shadowQF)) #TF as integer is 0/1
  
  return(flagDf)
  }
