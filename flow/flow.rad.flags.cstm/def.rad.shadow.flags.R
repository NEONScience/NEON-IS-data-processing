#use calculated azimuths to verify shading on one year of data, adjust as needed to finalize column for future threshold in pachy module
library(magrittr)
library(suncalc)
library(lubridate)
library(dplyr)

files <- lapply(names(file_patterns), function(type) {
  base::list.files(dir_paths[[type]], pattern = file_patterns[[type]], full.names = FALSE)
})
names(files) <- names(file_patterns)

# Read thresholds (optimized error handling)
if (base::length(files$threshold) > 1) {
  log$debug(base::paste0('threshold files are ', paste(files$threshold, collapse = ', ')))
  files$threshold <- files$threshold[1]
  log$info(base::paste0('Using first threshold file: ', files$threshold))
}

thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df(
  NameFile = fs::path(dir_paths$threshold, files$threshold)
)

# Verify terms exist (vectorized)
termTest <- "precipBulk"
if (!termTest %in% thsh$term_name) {
  log$error(base::paste0('Missing threshold term: ', termTest))
  stop()
}

# Extract thresholds using vectorized lookup
thsh_subset <- thsh[thsh$term_name == termTest, ]
threshold_lookup <- setNames(thsh_subset$number_value, thsh_subset$threshold_name)

# Pre-allocate threshold variables
thresholds <- list(
  inactiveHeater = threshold_lookup["InactiveHeater"],
  baseHeater = threshold_lookup["BaseHeater"],
  extremePrecipQF = threshold_lookup["ExtremePrecipMax"],
  funnelHeater = threshold_lookup["FunnelHeater"]
)

# Validate all thresholds exist
if (any(is.na(thresholds))) {
  log$error("Missing threshold values")
  stop()
}

# Read location data
fileLoc <- base::dir(dir_paths$location)
if (length(fileLoc) == 0) {
  log$error(base::paste0('No location data in ', dir_paths$location))
  stop()
}

if (length(fileLoc) > 1) {
  log$warn(base::paste0('Multiple location files, using first: ', fileLoc[1]))
  fileLoc <- fileLoc[1]
}

loc <- NEONprocIS.base::def.loc.meta(NameFile = fs::path(dir_paths$location, fileLoc))

#Read in thresholds data

#read in location data for lat and long 

######################DUMMY Thresholds


####################### Delete after thresholds finalized

#azimuth, altitude, length, length corrector 
az <- meta_filt$corrected_az[meta_filt$Site == site]
alt_adj <- meta_filt$alt_adj[meta_filt$Site == site]
len_corrector <- meta_filt$len_corrector[meta_filt$Site == site] 
len <- (meta_filt$distance[meta_filt$Site == site] * len_corrector)

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

#input site lat/long from neon website
### get lat long from neon location file 
df$lat = meta_filt$lat[meta_filt$Site==site]
df$lon =  meta_filt$long[meta_filt$Site==site]

#data frame by date
#suncalc altitude 0 = horizon, 1.57 = directly over head
#suncalc azimuth 0 = south 2.35 = NW
df2 <- suncalc::getSunlightPosition(data = df)

#convert to degrees and rotate so it lines up more logically. 
df2$deg_az <- round((df2$azimuth*(180/pi)) + 180)
df2$deg_alt <- (df2$altitude*(180/pi)) 


########how to make more customizable? 

#2.6m higher than radiation pedestal
df2$shadow_length_lightning <-  2.6/tan(df2$altitude) #3m rod - ~0.4m spn1 height

#consider buffer to be also programatic based on shadow length. 
df_shadow<- df2 %>% dplyr::filter( (deg_az >=az - deg_buffer & deg_az <= az + deg_buffer) & deg_alt >= alt_adj & shadow_length_lightning >= len) 

#use NOAA calculator to verify - don't forget TZ conversions. 
# https://gml.noaa.gov/grad/solcalc/

spn1df <- spn1$SRDDP_1min

# Check for proximity to df_lightning dates (Flag 1 potential)
#buffer based on distance to sensor. 
data <- data %>%
  mutate(
    # Create a temporary boolean for lightning from df_lightning
    shadowQF = sapply(readout_time, function(ts_time) {
      any(abs(difftime(ts_time, df_shadow$date, units = "mins")) <= buffer)
    })
  )


