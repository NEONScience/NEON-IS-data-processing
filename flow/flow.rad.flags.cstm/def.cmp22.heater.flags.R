##############################################################################################
#' @title Create custom heater flag from CMP22 heater streams
#' 
#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr
#' 
#' @description Definition function. Review heater streams of CMP22 sensor. If either stream is on flag 1, 
#' if both are off flag 0, if both are NA, flag -1 else flag -1. Used later for informational QM
#' 
#' @param data data frame or data.table of incoming CMP22 data. Must containt heater_1 or heater_2 columns.  
#' 
#' @param flagDf data frame input to append flags to reqs at least var readout_time
#'
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return data frame of cmp22 with appended column for heaterQF indicating that either heater was on. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' flagDf <- def.cmp22.heater.flags(data, flagDf, log)

#' @seealso Currently none

# changelog and author contributions / copyrights
#  Teresa Burlingame (2025-09-15)
#     Initial creation
##############################################################################################
def.cmp22.heater.flags <- function(data, 
                                   flagDf,
                                   log = NULL){
  
  
if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
} 
  
if(!"readout_time" %in% names(flagDf)){
  log$error("readout_time not in flagDF variable. Invalid configuration.")
  stop()
} 
  
#store names before transformation 
  original_flagDf_cols <- names(flagDf)
  
# if there are no heater streams add them in as NA
if(!('heater_1' %in% names(data))){
  data$heater_1 <- NA
  log$warn("Variable heater_1 not found in data. Appended to set as NA")
}
  
if(!('heater_2' %in% names(data))){
  data$heater_2 <- NA
  log$warn("Variable heater_2 not found in data. Appended to set as NA")
}


#initialize fields 
flagDf$heaterQF <- NA

#if either heater is on data is a 1, if both off 0, else -1
flagDf <- flagDf %>% 
  dplyr::left_join(data, by = "readout_time") %>% 
  dplyr::mutate(heaterQF = dplyr::case_when(heater_1 == 1 ~ 1, 
                                            heater_2 == 1 ~ 1,
                                            heater_1 ==0 & heater_2 == 0 ~ 0,
                                            is.na(heater_1) & is.na(heater_2) ~ -1, 
                                            TRUE ~ -1)) %>% 
  dplyr::select(dplyr::all_of(original_flagDf_cols), heaterQF)
return(flagDf)
}
