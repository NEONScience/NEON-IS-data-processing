##############################################################################################
#' @title  Process data from metone370380 tipping bucket sensors to 1 minute and 30 minute 
#' aggragations

#' @author
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr

#' @description Workflow. Add 0s to periods where no precipitation recorded, process Throughfall area conversion
#' (as applicable, location context based), process heater QMs, aggregate to 1 minute and apply 
#' extremePrecipFlag and finalQF, aggregate to 30 minutes, add uncertainty calculations.
#' 
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/location-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The location-id is the unique identifier of the location. \cr
#'
#' Nested within this path are (at a minimum) the folders:
#'         /data
#'         /flags
#'         /location
#'         /threshold
#'         /uncertainty_coeff


#' The flags folder holds one file containing calibration quality flags for 
#' the central processing day only. These files should respectively be named in the convention:
#' SOURCETYPE_LOCATIONID_YYYY-MM-DD_flagsCal.parquet
#' All other files in this directory will be ignored.
#'
#' The threshold folder contains a single file named thresholds.json that holds threshold parameters
#' applicable to the tipping buckets. 
#' 
#' The location folder holds location specific information so that throughfall specific area conversions can be applied
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param SchmData (Optional). A json-formatted character string containing the schema for the aggregated data, standard calibration and
#' custom plausibility QFs as well as the heaterQMs

#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data/flags/threshold folders in the input path that are to be copied with a 
#' symbolic link to the output path (i.e. carried through as-is). Note that the 'stats' directory
#' is automatically populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the precipitation and uncertainty estimates along with 
#' quality flags (incl. the final quality flag) produced by the custom tipping bucket algorithm, where DirOutBase 
#' replaces BASE_REPO of argument \code{DirIn} but otherwise retains the child directory structure of the 
#' input path. The terminal directories for each datum include, at a minimum, 'stats' and 'location'. The stats folder
#' contains one minute and 30-minute output for the relevant day of data. It contains the precipitation sum, uncertainty estimates, quality flags specific
#'to the bucket algorithm, and final quality flag. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn='/scratch/pfs/precipBucket_thresh_select'
#' DirOutBase='/scratch/pfs/out_tb'


#' wrap.precip.bucket(DirIn,DirOutBase,DirSubCopy)

#' @seealso Currently none

# changelog and author contributions / copyrights
#   Teresa Burlingame  (2025-06-10)
#     Initial creation
##############################################################################################
wrap.precip.bucket<- function(DirIn,
                              DirOutBase,
                              SchmData=NULL,
                              DirSubCopy=NULL,
                              log=NULL
){
  
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory and create the output directory.
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirInData <- fs::path(DirIn,'data')
  dirInQf <- fs::path(DirIn,'flags')
  dirInThsh <- fs::path(DirIn,'threshold')
  dirInLoc <- fs::path(DirIn,'location')
  dirUcrtCoef <- fs::path(DirIn,'uncertainty_coef')
  
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutStat<- fs::path(dirOut,'stats')
 
  
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = c('stats'),
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,c('stats')))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  
  #grab files for cal, data, thresholds
  fileData <- base::list.files(dirInData,pattern='.parquet',full.names=FALSE)
  fileQfCal <- base::list.files(dirInQf,pattern='flagsCal.parquet',full.names=FALSE)
  fileThsh <- base::list.files(dirInThsh,pattern='threshold',full.names=FALSE)

  # Read in the thresholds file (read first file only, there should only be 1)
  if(base::length(fileThsh) > 1){
    fileThsh <- fileThsh[1]
    log$info(base::paste0('There is more than one threshold file in ',dirInThsh,'. Using ',fileThsh))
  }
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df((NameFile=base::paste0(dirInThsh,'/',fileThsh)))
  
  # Verify that the term(s) needed in the input parameters are included in the threshold files
  termTest <- "precipBulk"
  exstThsh <- termTest %in% base::unique(thsh$term_name) # Do the terms exist in the thresholds
  if(base::sum(exstThsh) != base::length(termTest)){
    log$error(base::paste0('Thresholds for term(s): ',base::paste(termTest[!exstThsh],collapse=','),' do not exist in the thresholds file. Cannot proceed.')) 
    stop()
  }
  # Assign thresholds
  
  ###########TODO temporary sub in of names remove after testing is complete
  
  thsh$threshold_name[thsh$threshold_name == "Time dependent max range test value at point 1"]  <- "inactiveHeater"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max range test value at point 2"]  <- "baseHeater"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max soft range test at point 1"]  <- "extremePrecipQF"
  thsh$threshold_name[thsh$threshold_name == "Time dependent max soft range test at point 2"]  <- "funnelHeater"
  
  
  ######################
  
  # thresholds to variables
  thshIdxTerm <- thsh[thsh$term_name == termTest,]
  inactiveHeater <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "inactiveHeater"]
  baseHeater <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "baseHeater"]
  extremePrecipQF <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "extremePrecipQF"]
  funnelHeater <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == "funnelHeater"]

  #verify that the thresholds are all there
  ThshList <- c(inactiveHeater, baseHeater, extremePrecipQF, funnelHeater )
  
  if (length(ThshList) < nrow(thsh)) {
    log$error(base::paste0('Not all Thresholds specified for term(s): ',base::paste(termTest[!exstThsh],collapse=','),' do not exist in the thresholds file. Cannot proceed.')) 
    stop()
  }

  #read in location information 
  fileLoc <- base::dir(dirInLoc)
  
  ##### TODO chat with cove about logic of stop vs return()
  # If there is no location file, skip
  numFileLoc <- base::length(fileLoc)
  if(numFileLoc == 0){
    log$warn(base::paste0('No location data in ',dirInLoc,'. Skipping...'))
    return()
  }
  
  # If there is more than one location file, use the first
  if(numFileLoc > 1){
    log$warn(base::paste0('There is more than one location file in ',dirInLoc,'. Using the first... (',fileLoc[1],')'))
    fileLoc <- fileLoc[1]
  }
  
  # Load in the location json
  loc <- NEONprocIS.base::def.loc.meta(NameFile=base::paste0(dirInLoc,'/',fileLoc))
  
  # Read the datasets 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            RmvDupl=TRUE,
                                            Df=TRUE, 
                                            log=log)
  
  qfCal<- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInQf,fileQfCal),
                                                 VarTime='readout_time',
                                                 RmvDupl=TRUE,
                                                 Df=TRUE,
                                                 log=log)
  #combine into one file
  data <- full_join(data,qfCal, by = 'readout_time')
  #############
  
  # pull in UCRT file
  fileUcrt <- base::dir(dirUcrtCoef)
  
  if(base::length(fileUcrt) != 1){
    log$warn(base::paste0("There are either zero or more than one uncertainty coefficient files in path: ",dirUcrtCoef,"... Uncertainty coefs will not be read in. This is fine if the uncertainty function doesn't need it, but you should check..."))
    ucrtCoef <- base::list()
  } else {
    nameFileUcrt <- base::paste0(dirUcrtCoef,'/',fileUcrt) # Full path to file
    
  # Open the uncertainty file
  ucrtCoef  <- base::try(rjson::fromJSON(file=nameFileUcrt,simplify=TRUE),silent=FALSE)
  if(base::class(ucrtCoef) == 'try-error'){
    # Generate error and stop execution
    log$error(base::paste0('File: ', nameFileUcrt, ' is unreadable.')) 
    stop()
  }
  
  ucrtCoef_df <- dplyr::bind_rows(ucrtCoef)
  
  uCvalA1 <- as.numeric(ucrtCoef_df$Value[ucrtCoef_df$Name == "U_CVALA1"])
  
  if (length(uCvalA1) < 1) {
    # Generate error and stop execution
    log$error(base::paste0('Uncertainty value uCvalA1 necessary and not available')) 
    stop()
  }
  
  #Convert all NA values to 0
  data$precipitation[is.na(data$precipitation)] <- 0
  
  ########## TODO how to handle NAs in val cal etc? 
  data$validCalQF[is.na(data$validCalQF)] <- 0 
  data$suspectCalQF[is.na(data$suspectCalQF)] <- 0 
  
  #if throughfall downscale precip and UCRT based on area
  
  if (grepl(loc$context, pattern = "throughfall")){
    # convert throughfall values (based on context)
    tf_conv <- 32429/251400 #Area of the collector
    
    data$precipitation <- data$precipitation*tf_conv
    
    #use tip and area conversions to calculate uncertainties
    tipSingle <- max(data$precipitation)
    uThPTi <- uCvalA1 * tipSingle
    uAtPTi <- 0.01 * tipSingle #In ATBD and java code 
    data$combinedUcrt <- NA
    data$combinedUcrt[data$precipitation > 0] <- sqrt(uThPTi^2 + uAtPTi^2)
    log$debug(base::paste0('Throughfall sensor detected at ', loc$name, ' applying area conversion'))
  } else {
    data$combinedUcrt <- NA
    data$combinedUcrt <- uCvalA1
  }
  
  # Apply heater QMs If throughfall or non heatead should be NA
  
  data <- data %>%
    dplyr::mutate(
      precipHeaterQF = dplyr::case_when(
        heater_current < inactiveHeater ~ 0,                              
        heater_current >= inactiveHeater & heater_current < baseHeater ~ 1,         
        heater_current >= baseHeater & heater_current <= funnelHeater ~ 2,         
        heater_current > funnelHeater ~ 3,                               
        TRUE ~ NA #should handle a missing stream in TFs
      ) )
  
  
  # Aggregate to 1 minute and apply extremePrecip and heaterQF Flags
  
  stats_aggr01 <- data %>% 
    dplyr::mutate(startDateTime = lubridate::floor_date(readout_time, '1 minute')) %>%
    dplyr::mutate(endDateTime = lubridate::ceiling_date(readout_time, '1 minute', change_on_boundary = T)) %>%
    dplyr::group_by(startDateTime, endDateTime) %>%
    dplyr::summarise(
      precipBulk = base::sum(precipitation),
      precipExpUncert = sum(combinedUcrt, na.rm = T), 
      precipHeater0QM = base::round(length(which(precipHeaterQF == 0))/n() * 100, 0),
      precipHeater1QM = base::round(length(which(precipHeaterQF == 1))/n() * 100, 0),
      precipHeater2QM = base::round(length(which(precipHeaterQF == 2))/n() * 100, 0),
      precipHeater3QM = base::round(length(which(precipHeaterQF == 3))/n() * 100, 0),
      validCalQF = base::max(validCalQF, na.rm = T),
      suspectCalQF = base::max(validCalQF, na.rm = T)) %>%
    dplyr::mutate(extremePrecipQF = dplyr::case_when(
      precipBulk >= extremePrecipQF ~ 1,                              
      precipBulk < extremePrecipQF ~ 0,         
      TRUE ~ -1 ))%>%
    #flag data with finalQF if extreme precip observed.
    dplyr::mutate(finalQF = dplyr::case_when(
      extremePrecipQF == 1 ~ 1, 
      TRUE ~ 0)) %>% 
    dplyr::mutate(finalQF = as.integer(finalQF)) #make integer

  
  #aggregate one minute data further to 30 minute, taking the max calue for flags, the mean for QMs
  stats_aggr30 <- stats_aggr01 %>% 
    dplyr::mutate(startDateTime = lubridate::floor_date(startDateTime, '30 minute')) %>%
    dplyr::mutate(endDateTime = lubridate::ceiling_date(endDateTime, '30 minute')) %>% 
    dplyr::group_by(startDateTime, endDateTime) %>%
    dplyr::summarise(
      precipBulk = base::sum(precipBulk, na.rm = T),
      precipExpUncert = max(precipExpUncert, na.rm = T), #matches portal output
      precipHeater0QM = base::round(mean(precipHeater0QM, na.rm = T), 0),
      precipHeater1QM = base::round(mean(precipHeater1QM, na.rm = T), 0),
      precipHeater2QM = base::round(mean(precipHeater2QM, na.rm = T), 0),
      precipHeater3QM = base::round(mean(precipHeater3QM, na.rm = T), 0),
      validCalQF = base::max(validCalQF, na.rm = T),
      suspectCalQF = base::max(validCalQF, na.rm = T),
      extremePrecipQF = base::max(extremePrecipQF, na.rm = T),
      finalQF = as.integer(max(finalQF, na.rm = T))) 
  
### reorder data. 

  
# Write out the file for this aggregation interval. 
  
  nameFileIdx <- fileData
    if(!is.na(nameFileIdx)){
      
      # Append the center date to the end of the file name to know where it came from
      nameFileIdxSplt <- base::strsplit(nameFileIdx,'.',fixed=TRUE)[[1]]
      nameFileStatIdxSplt001<- c(paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt)-1)],
                                         '_stats_001'),
                                  utils::tail(nameFileIdxSplt,1))
      nameFileStatIdxSplt030 <- c(paste0(nameFileIdxSplt[1:(length(nameFileIdxSplt)-1)],
                                         '_stats_030'),
                                  utils::tail(nameFileIdxSplt,1))
      nameFileStatOut001Idx <- base::paste0(nameFileStatIdxSplt001,collapse='.')
      nameFileStatOut030Idx <- base::paste0(nameFileStatIdxSplt030,collapse='.')
      
      # Write out the data to file
      fileStatOut001Idx<- fs::path(dirOutStat,nameFileStatOut001Idx)
      fileStatOut030Idx <- fs::path(dirOutStat,nameFileStatOut030Idx)
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = stats_aggr01,
          NameFile = fileStatOut001Idx,
          NameFileSchm=NULL,
          Schm=SchmData,
          log=log
        ),
        silent = TRUE)
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          fileStatOut001Idx,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Wrote 1 min precipitation to file ',
          fileStatOut001Idx
        ))
      }
      
      rptWrte <-
        base::try(NEONprocIS.base::def.wrte.parq(
          data = stats_aggr30,
          NameFile = fileStatOut030Idx,
          NameFileSchm=NULL,
          Schm=SchmData,
          log=log
        ),
        silent = TRUE)
      if ('try-error' %in% base::class(rptWrte)) {
        log$error(base::paste0(
          'Cannot write output to ',
          fileStatOut030Idx,
          '. ',
          attr(rptWrte, "condition")
        ))
        stop()
      } else {
        log$info(base::paste0(
          'Wrote 30 min precipitation to file ',
          fileStatOut030Idx
        ))
      }
  
    }
  

  return()
} 


