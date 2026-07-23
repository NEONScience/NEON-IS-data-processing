##############################################################################################
#' @title Wrapper for L4 Discharge OS input table processing

#' @author
#' Zachary Nickerson \email{nickerson@battelleecology.org}

#' @description Wrapper function. An OS input table for L4 discharge is read in
#' and parsed out by date and site for use in the main L4 discharge processing
#' module

#' @param DirInBase Character value. The input path that contains repositories
#' for OS tables.
#' 
#' @param TableName Character value. Partial name of the OS publication table. 
#' Must be one of the following: 
#' 
#' controlInfo
#' curveID
#' gaugeDsc
#' gaugePress
#' prior
#' sampled
#' stageDscCurve
#' 
#' @param DirOutBase Character value. The output path that will replace the 
#' #/pfs/BASE_REPO portion of DirInBase.

#' @return 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' Not run
# DirIn <- "/home/nickerson/pfs/testing/2025/10/01/l4discharge_ARIK102100"
# DirOutBase <- "/home/nickerson/pfs/out"
# dirInTables <- "/home/nickerson/pfs/l4discharge_os_table_loader"
# dirList <- list.files(dirInTables,full.names = F)
# tableNameMap <- list()
# for(d in 1:length(dirList)){
#   fileName <- dirList[d]
#   filePath <- list.files(dirInTables,
#                          pattern=fileName,
#                          full.names = T)
#   currFile <- read.csv(filePath,encoding = "UTF-8",header = T)
#   tableNameMap[[gsub("\\.csv$","",
#                      gsub("^.*\\.001\\.","",
#                           fileName))]] <- currFile
# }
# list2env(tableNameMap,envir = .GlobalEnv)
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# wrap.discharge.parse.os.inputs(
#   DirIn=DirIn,
#   csd_constantBiasShift_pub=csd_constantBiasShift_pub,
#   csd_dataGapToFillMethodMapping_pub=csd_dataGapToFillMethodMapping_pub,
#   csd_gapFillingRegression_pub=csd_gapFillingRegression_pub,
#   csd_gaugeWaterColumnRegression_pub=csd_gaugeWaterColumnRegression_pub,
#   sdrc_controlInfo_pub=sdrc_controlInfo_pub,
#   sdrc_curveIdentification_pub=sdrc_curveIdentification_pub,
#   sdrc_priorParameters_pub=sdrc_priorParameters_pub,
#   sdrc_gaugeDischargeMeas_pub=sdrc_gaugeDischargeMeas_pub,
#   sdrc_sampledParameters_pub=sdrc_sampledParameters_pub,
#   sdrc_gaugePressureRelationship_pub=sdrc_gaugePressureRelationship_pub,
#   sdrc_stageDischargeCurveInfo_pub=sdrc_stageDischargeCurveInfo_pub,
#   DirOutBase=DirOutBase,
#   log=log
# )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-10-31)
#     original creation
#   Nora Catolico(2025-11-18)
#     reorganized input directories and added error logging
#   Nora Catolico(2026-07-23)
#     updated dates to POSIX
##############################################################################################
wrap.discharge.parse.os.inputs <- function(DirIn,
                                           csd_constantBiasShift_pub,
                                           csd_dataGapToFillMethodMapping_pub,
                                           csd_gapFillingRegression_pub,
                                           csd_gaugeWaterColumnRegression_pub,
                                           sdrc_controlInfo_pub,
                                           sdrc_curveIdentification_pub,
                                           sdrc_priorParameters_pub,
                                           sdrc_gaugeDischargeMeas_pub,
                                           sdrc_sampledParameters_pub,
                                           sdrc_gaugePressureRelationship_pub,
                                           sdrc_stageDischargeCurveInfo_pub,
                                           DirOutBase,
                                           log=NULL
){
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Set constants
  secInDay <- 60*60*24
  
  # What is the date and site for the current directory?
  startDate <- as.POSIXct(stringr::str_extract(DirIn, "\\d{4}/\\d{2}/\\d{2}"),
                          tz="UTC")
  endDate <- startDate+secInDay
  site <- stringr::str_extract(DirIn, "(?<=l4discharge_)[A-Z]{4}")
  date <- format(startDate,"%Y/%m/%d") #for file naming
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirInData <- fs::path(DirIn,'data')
  
  # create output 
  DirOutData <- base::paste0(DirOutBase,InfoDirIn$dirRepo,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn),
                                     DirDest=base::paste0(DirOutBase,"/",date),
                                     LnkSubObj=TRUE,
                                     log=log) 
  
  # The flags folder is already populated from the calibration module. Copy over any existing files.
  fileCopy <- base::list.files(DirInData,recursive=TRUE) # Files to copy over
  # Symbolically link each file
  for(idxFileCopy in fileCopy){
    cmdCopy <- base::paste0('ln -s ',base::paste0(DirInData,'/',idxFileCopy),' ',base::paste0(DirOutData,'/',idxFileCopy))
    rptCopy <- base::system(cmdCopy)
  }
  
  # Which curveID(s) is/are active for this site*date?
  if(grepl("TOOK150",DirIn)){
    curveIDsite <- "TKIN"
  }else{
    if(grepl("TOOK160",DirIn)){
      curveIDsite <- "TKOT"
    }else{
      curveIDsite <- site
    }
  }
  currCurveData <- sdrc_curveIdentification_pub[
    grepl(paste0("^",curveIDsite,"\\."), sdrc_curveIdentification_pub$curveID)
    &((as.POSIXct(sdrc_curveIdentification_pub$curveStartDate,tz="UTC")<=startDate
       &as.POSIXct(sdrc_curveIdentification_pub$curveEndDate,tz="UTC")>=endDate)
      |(as.POSIXct(sdrc_curveIdentification_pub$curveStartDate,tz="UTC")<=startDate
        &as.POSIXct(sdrc_curveIdentification_pub$curveEndDate,tz="UTC")>=startDate)
      |(as.POSIXct(sdrc_curveIdentification_pub$curveStartDate,tz="UTC")<=endDate
        &as.POSIXct(sdrc_curveIdentification_pub$curveEndDate,tz="UTC")>=endDate)
    ),
  ]
  
  # If there are no active curve IDs, proceed with the max curve ID
  if(nrow(currCurveData)==0){
    log$info(paste0("No active curveID for ",site," on ",date,". Using the max curveID for this site."))
    currCurveData <- sdrc_curveIdentification_pub[
      grepl(paste0("^",curveIDsite,"\\."), sdrc_curveIdentification_pub$curveID)
      &sdrc_curveIdentification_pub$curveID==max(sdrc_curveIdentification_pub$curveID[grepl(paste0("^",curveIDsite,"\\."), sdrc_curveIdentification_pub$curveID)]),
    ]
  }

  # Write out data associated with active curve

  # Write curveIdentification
  write_currCurveData<-try(write.csv(currCurveData,
                                      paste(DirOutData,
                                            "NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv",
                                            sep = "/"),
                                      row.names = F))
  if(any(grepl('try-error',class(write_currCurveData)))){
    log$error(base::paste0('Writing the currCurveData output data failed: ',attr(write_currCurveData,"condition")))
    stop()
  } else {
    log$info("currCurveData data written out.")
  }
  
  # Write controls data associated with this curve
  surveyDate <- as.Date(currCurveData$controlSurveyEndDateTime,tz="UTC")
  if(grepl("TOOK150",DirIn)){
    surveyLoc <- "TOOK.AOS.discharge.inflow"
  }else{
    if(grepl("TOOK160",DirIn)){
      surveyLoc <- "TOOK.AOS.discharge.outflow"
    }else{
      surveyLoc <- paste0(site,".AOS.discharge")
    }
  }
  write_sdrc_controlInfo<-try(write.csv(
    sdrc_controlInfo_pub[as.Date(sdrc_controlInfo_pub$endDate,
                                  tz="UTC",
                                  format="%Y-%m-%dT%H:%M:%SZ")
                          %in%surveyDate
                          &sdrc_controlInfo_pub$namedLocation==surveyLoc,],
    paste(DirOutData,
          "NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv",
          sep = "/"),
    row.names = F))
  if(any(grepl('try-error',class(write_sdrc_controlInfo)))){
    log$error(base::paste0('Writing the sdrc_controlInfo output data failed: ',attr(write_sdrc_controlInfo,"condition")))
    stop()
  } else {
    log$info("sdrc_controlInfo data written out.")
  }
  
  write_sdrc_priorParameters<-try(write.csv(
    sdrc_priorParameters_pub[as.Date(sdrc_priorParameters_pub$endDate,
                                      tz="UTC",
                                      format="%Y-%m-%dT%H:%M:%SZ")
                              %in%surveyDate
                              &sdrc_priorParameters_pub$namedLocation==surveyLoc,],
    paste(DirOutData,
          "NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv",
          sep = "/"),
    row.names = F))
  if(any(grepl('try-error',class(write_sdrc_priorParameters)))){
    log$error(base::paste0('Writing the sdrc_priorParameters output data failed: ',attr(write_sdrc_priorParameters,"condition")))
    stop()
  } else {
    log$info("sdrc_priorParameters data written out.")
  }
  
  # Write the rating curve data associated with this curveID
  curveID <- currCurveData$curveID
  write_sdrc_stageDischargeCurveInfo<-try(write.csv(
    sdrc_stageDischargeCurveInfo_pub[sdrc_stageDischargeCurveInfo_pub$curveID
                                      %in%curveID,],
    paste(DirOutData,
          "NEON.DOM.SITE.DP4.00133.001.sdrc_stageDischargeCurveInfo_pub.csv",
          sep = "/"),
    row.names = F))
  if(any(grepl('try-error',class(write_sdrc_stageDischargeCurveInfo)))){
    log$error(base::paste0('Writing the sdrc_stageDischargeCurveInfo output data failed: ',attr(write_sdrc_stageDischargeCurveInfo,"condition")))
    stop()
  } else {
    log$info("sdrc_stageDischargeCurveInfo data written out.")
  }
  
  write_sdrc_gaugeDischargeMeas<-try(write.csv(
    sdrc_gaugeDischargeMeas_pub[sdrc_gaugeDischargeMeas_pub$curveID
                                %in%curveID,],
    paste(DirOutData,
          "NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.csv",
          sep = "/"),
    row.names = F))
  if(any(grepl('try-error',class(write_sdrc_gaugeDischargeMeas)))){
    log$error(base::paste0('Writing the sdrc_gaugeDischargeMeas output data failed: ',attr(write_sdrc_gaugeDischargeMeas,"condition")))
    stop()
  } else {
    log$info("sdrc_gaugeDischargeMeas data written out.")
  }
  
  write_sdrc_sampledParameters<-try(write.csv(
    sdrc_sampledParameters_pub[sdrc_sampledParameters_pub$curveID
                                %in%curveID,],
    paste(DirOutData,
          "NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.csv",
          sep = "/"),
    row.names = F))
  if(any(grepl('try-error',class(write_sdrc_sampledParameters)))){
    log$error(base::paste0('Writing the sdrc_sampledParameters output data failed: ',attr(write_sdrc_sampledParameters,"condition")))
    stop()
  } else {
    log$info("sdrc_sampledParameters data written out.")
  }
  
  # Which regressionID(s) is/are active for this site*date?
  if(grepl("TOOK150",DirIn)){
    regIDsite <- "TKIN"
  }else{
    if(grepl("TOOK160",DirIn)){
      regIDsite <- "TKOT"
    }else{
      regIDsite <- site
    }
  }
  currRegData <- csd_gaugeWaterColumnRegression_pub[
    grepl(paste0("^",regIDsite,"\\."), csd_gaugeWaterColumnRegression_pub$regressionID)
    &((as.POSIXct(csd_gaugeWaterColumnRegression_pub$regressionStartDate,tz="UTC")<=startDate
       &as.POSIXct(csd_gaugeWaterColumnRegression_pub$regressionEndDate,tz="UTC")>=endDate)
      |(as.POSIXct(csd_gaugeWaterColumnRegression_pub$regressionStartDate,tz="UTC")<=startDate
        &as.POSIXct(csd_gaugeWaterColumnRegression_pub$regressionEndDate,tz="UTC")>=startDate)
      |(as.POSIXct(csd_gaugeWaterColumnRegression_pub$regressionStartDate,tz="UTC")<=endDate
        &as.POSIXct(csd_gaugeWaterColumnRegression_pub$regressionEndDate,tz="UTC")>=endDate)
    ),
  ]
  
  # If there are no active regressionIDs, proceed with the max regressionID
  if(nrow(currRegData)==0){
    log$info(paste0("No active regressionID for ",site," on ",date,". Using the max regressionID for this site."))
    currRegData <- csd_gaugeWaterColumnRegression_pub[
      grepl(paste0("^",regIDsite,"\\."), csd_gaugeWaterColumnRegression_pub$regressionID)
      &csd_gaugeWaterColumnRegression_pub$regressionID==max(csd_gaugeWaterColumnRegression_pub$regressionID[grepl(paste0("^",regIDsite,"\\."), csd_gaugeWaterColumnRegression_pub$regressionID)]),
    ]
  }

  # Write out data associated with active regression

  # Write gaugeWaterColumnRegression
  write_currRegData<-try(write.csv(currRegData,
                                    paste(DirOutData,
                                          "NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv",
                                          sep = "/"),
                                    row.names = F))
  if(any(grepl('try-error',class(write_currRegData)))){
    log$error(base::paste0('Writing the currRegData output data failed: ',attr(write_currRegData,"condition")))
    stop()
  } else {
    log$info("currRegData data written out.")
  }
  
  # Write the gauge-pressure relationship data associated with this curveID
  regressionID <- currRegData$regressionID
  sdrc_gaugePressureRelationship<-try(write.csv(
    sdrc_gaugePressureRelationship_pub[
      sdrc_gaugePressureRelationship_pub$regressionID
      %in%regressionID,],
    paste(DirOutData,
          "NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.csv",
          sep = "/"),
    row.names = F))
  if(any(grepl('try-error',class(sdrc_gaugePressureRelationship)))){
    log$error(base::paste0('Writing the gaugePressureRelationship output data failed: ',attr(sdrc_gaugePressureRelationship,"condition")))
    stop()
  } else {
    log$info("gaugePressureRelationship data written out.")
  }
  
  # Are there any correction-related tables that need to be published on this site*day?
  # Are there any gaps that end on this day?
  currGapData <- csd_dataGapToFillMethodMapping_pub[
   csd_dataGapToFillMethodMapping_pub$namedLocation==surveyLoc
   &as.Date(csd_dataGapToFillMethodMapping_pub$endDate,tz="UTC",format="%Y-%m-%dT%H:%M:%SZ")==endDate
  ,]
  if(nrow(currGapData)>0){
    # Write dataGapToFillMethodMapping
    write_currGapData<-try(write.csv(currGapData,
                                     paste(DirOutData,
                                           "NEON.DOM.SITE.DP1.00133.001.csd_dataGapToFillMethodMapping_pub.csv",
                                           sep = "/"),
                                     row.names = F))
    if(any(grepl('try-error',class(write_currGapData)))){
      log$error(base::paste0('Writing the currGapData output data failed: ',attr(write_currGapData,"condition")))
      stop()
    } else {
      log$info("currGapData data written out.")
    }
  }else{
    log$info("No records to write out for csd_dataGapToFillMethodMapping_pub.")
  }
  # Are there any constant bias shifts that end on this day?
  currShiftData <- csd_constantBiasShift_pub[
    csd_constantBiasShift_pub$namedLocation==surveyLoc
    &as.Date(csd_constantBiasShift_pub$endDate,tz="UTC",format="%Y-%m-%dT%H:%M:%SZ")==endDate
    ,]
  if(nrow(currShiftData)>0){
    # Write constantBiasShift
    write_currShiftData<-try(write.csv(currShiftData,
                                     paste(DirOutData,
                                           "NEON.DOM.SITE.DP1.00133.001.csd_constantBiasShift_pub.csv",
                                           sep = "/"),
                                     row.names = F))
    if(any(grepl('try-error',class(write_currShiftData)))){
      log$error(base::paste0('Writing the currShiftData output data failed: ',attr(write_currShiftData,"condition")))
      stop()
    } else {
      log$info("currShiftData data written out.")
    }
  }else{
    log$info("No records to write out for csd_constantBiasShift_pub")
  }
  # Are there any gap filling regressions that end on this day?
  currGapRegData <- csd_gapFillingRegression_pub[
    csd_gapFillingRegression_pub$namedLocation==surveyLoc
    &as.Date(csd_gapFillingRegression_pub$endDate,tz="UTC",format="%Y-%m-%dT%H:%M:%SZ")==endDate
    ,]
  if(nrow(currGapRegData)>0){
    # Write gapFillingRegression
    write_currGapRegData<-try(write.csv(currGapRegData,
                                     paste(DirOutData,
                                           "NEON.DOM.SITE.DP1.00133.001.csd_gapFillingRegression_pub.csv",
                                           sep = "/"),
                                     row.names = F))
    if(any(grepl('try-error',class(write_currGapRegData)))){
      log$error(base::paste0('Writing the currGapRegData output data failed: ',attr(write_currGapRegData,"condition")))
      stop()
    } else {
      log$info("currGapRegData data written out.")
    }
  }else{
    log$info("No records to write out for csd_gapFillingRegression_pub")
  }
  
  return()
}# End wrap.discharge.os.inputs