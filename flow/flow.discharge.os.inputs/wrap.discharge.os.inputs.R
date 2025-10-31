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
# DirTables <- "/home/NEON/nickerson/pfs"
# DirIn <- "/home/NEON/nickerson/pfs/testing/2024/02/25/l4discharge_HOPB132100/data"
# DirOutBase <- "/home/NEON/nickerson/pfs/out"
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# wrap.discharge.os.inputs(DirTables=DirTables,
#                          DirIn=DirInBase,
#                          DirOutBase=DirOutBase,
#                          log=log)

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-10-31)
#     original creation
##############################################################################################
wrap.discharge.os.inputs <- function(DirTables,
                                     DirIn,
                                     DirOutBase,
                                     log=NULL
){
  
  library(stringr)
  
  # Set constants
  secInDay <- 60*60*24
  
  # Read in all OS table loader outputs
  dirList <- list.files(DirTables,pattern = "table_loader")
  tableNameMap <- list()
  for(d in 1:length(dirList)){
    fileName <- list.files(paste(DirTables,dirList[d],sep = "/"))
    filePath <- list.files(paste(DirTables,dirList[d],sep = "/"),
                           full.names = T)
    currFile <- read.csv(filePath,encoding = "UTF-8",header = T)
    assign(gsub("\\.csv$","",
                gsub("^.*\\.001\\.","",
                     fileName)),
           currFile)
    tableNameMap[[gsub("\\.csv$","",
                       gsub("^.*\\.001\\.","",
                            fileName))]] <- fileName
  }
  
  # For each subdirectory, write out the appropriate data
  
  # What is the date and site for the current directory?
  startDate <- as.POSIXct(stringr::str_extract(DirIn, "\\d{4}/\\d{2}/\\d{2}"),
                  tz="UTC")
  endDate <- startDate+secInDay
  siteID <- stringr::str_extract(DirIn, "(?<=l4discharge_)[A-Z]{4}")
  
  # Create the output directory
  DirOut <- gsub("^.*testing",DirOutBase,DirIn)
  dir.create(DirOut,recursive = T)
  
  # Which curveID(s) is/are active for this site*date?
  currCurveData <- sdrc_curveIdentification_pub[
    sdrc_curveIdentification_pub$siteID==siteID
    &((sdrc_curveIdentification_pub$curveStartDate<=startDate
       &sdrc_curveIdentification_pub$curveEndDate>=endDate)
      |(sdrc_curveIdentification_pub$curveStartDate<=startDate
        &sdrc_curveIdentification_pub$curveEndDate>=startDate)
      |(sdrc_curveIdentification_pub$curveStartDate<=endDate
        &sdrc_curveIdentification_pub$curveEndDate>=endDate)
    ),
  ]
  
  # If there are no active curve IDs, do not write out any data
  # If there are active curve IDs, write out data associated with active curve
  if(nrow(currCurveData)>0){
    # Write curveIdentification
    write.csv(currCurveData,
              paste(DirOut,
                    tableNameMap[["sdrc_curveIdentification_pub"]],
                    sep = "/"),
              row.names = F)
    # Write controls data associated with this curve
    surveyDate <- as.Date(currCurveData$controlSurveyEndDateTime,tz="UTC")
    write.csv(
      sdrc_controlInfo_pub[as.Date(sdrc_controlInfo_pub$endDate,
                                   tz="UTC",
                                   format="%Y-%m-%dT%H:%M:%SZ")
                           %in%surveyDate
                           &sdrc_controlInfo_pub$siteID==siteID,],
      paste(DirOut,
            tableNameMap[["sdrc_controlInfo_pub"]],
            sep = "/"),
      row.names = F)
    write.csv(
      sdrc_priorParameters_pub[as.Date(sdrc_priorParameters_pub$endDate,
                                   tz="UTC",
                                   format="%Y-%m-%dT%H:%M:%SZ")
                           %in%surveyDate
                           &sdrc_priorParameters_pub$siteID==siteID,],
      paste(DirOut,
            tableNameMap[["sdrc_priorParameters_pub"]],
            sep = "/"),
      row.names = F)
    # Write the rating curve data associated with this curveID
    curveID <- currCurveData$curveID
    write.csv(
      sdrc_stageDischargeCurveInfo_pub[sdrc_stageDischargeCurveInfo_pub$curveID
                                       %in%curveID,],
      paste(DirOut,
            tableNameMap[["sdrc_stageDischargeCurveInfo_pub"]],
            sep = "/"),
      row.names = F)
    write.csv(
      sdrc_gaugeDischargeMeas_pub[sdrc_gaugeDischargeMeas_pub$curveID
                                       %in%curveID,],
      paste(DirOut,
            tableNameMap[["sdrc_gaugeDischargeMeas_pub"]],
            sep = "/"),
      row.names = F)
    write.csv(
      sdrc_sampledParameters_pub[sdrc_sampledParameters_pub$curveID
                                  %in%curveID,],
      paste(DirOut,
            tableNameMap[["sdrc_sampledParameters_pub"]],
            sep = "/"),
      row.names = F)
  }
  
  # Which regressionID(s) is/are active for this site*date?
  currRegData <- csd_gaugeWaterColumnRegression_pub[
    csd_gaugeWaterColumnRegression_pub$siteID==siteID
    &((csd_gaugeWaterColumnRegression_pub$regressionStartDate<=startDate
       &csd_gaugeWaterColumnRegression_pub$regressionEndDate>=endDate)
      |(csd_gaugeWaterColumnRegression_pub$regressionStartDate<=startDate
        &csd_gaugeWaterColumnRegression_pub$regressionEndDate>=startDate)
      |(csd_gaugeWaterColumnRegression_pub$regressionStartDate<=endDate
        &csd_gaugeWaterColumnRegression_pub$regressionEndDate>=endDate)
    ),
  ]
  
  # If there are no active regressionIDs, do not write out any data
  # If there are active regressionIDs, write out data associated with active curve
  if(nrow(currRegData)>0){
    # Write gaugeWaterColumnRegression
    write.csv(currRegData,
              paste(DirOut,
                    tableNameMap[["csd_gaugeWaterColumnRegression_pub"]],
                    sep = "/"),
              row.names = F)
    # Write the gauge-pressure relationship data associated with this curveID
    regressionID <- currRegData$regressionID
    write.csv(
      sdrc_gaugePressureRelationship_pub[
        sdrc_gaugePressureRelationship_pub$regressionID
        %in%regressionID,],
      paste(DirOut,
            tableNameMap[["sdrc_gaugePressureRelationship_pub"]],
            sep = "/"),
      row.names = F)
  }
  return()
}# End wrap.discharge.os.inputs