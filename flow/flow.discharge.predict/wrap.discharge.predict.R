##############################################################################################
#' @title Model 1-Min Water Column Height to 15-Min Discharge Data

#' @author
#' Zachary Nickerson \email{nickerson@battelleecology.org}

#' @description Wrapper function. Average water column height data to 15-min; 
#' model continuous stage based on the relationship between gauge height and 
#' water column height; model continuous discharge using a 3rd party Bayesian 
#' model executable.

#' @param DirInWch Character value. The input path to the data from a single 
#' source ID, structured as follows:
#' #/pfs/surfacewaterPhysical_level1_group_consolidate_srf/yyyy/mm/dd/source-id/#,
#' where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 
#' 'yyyy/mm/dd' structure which indicates the 4-digit year, 2-digit month, and
#' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within this path are the folders:
#'         /data
#'         /location
#'         /flags
#'         
#' @param DirInReg Character value. The input path to data for the L1 OS table 
#' csd_gaugeWaterColumnRegression_pub, structured as follows:
#' #/pfs/l4discharge_csd_gag_regression_table_loader/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 
#' 'pfs'. \cr 
#'
#' Nested within this path is the file:
#'         /NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub
#' 
#' @param DirInRel Character value. The input path to data for the L1 OS table 
#' csd_gaugePressureRelationship_pub, structured as follows:
#' #/pfs/l4discharge_sdrc_gaugePress_table_loader/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 
#' 'pfs'. \cr 
#'
#' Nested within this path is the file:
#'         /NEON.DOM.SITE.DP4.00133.001.csd_gaugePressureRelationship_pub        
#'
#' @param DirInCrv Character value. The input path to data for the L1 OS table 
#' sdrc_curveIdentification_pub, structured as follows:
#' #/pfs/l4discharge_sdrc_curveID_table_loader/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 
#' 'pfs'. \cr 
#'
#' Nested within this path is the file:
#'         /NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub  
#'
#' @param DirInCtr Character value. The input path to data for the L1 OS table 
#' sdrc_controlInfo_pub, structured as follows:
#' #/pfs/l4discharge_sdrc_controlInfo_table_loader/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 
#' 'pfs'. \cr 
#'
#' Nested within this path is the file:
#'         /NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub
#'
#' @param DirInPri Character value. The input path to data for the L1 OS table 
#' sdrc_priorParameters_pub, structured as follows:
#' #/pfs/l4discharge_sdrc_prior_table_loader/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 
#' 'pfs'. \cr 
#'
#' Nested within this path is the file:
#'         /NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.
#'
#' @param DirInGag Character value. The input path to data for the L1 OS table 
#' sdrc_gaugeDischargeMeas_pub, structured as follows:
#' #/pfs/l4discharge_sdrc_gaugeDsc_table_loader/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 
#' 'pfs'. \cr 
#'
#' Nested within this path is the file:
#'         /NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.
#'
#' @param DirInSpg Character value. The input path to data for the L1 OS table 
#' sdrc_sampledParameters_pub, structured as follows:
#' #/pfs/l4discharge_sdrc_sampled_table_loader/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 
#' 'pfs'. \cr 
#'
#' Nested within this path is the file:
#'         /NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.
#'         
#' @param DirOutBase Character value. The output path that will replace the 
#' #/pfs/BASE_REPO portion of DirInWch.

#' @return A repository in DirOutBase containing the merged and filtered Kafka
#' output, where DirOutBase replaces BASE_REPO of argument \code{DirInWch} but 
#' otherwise retains the child directory structure of the input path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-09-18)
#     original creation
##############################################################################################
wrap.troll.uncertainty <- function(DirInWch=NULL,
                                   DirInReg=NULL,
                                   DirInRel=NULL,
                                   DirInCrv=NULL,
                                   DirInCtr=NULL,
                                   DirInPri=NULL,
                                   DirInGag=NULL,
                                   DirInSpg=NULL,
                                   DirOutBase
){

  library(NEONprocIS.base)
  library(stageQCurve)
  library(tidyverse)
  
  dataDir <- "/home/NEON/nickerson/pfs/" # devissom
  # dataDir <- "C:/Users/nickerson/Documents/l4discharge_pach_dev/" # zn local
  
  # TESTING DEVELOPMENT - ARIK 2025-08-25
  siteID <- "HOPB"
  qHOR <- "132"
  qVER <- "100"
  yyyy <- "2025"
  mm <- "08"
  dd <- "25"
  
  # Set constants
  secInDay <- 60*60*24
  secIn15min <- 60*15
  startDate <- as.POSIXct(paste(yyyy,mm,dd,sep = "-"),tz="UTC")
  endDate <- as.POSIXct(paste(yyyy,mm,dd,sep = "-"),tz="UTC")+secInDay-secIn15min
  
  # Set pasths and parse configuration files ####
  dirBaM <- "modules/l4_discharge_model/BaM_beta/" # devissom
  # dirBaM <- "C:/Users/nickerson/Documents/GitHub/NEON-hydro-clean-fill-proc/shiny-cleanFlow/BaM_beta/" # zn local
  dirConfig <- paste0(dirBaM,"BaM_BaRatin/")
  nameRegex <- "^\"|\" .*"
  configRegex <- "^\'|\' .*"
  predRegex <- "[0-9]{1,}"
  Config_BaM <- readLines(paste0(dirBaM,"Config_BaM.txt"))
  RunOptionsName <- gsub(nameRegex,"",
                         Config_BaM[2])
  ModelName <- gsub(nameRegex,"",
                    Config_BaM[3])
  ControlMatrixName <- gsub(nameRegex,"",
                            Config_BaM[4])
  DataName <- gsub(nameRegex,"",
                   Config_BaM[5])
  cookedMCMCName <- gsub(nameRegex,"",
                         readLines(paste0(dirConfig,
                                          gsub(nameRegex,"",
                                               Config_BaM[8])))[1])
  PredMasterName <- gsub(nameRegex,"",
                         Config_BaM[11])
  ConfigPredictions <- readLines(paste0(dirConfig,PredMasterName))
  Config_Pred_Maxpost <- readLines(paste0(dirConfig,
                                          gsub(configRegex,"",
                                               ConfigPredictions[2])))
  Config_Pred_hU <- readLines(paste0(dirConfig,
                                     gsub(configRegex,"",
                                          ConfigPredictions[3])))
  Config_Pred_TotalU <- readLines(paste0(dirConfig,
                                         gsub(configRegex,"",
                                              ConfigPredictions[4])))
  stageSeries <- gsub(configRegex,"",
                      Config_Pred_Maxpost[1])
  stageSpaghettis <- gsub(configRegex,"",
                          Config_Pred_hU[1])
  QMaxpostSpagName <- gsub(configRegex,"",
                           Config_Pred_Maxpost[7])
  QGaugeUncSpagName <- gsub(configRegex,"",
                            Config_Pred_hU[7])
  QGaugeUncEnvName <- gsub(configRegex,"",
                           Config_Pred_hU[10])
  QTotalUncSpagName <- gsub(configRegex,"",
                            Config_Pred_TotalU[7])
  QTotalUncEnvName <- gsub(configRegex,"",
                           Config_Pred_TotalU[10])
  dataPath <- gsub(configRegex,"",
                   readLines(paste0(dirConfig,DataName))[1])
  
  # Read in the WCH data - stashed locally from pachctl query ####
  dirHOR <- paste0("surfacewater-physical_",siteID,qHOR,qVER)
  wchFile <- paste(dirHOR,
                   paste(yyyy,mm,dd,sep = "-"),
                   "EOS_1_min_001.parquet",
                   sep = "_")
  EOS_1_min <- NEONprocIS.base::def.read.parq(
    paste(dataDir,
          "surfacewaterPhysical_level1_group_consolidate_srf",
          yyyy,mm,dd,
          dirHOR,
          "data",
          wchFile,
          sep = "/"),
  )
  
  # Average to 15 minute WCH data ####
  EOS_1_min_sum <- EOS_1_min%>%
    dplyr::mutate(roundDate=lubridate::round_date(endDateTime,"15 mins"))%>%
    dplyr::group_by(roundDate)%>%
    dplyr::summarise(wchMean=mean(surfacewaterColumnHeight,
                                  na.rm=T),
                     wchNumPts=sum(!is.na(surfacewaterColumnHeight)),
                     wchNonSysUncert=mean(surfacewaterColumnNonSysUncert,
                                          na.rm=T),
                     wchFinalQF=sum(surfacewaterColumnHeightFinalQF,
                                    na.rm=T)/n()*100,
                     wchFinalQFSciRvw=sum(surfacewaterColumnHeightFinalQFSciRvw,
                                          na.rm=T)/n()*100
    )%>%
    dplyr::mutate(dplyr::across(dplyr::everything(), ~ replace(., is.nan(.), NA)))
  
  # Determine if modeling with a current or previous regression ####
  
  # Read in the gaugeWaterColumnRegression data - stashed local from pachctl query
  gaugeWaterColumnRegression <- read.csv(
    paste0(dataDir,"l4discharge_csd_gag_regression_table_loader/NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv"),
    header = TRUE,
    encoding = "UTF-8"
  )
  # Check if there is a regression available 
  regAvailable <- any(gaugeWaterColumnRegression$siteID==siteID
                      &((gaugeWaterColumnRegression$regressionStartDate<=startDate
                         &gaugeWaterColumnRegression$regressionEndDate>=endDate)
                        |(gaugeWaterColumnRegression$regressionStartDate<=startDate
                          &gaugeWaterColumnRegression$regressionEndDate>=startDate)
                        |(gaugeWaterColumnRegression$regressionStartDate<=endDate
                          &gaugeWaterColumnRegression$regressionEndDate>=endDate)
                        )
                      )
  if(regAvailable){
    # If yes, subset to that regression
    gaugeWaterColumnRegression <- gaugeWaterColumnRegression[
      gaugeWaterColumnRegression$siteID==siteID
      &((gaugeWaterColumnRegression$regressionStartDate<=startDate
         &gaugeWaterColumnRegression$regressionEndDate>=endDate)
        |(gaugeWaterColumnRegression$regressionStartDate<=startDate
          &gaugeWaterColumnRegression$regressionEndDate>=startDate)
        |(gaugeWaterColumnRegression$regressionStartDate<=endDate
          &gaugeWaterColumnRegression$regressionEndDate>=endDate)
      ),
    ]
  }else{
    # If no, subset to most recent regression
    prevReg <- max(gaugeWaterColumnRegression$regressionID[
      gaugeWaterColumnRegression$siteID==siteID
    ])
    gaugeWaterColumnRegression <- gaugeWaterColumnRegression[
      gaugeWaterColumnRegression$regressionID==prevReg,
    ]
  }
  gaugeWaterColumnRegression$regressionStartDate <- as.POSIXct(gaugeWaterColumnRegression$regressionStartDate,
                                                               tz="UTC")
  gaugeWaterColumnRegression$regressionEndDate <- as.POSIXct(gaugeWaterColumnRegression$regressionEndDate,
                                                             tz="UTC")
  regID <- gaugeWaterColumnRegression$regressionID
  
  EOS_1_min_sum$regressionID <- NA
  EOS_1_min_sum$calcStage <- NA
  EOS_1_min_sum$stageUnc <- NA
  for(r in 1:length(regID)){
    currReg <- gaugeWaterColumnRegression[r,]
    # Model stage and estimate systematic uncertainty ####
  
    # Read in the curveIdentification data - stashed locally from pachctl query
    gaugePressureRelationship <- read.csv(
      paste0(dataDir,"l4discharge_sdrc_gaugePress_table_loader/NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.csv"),
      header = TRUE,
      encoding = "UTF-8"
    )
    gaugePress <- gaugePressureRelationship[
      gaugePressureRelationship$regressionID==currReg$regressionID,
    ]
    gaugePress$endDate <- as.POSIXct(gaugePress$endDate,
                                     tz="UTC",
                                     format="%Y-%m-%dT%H:%M:%SZ")
    
    # Predict model fit between stage and water column height
    gaugePress_model <- lm(gaugeHeight~calcWaterColumnHeight,
                           data=gaugePress)
    if(regAvailable){
      xsub <- which(EOS_1_min_sum$roundDate>=currReg$regressionStartDate
                    &EOS_1_min_sum$roundDate<=currReg$regressionEndDate)
    }else{
      xsub <- seq(1:nrow(EOS_1_min_sum))
    }
    EOS_1_min_sum$regressionID[xsub] <- currReg$regressionID
    gaugePress_model_fit <- data.frame(
      stats::predict(
        gaugePress_model,
        newdata = data.frame(calcWaterColumnHeight=EOS_1_min_sum$wchMean[xsub]),
        interval = "confidence"))
    gaugePress_model_fit$unc <- gaugePress_model_fit$upr-gaugePress_model_fit$fit
    
    # Add modeled stage to data
    EOS_1_min_sum$calcStage[xsub] <- gaugePress_model_fit$fit
    
    # Sum systematic and nonsystematic uncertainty and add to data
    EOS_1_min_sum$stageUnc[xsub] <- EOS_1_min_sum$wchNonSysUncert[xsub]+gaugePress_model_fit$unc
  }
  
  # Add discharge fields to data ####
  EOS_1_min_sum$curveID <- NA
  EOS_1_min_sum$csd <- NA
  EOS_1_min_sum$uprPara <- NA
  EOS_1_min_sum$lwrPara <- NA
  EOS_1_min_sum$uprRemn <- NA
  EOS_1_min_sum$lwrRemn <- NA
  
  # Determine if modeling with a current or previous rating curve ####
  
  # Read in the curveIdentification data - stashed locally from pachctl query
  curveIdentification <- read.csv(
    paste0(dataDir,"l4discharge_sdrc_curveID_table_loader/NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv"),
    header = TRUE,
    encoding = "UTF-8"
  )
  # Check if there is a curve available 
  curveAvailable <- any(curveIdentification$siteID==siteID
                        &((curveIdentification$curveStartDate<=startDate
                           &curveIdentification$curveEndDate>=endDate)
                          |(curveIdentification$curveStartDate<=startDate
                            &curveIdentification$curveEndDate>=startDate)
                          |(curveIdentification$curveStartDate<=endDate
                            &curveIdentification$curveEndDate>=endDate)
                          )
                        )
  if(curveAvailable){
    # If yes, subset to that curve
    curveIdentification <- curveIdentification[
      curveIdentification$siteID==siteID
      &((curveIdentification$curveStartDate<=startDate
         &curveIdentification$curveEndDate>=endDate)
        |(curveIdentification$curveStartDate<=startDate
          &curveIdentification$curveEndDate>=startDate)
        |(curveIdentification$curveStartDate<=endDate
          &curveIdentification$curveEndDate>=endDate)
      ),
    ]
  }else{
    # If no, subset to most recent curve
    prevCurve <- max(curveIdentification$curveID[
      curveIdentification$siteID==siteID
    ])
    curveIdentification <- curveIdentification[
      curveIdentification$curveID==prevCurve,
    ]
  }
  curveIdentification$curveStartDate <- as.POSIXct(curveIdentification$curveStartDate,
                                                   tz="UTC")
  curveIdentification$curveEndDate <- as.POSIXct(curveIdentification$curveEndDate,
                                                 tz="UTC")
  curveID <- curveIdentification$curveID
  
  for(c in 1:length(curveID)){
    currCurve <- curveIdentification[c,]
    # Configure priors for BaM! predictive model ####
    
    # Read in the controlInfo data - stashed locally from pachctl query
    surveyDate <- as.Date(currCurve$controlSurveyEndDateTime)
    controlInfo <- read.csv(
      paste0(dataDir,"l4discharge_sdrc_controlInfo_table_loader/NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv"),
      header = TRUE,
      encoding = "UTF-8"
    )
    controlInfo <- controlInfo[
      controlInfo$siteID==siteID
      &as.Date(controlInfo$endDate)==surveyDate,
    ]
    # Read in the priorParameters data - stashed locally from pachctl query
    priorParameters <- read.csv(
      paste0(dataDir,"l4discharge_sdrc_prior_table_loader/NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv"),
      header = TRUE,
      encoding = "UTF-8"
    )
    priorParameters <- priorParameters[
      priorParameters$siteID==siteID
      &as.Date(priorParameters$endDate)==surveyDate,
    ]
    
    # Configure model run
    stageQCurve::txt.out.run.opts(runType = "pred", 
                                  RunOptionsPath = paste0(dirConfig, 
                                                          RunOptionsName))
    
    # Write out control activation state
    controlMatrixPath <- paste0(dirConfig, ControlMatrixName)
    priorParamsPath <- paste0(dirConfig, ModelName)
    numCtrls <- nrow(priorParameters)
    Config_ControlMatrix <- matrix(data=NA, nrow = numCtrls, ncol = numCtrls)
    for(rw in 1:numCtrls){
      for(cl in 1:numCtrls){
        Config_ControlMatrix[rw,cl] <- controlInfo$controlActivationState[
          controlInfo$controlNumber == cl & controlInfo$segmentNumber == rw
        ]
      }
    }
    write.table(Config_ControlMatrix, controlMatrixPath, 
                row.names = F, col.names = F)
    
    # Write out hydraulic control configurations
    Config_Model <- matrix(data = NA, nrow = (4 + 12*numCtrls))
    Config_Model[1] <- '"BaRatin"'
    Config_Model[2:3] <- 1
    Config_Model[4] <- 3 * numCtrls
    for(j in 1:numCtrls){
      offset <- (j-1)*12
      #Divide by two and round to three places after the decimal
      kUnc <- format(
        priorParameters$priorActivationStageUnc[
          priorParameters$controlNumber == j
        ]/1.96, 
        digits = 3)
      aUnc <- format(
        priorParameters$priorCoefficientUnc[
          priorParameters$controlNumber == j
        ]/1.96,
        digits = 3)
      cUnc <- format(priorParameters$priorExponentUnc[
        priorParameters$controlNumber == j
      ]/1.96,
      digits = 3)
      Config_Model[offset+5] <- paste0('"k', j, '"')
      Config_Model[offset+6] <- priorParameters$priorActivationStage[
        priorParameters$controlNumber == j
      ]
      Config_Model[offset+7] <- "'Gaussian'"
      Config_Model[offset+8] <- paste(
        priorParameters$priorActivationStage[priorParameters$controlNumber == j],
        as.character(kUnc),
        sep = ",")
      Config_Model[offset+9] <- paste0('"a', j, '"')
      Config_Model[offset+10] <- priorParameters$priorCoefficient[
        priorParameters$controlNumber == j
      ]
      Config_Model[offset+11] <- "'Gaussian'"
      Config_Model[offset+12] <- paste(
        priorParameters$priorCoefficient[priorParameters$controlNumber == j],
        as.character(aUnc),
        sep = ",")
      Config_Model[offset+13] <- paste0('"c', j, '"')
      Config_Model[offset+14] <- priorParameters$priorExponent[
        priorParameters$controlNumber == j
      ]
      Config_Model[offset+15] <- "'Gaussian'"
      Config_Model[offset+16] <- paste(
        priorParameters$priorExponent[priorParameters$controlNumber == j],
        as.character(cUnc),
        sep = ",")
    }
    write.table(Config_Model, priorParamsPath, row.names = F, col.names = F, quote = F)
    
    # Configure gaugings for BaM! predictive model ####
    
    # Read in the priorParameters data - stashed locally from pachctl query
    gaugeDischargeMeas <- read.csv(
      paste0(dataDir,"l4discharge_sdrc_gaugeDsc_table_loader/NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.csv"),
      header = TRUE,
      encoding = "UTF-8"
    )
    gaugeDischargeMeas <- gaugeDischargeMeas[
      gaugeDischargeMeas$curveID==currCurve$curveID,
    ]
    
    # Reconfigure the guagings data table
    gaugeDischargeMeas <- gaugeDischargeMeas[
      ,c("gaugeHeight","gaugeHeightUnc","streamDischarge","streamDischargeUnc")
    ]
    names(gaugeDischargeMeas) <- c("H","uH","Q","uQ")
    gaugeDischargeMeas$bH <- 0.00
    gaugeDischargeMeas$bHindx <- 0.00
    gaugeDischargeMeas$bQ <- 0.00
    gaugeDischargeMeas$bQindx <- 0.00
    gaugeDischargeMeas$uQ <- gaugeDischargeMeas$uQ*1.96
    gagNam <- c('H','uH','bH','bHindx','Q','uQ','bQ','bQindx')
    gaugeDischargeMeas <- gaugeDischargeMeas[,gagNam]
    
    # Write out configured gaugings from transition output
    write.table(gaugeDischargeMeas,
                paste0(dirConfig,"data/Gaugings.txt"),
                sep = "\t",
                row.names = F,
                quote = F)
    
    # Write configuration files
    Config_Data <- readLines(paste0(dirConfig, DataName))
    Config_Data[3] <- gsub("[0-9]{1,6}",nrow(gaugeDischargeMeas),Config_Data[3])
    writeLines(Config_Data, paste0(dirConfig, DataName))
    
    # Configure spaghettis for BaM! predictive model ####
    
    # Read in the priorParameters data - stashed locally from pachctl query
    sampledParameters <- read.csv(
      paste0(dataDir,"l4discharge_sdrc_sampled_table_loader/NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.csv"),
      header = TRUE,
      encoding = "UTF-8"
    )
    sampledParameters <- sampledParameters[
      sampledParameters$curveID==currCurve$curveID,
    ]
    
    # Configure spaghettis
    spagOutPath <- paste0(dirConfig,cookedMCMCName)
    stageQCurve::txt.out.spag.data(spagDataIn = sampledParameters,
                                   spagOutPath = spagOutPath)
    
    # Write out the single spaghetti for the maxPost timeseries
    # Need to remove any NAs introduced during regularization
    if(curveAvailable){
      actualDataIdx <- which(!is.na(EOS_1_min_sum$calcStage)
                             &EOS_1_min_sum$roundDate>=currCurve$curveStartDate
                             &EOS_1_min_sum$roundDate<=currCurve$curveEndDate)
    }else{
      actualDataIdx <- which(!is.na(EOS_1_min_sum$calcStage))
    }
    dataForBaM <- EOS_1_min_sum$calcStage[actualDataIdx]
    write.table(dataForBaM,
                paste0(dirConfig,"data/Ht.txt"),
                sep = "\t",
                row.names = F,
                col.names = F)
    
    # Write out the set of spaghetti for the hu and Totalu
    numSpag <- as.numeric(gsub(" {1,}!.*","",Config_Pred_TotalU[3]))
    kMean <- dataForBaM
    kStd <- EOS_1_min_sum$stageUnc[actualDataIdx]
    #Need to loop through these to the whole list
    stage_noisy <- matrix(NA, ncol = numSpag, nrow = length(dataForBaM))
    for(j in 1:length(dataForBaM)){
      stage_noisy[j,] <- rnorm(numSpag,mean = kMean[j],sd = kStd[j])
    }
    #Write out the "noisy" file
    write.table(stage_noisy,
                paste0(dirConfig,"data/Ht_noisy.txt"),
                sep = "\t",
                row.names = F,
                col.names = F)
    
    # Update number of observations for each of the prediction files
    Config_Pred_Maxpost[2] <- gsub(predRegex,length(dataForBaM),
                                   Config_Pred_Maxpost[2])
    writeLines(Config_Pred_Maxpost,paste0(dirConfig,
                                          gsub(configRegex,"",
                                               ConfigPredictions[2])))
    Config_Pred_hU[2] <- gsub(predRegex,length(dataForBaM),
                              Config_Pred_hU[2])
    writeLines(Config_Pred_hU,paste0(dirConfig,
                                     gsub(configRegex,"",
                                          ConfigPredictions[3])))
    Config_Pred_TotalU[2] <- gsub(predRegex,length(dataForBaM),
                                  Config_Pred_TotalU[2])
    writeLines(Config_Pred_TotalU,paste0(dirConfig,
                                         gsub(configRegex,"",
                                              ConfigPredictions[4])))
    
    # Run BaM! - prediction mode ####
    setwd(dirBaM)
    system2("/home/NEON/nickerson/R/NEON-IS-data-processing/modules/l4_discharge_model/BaM_beta/BaM_exe") # Linux executable
    # system2("BaM_MiniDMSL.exe") # Windows executable
    
    # Read in and format model outputs ####
    Qt_Maxpost_spag <- read.table(paste0(dirConfig,QMaxpostSpagName),
                                  header = F)
    Qt_hU_env <- read.table(paste0(dirConfig,QGaugeUncEnvName),
                            header = T)
    Qt_TotalU_env <- read.table(paste0(dirConfig,QTotalUncEnvName),
                                header = T)
    
    # Add discharge values to data in liters per second ####
    EOS_1_min_sum$csd[actualDataIdx] <- Qt_Maxpost_spag$V1*1000
    EOS_1_min_sum$uprPara[actualDataIdx] <- Qt_hU_env$Q_q97.5*1000
    EOS_1_min_sum$lwrPara[actualDataIdx] <- Qt_hU_env$Q_q2.5*1000
    EOS_1_min_sum$uprRemn[actualDataIdx] <- Qt_TotalU_env$Q_q97.5*1000
    EOS_1_min_sum$uprRemn[EOS_1_min_sum$uprRemn<0] <- 0
    EOS_1_min_sum$lwrRemn[actualDataIdx] <- Qt_TotalU_env$Q_q2.5*1000
    EOS_1_min_sum$lwrRemn[EOS_1_min_sum$lwrRemn<0] <- 0
    EOS_1_min_sum$curveID[actualDataIdx] <- currCurve$curveID
  }
  
  # Format the publication table ####
  colHeaders <- c('startDateTime',
                  'endDateTime',
                  'stationHorizontalID',
                  'curveID',
                  'regressionID',
                  'waterColumnHeightContinuous',
                  'stageContinuous',
                  'stageTotalUncert',
                  'dischargeContinuous',
                  'dischargeUpperParamUncert',
                  'dischargeLowerParamUncert',
                  'dischargeUpperRemnUncert',
                  'dischargeLowerRemnUncert',
                  'waterColumnHeightNullFailQM',
                  'dischargeFinalQF')
  CSD_15_min <- data.frame(matrix(data=NA,
                                  nrow=nrow(EOS_1_min_sum),
                                  ncol=length(colHeaders)))
  names(CSD_15_min) <- colHeaders
  CSD_15_min$startDateTime <- EOS_1_min_sum$roundDate
  CSD_15_min$endDateTime <- EOS_1_min_sum$roundDate+secIn15min
  CSD_15_min$stationHorizontalID <- qHOR
  CSD_15_min$curveID <- EOS_1_min_sum$curveID
  CSD_15_min$regressionID <- EOS_1_min_sum$regressionID
  CSD_15_min$waterColumnHeightContinuous <- EOS_1_min_sum$wchMean
  CSD_15_min$stageContinuous <- EOS_1_min_sum$calcStage
  CSD_15_min$stageTotalUncert <- EOS_1_min_sum$stageUnc
  CSD_15_min$dischargeContinuous <- EOS_1_min_sum$csd
  CSD_15_min$dischargeUpperParamUncert <- EOS_1_min_sum$uprPara
  CSD_15_min$dischargeLowerParamUncert <- EOS_1_min_sum$lwrPara
  CSD_15_min$dischargeUpperRemnUncert <- EOS_1_min_sum$uprRemn
  CSD_15_min$dischargeLowerRemnUncert <- EOS_1_min_sum$lwrRemn
  CSD_15_min$waterColumnHeightNullFailQM <- (15-EOS_1_min_sum$wchNumPts)/15*100
  CSD_15_min$dischargeFinalQF <- ifelse(is.na(CSD_15_min$dischargeContinuous),1,0)
  CSD_15_min$regressionID[is.na(CSD_15_min$waterColumnHeightContinuous)] <- NA
  
  # write_CSD_15_min <- try(NEONprocIS.base::def.wrte.parq(
  #   data = CSD_15_min,
  #   NameFile = base::paste0(DirOutUcrt,"/",
  #                           Context,"_",sensor,"_",CFGLOC,"_",
  #                           format(timeBgn,format = "%Y-%m-%d"),
  #                           "_","ucrt_",window,".parquet"),
  #   Schm = SchmUcrtOutAgr),
  #   silent=TRUE)
  # 
  # return()
}
