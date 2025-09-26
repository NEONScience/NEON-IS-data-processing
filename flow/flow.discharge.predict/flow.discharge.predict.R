##############################################################################################
#' @title

#' @author
#' Zachary Nickerson \email{nickerson@battelleecology.org}

#' @description

#' @param

#' @return 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-09-18)
#     original creation
##############################################################################################

library(NEONprocIS.base)
library(stageQCurve)
library(tidyverse)

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
dirBaM <- "modules/l4_discharge_model/BaM_beta/"
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
  paste("/home/NEON/nickerson/pfs",
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
                   wchMin=min(surfacewaterColumnHeight,
                              na.rm=T),
                   wchMax=max(surfacewaterColumnHeight,
                              na.rm=T),
                   wchVar=var(surfacewaterColumnHeight,
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
  "/home/NEON/nickerson/pfs/l4discharge_csd_gag_regression_table_loader/NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv",
  header = TRUE,
  encoding = "UTF-8"
)
# Check if there is a regression available 
regAvailable <- any(gaugeWaterColumnRegression$siteID==siteID
                    &gaugeWaterColumnRegression$regressionStartDate<=startDate
                    &gaugeWaterColumnRegression$regressionEndDate>=endDate)
if(regAvailable){
  # If yes, subset to that regression
  gaugeWaterColumnRegression <- gaugeWaterColumnRegression[
    gaugeWaterColumnRegression$siteID==siteID
    &gaugeWaterColumnRegression$regressionStartDate<=startDate
    &gaugeWaterColumnRegression$regressionEndDate>=endDate,
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
regID <- gaugeWaterColumnRegression$regressionID

# Model stage and estimate systematic uncertainty ####

# Read in the curveIdentification data - stashed locally from pachctl query
gaugePressureRelationship <- read.csv(
  "/home/NEON/nickerson/pfs/l4discharge_sdrc_gaugePress_table_loader/NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.csv",
  header = TRUE,
  encoding = "UTF-8"
)
gaugePress <- gaugePressureRelationship[
  gaugePressureRelationship$regressionID==regID,
]

# Predict model fit between stage and water column height
gaugePress_model <- lm(gaugeHeight~calcWaterColumnHeight,
                       data=gaugePress)
gaugePress_model_fit <- data.frame(
  stats::predict(
    gaugePress_model,
    newdata = data.frame(calcWaterColumnHeight=EOS_1_min_sum$wchMean),
    interval = "confidence"))
gaugePress_model_fit$unc <- gaugePress_model_fit$upr-gaugePress_model_fit$fit

# Add modeled stage to data
EOS_1_min_sum$calcStage <- gaugePress_model_fit$fit

# Sum systematic and nonsystematic uncertainty and add to data
EOS_1_min_sum$stageUnc <- EOS_1_min_sum$wchNonSysUncert+gaugePress_model_fit$unc

# Determine if modeling with a current or previous rating curve ####

# Read in the curveIdentification data - stashed locally from pachctl query
curveIdentification <- read.csv(
  "/home/NEON/nickerson/pfs/l4discharge_sdrc_curveID_table_loader/NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv",
  header = TRUE,
  encoding = "UTF-8"
)
# Check if there is a curve available 
curveAvailable <- any(curveIdentification$siteID==siteID
                      &curveIdentification$curveStartDate<=startDate
                      &curveIdentification$curveEndDate>=endDate)
if(curveAvailable){
  # If yes, subset to that curve
  curveIdentification <- curveIdentification[
    curveIdentification$siteID==siteID
    &curveIdentification$curveStartDate<=startDate
    &curveIdentification$curveEndDate>=endDate,
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
curveID <- curveIdentification$curveID

# Configure priors for BaM! predictive model ####

# Read in the controlInfo data - stashed locally from pachctl query
surveyDate <- as.Date(curveIdentification$controlSurveyEndDateTime)
controlInfo <- read.csv(
  "/home/NEON/nickerson/pfs/l4discharge_sdrc_controlInfo_table_loader/NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv",
  header = TRUE,
  encoding = "UTF-8"
)
controlInfo <- controlInfo[
  controlInfo$siteID==siteID
  &as.Date(controlInfo$endDate)==surveyDate,
]
# Read in the priorParameters data - stashed locally from pachctl query
priorParameters <- read.csv(
  "/home/NEON/nickerson/pfs/l4discharge_sdrc_prior_table_loader/NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv",
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
  "/home/NEON/nickerson/pfs/l4discharge_sdrc_gaugeDsc_table_loader/NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.csv",
  header = TRUE,
  encoding = "UTF-8"
)
gaugeDischargeMeas <- gaugeDischargeMeas[
  gaugeDischargeMeas$curveID==curveID,
]

# Reconfigure the guagings data table
gaugeDischargeMeas <- gaugeDischargeMeas[
  ,c("gaugeHeight","gaugeHeightUnc","streamDischarge","streamDischargeUnc")
]
names(gaugeDischargeMeas) <- c("H","uH","Q","uQ")
gaugeDischargeMeas$bH <- NA
gaugeDischargeMeas$bHindex <- NA
gaugeDischargeMeas$bQ <- NA
gaugeDischargeMeas$bQindex <- NA

# Write out configured gaugings from transition output
write.table(gaugeDischargeMeas,
            paste0(dirConfig,"data/gaugings.txt"),
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
  "/home/NEON/nickerson/pfs/l4discharge_sdrc_sampled_table_loader/NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.csv",
  header = TRUE,
  encoding = "UTF-8"
)
sampledParameters <- sampledParameters[
  sampledParameters$curveID==curveID,
]

# Configure spaghettis
spagOutPath <- paste0(dirConfig,cookedMCMCName)
stageQCurve::txt.out.spag.data(spagDataIn = sampledParameters,
                               spagOutPath = spagOutPath)

# Write out the single spaghetti for the maxPost timeseries
# Need to remove any NAs introduced during regularization
actualDataIdx <- which(!is.na(EOS_1_min_sum$calcStage))
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

#__ Update number of observations for each of the prediction files ####
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
system2("BaM_MiniDMSL.exe")


#











