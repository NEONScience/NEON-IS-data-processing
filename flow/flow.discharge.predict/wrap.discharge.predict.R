##############################################################################################
#' @title Model 1-Min Water Column Height to 15-Min Discharge Data

#' @author
#' Zachary Nickerson \email{nickerson@battelleecology.org}

#' @description Wrapper function. Average water column height data to 15-min; 
#' model continuous stage based on the relationship between gauge height and 
#' water column height; model continuous discharge using a 3rd party Bayesian 
#' model executable.

#' @param DirIn Character value. The input path to the data from a single 
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
#' @param DirOutBase Character value. The output path that will replace the 
#' #/pfs/BASE_REPO portion of DirInWch.

#' @return A repository in DirOutBase containing the merged and filtered Kafka
#' output, where DirOutBase replaces BASE_REPO of argument \code{DirInWch} but 
#' otherwise retains the child directory structure of the input path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
# DirIn <-'/home/NEON/nickerson/pfs/testing/2024/02/25/l4discharge_HOPB132100/data'
# DirBaM <- '/home/NEON/nickerson/R/NEON-IS-data-processing/flow/flow.discharge.predict/BaM_beta'
# DirOutBase <- "/home/NEON/nickerson/pfs/out"
# SchmDataOut="/home/NEON/nickerson/pfs/l4discharge_avro_schemas/l4discharge/l4discharge_dp04.avsc"
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
# wrap.discharge.predict(DirIn=DirIn,
#                        DirBaM=DirBaM,
#                        DirOutBase=DirOutBase,
#                        SchmDataOut=SchmDataOut,
#                        log=log)

#' @seealso None currently

# changelog and author contributions / copyrights
#   Zachary Nickerson (2025-09-18)
#     original creation
#   Nora Catolico (2025-12-17) 
#     added error logging, updates to better interact with pachyderm
##############################################################################################
wrap.discharge.predict <- function(DirIn,
                                   DirBaM,
                                   DirOutBase,
                                   SchmDataOut=NULL,
                                   log=NULL
){

  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirInData <- fs::path(DirIn,'data')
  
  # create output 
  DirOutData <- base::paste0(DirOutBase,InfoDirIn$dirRepo,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  
  # Determine the type of file available ####
  DirInOSData <- paste0(DirIn,"/data")
  correctedFile <- list.files(DirInOSData,pattern = "CSD_15_min")
  
  DirInSWE <- def.dir.in.partial(DirBgn = DirIn,
                       nameDirSubPartial = 'surfacewater-physical',
                       log = log)
  DirInSWEdata <- paste0(DirInSWE,"/data")
  uncorrectedFile <- list.files(DirInSWEdata,pattern = "EOS_1_min")
  
  # Read in CSD file if available
  if(length(correctedFile)>0){
    CSD_15_min  <-
      base::try(read.csv(paste(DirInOSData,correctedFile,sep = "/")))
    if (base::any(base::class(CSD_15_min) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0(DirInOSData,"/",correctedFile," is unreadable"))
      base::stop()
    }
  }else{
    CSD_15_min <- NULL
  }
  
  # Read in the WCH data if available - stashed locally from pachctl query ####
  if(length(uncorrectedFile)>0){
    EOS_1_min <- 
      base::try(NEONprocIS.base::def.read.parq(paste(DirInSWEdata,uncorrectedFile,sep = "/")))
    if (base::any(base::class(EOS_1_min) == 'try-error')) {
      # Generate error and stop execution
      log$error(base::paste0(DirInSWEdata,"/",uncorrectedFile," is unreadable"))
      base::stop()
    }
  }else{
    EOS_1_min <- NULL
  }
  
  
  # Workflow for predicting uncorrected discharge ####
  if(length(EOS_1_min)>0 & length(CSD_15_min)==0){
    log$info(base::paste0("No CSD file exists. SWE file will be processed. FileName: ",uncorrectedFile))
    
    # Set constants
    secInDay <- 60*60*24
    secIn15min <- 60*15
    siteID <- substr(uncorrectedFile,23,26)
    qHOR <- substr(uncorrectedFile,27,29)
    startDate <- as.POSIXct(substr(uncorrectedFile,34,43),tz="UTC")
    endDate <- startDate+secInDay-secIn15min
  
    # Set paths and parse configuration files ####
    dirConfig <- paste0(DirBaM,"/BaM_BaRatin/")
    nameRegex <- "^\"|\" .*"
    configRegex <- "^\'|\' .*"
    predRegex <- "[0-9]{1,}"
    Config_BaM <- readLines(paste0(DirBaM,"/Config_BaM.txt"))
    
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
      paste(DirInOSData,
            "NEON.DOM.SITE.DP1.00133.001.csd_gaugeWaterColumnRegression_pub.csv",
            sep="/"),
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
        paste(DirInOSData,
              "NEON.DOM.SITE.DP4.00133.001.sdrc_gaugePressureRelationship_pub.csv",
              sep="/"),
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
      paste(DirInOSData,
            "NEON.DOM.SITE.DP1.00133.001.sdrc_curveIdentification_pub.csv",
            sep="/"),
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
      #c=1
      currCurve <- curveIdentification[c,]
      # Configure priors for BaM! predictive model ####
      
      # Read in the controlInfo data - stashed locally from pachctl query
      surveyDate <- as.Date(currCurve$controlSurveyEndDateTime)
      controlInfo <- read.csv(
        paste(DirInOSData,
              "NEON.DOM.SITE.DP1.00133.001.sdrc_controlInfo_pub.csv",
              sep="/"),
        header = TRUE,
        encoding = "UTF-8"
      )
      controlInfo <- controlInfo[
        controlInfo$siteID==siteID
        &as.Date(controlInfo$endDate)==surveyDate,
      ]
      # Read in the priorParameters data - stashed locally from pachctl query
      priorParameters <- read.csv(
        paste(DirInOSData,
              "NEON.DOM.SITE.DP1.00133.001.sdrc_priorParameters_pub.csv",
              sep="/"),
        header = TRUE,
        encoding = "UTF-8"
      )
      priorParameters <- priorParameters[
        priorParameters$siteID==siteID
        &as.Date(priorParameters$endDate)==surveyDate,
      ]
      
      # Configure model run
      RunOptionsPath = paste0(dirConfig,RunOptionsName)
      RunOptions <- readLines(RunOptionsPath)
      RunOptions[1] <- gsub("\\.true\\.",".false.",RunOptions[1])
      RunOptions[2] <- gsub("\\.true\\.",".false.",RunOptions[2])
      RunOptions[3] <- gsub("\\.true\\.",".false.",RunOptions[3])
      RunOptions[4] <- gsub("\\.false\\.",".true.",RunOptions[4])
      writeLines(RunOptions, RunOptionsPath)
      
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
        paste(DirInOSData,
              "NEON.DOM.SITE.DP4.00133.001.sdrc_gaugeDischargeMeas_pub.csv",
              sep="/"),
        header = TRUE,
        encoding = "UTF-8"
      )
      gaugeDischargeMeas <- gaugeDischargeMeas[
        gaugeDischargeMeas$curveID==currCurve$curveID,
      ]
      
      # Reconfigure the gaugings data table
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
        paste(DirInOSData,
              "NEON.DOM.SITE.DP4.00133.001.sdrc_sampledParameters_pub.csv",
              sep="/"),
        header = TRUE,
        encoding = "UTF-8"
      )
      sampledParameters <- sampledParameters[
        sampledParameters$curveID==currCurve$curveID,
      ]
      
      # Configure spaghettis
      spagOutPath <- paste0(dirConfig,cookedMCMCName)
      numCtrls <- unique(sampledParameters$controlNumber)
      numCtrls <- seq(along = numCtrls)
      ctrlNames <- rep(NA,(length(numCtrls)*3))
      offsetNames <- rep(NA,length(numCtrls))
      for(i in seq(along = numCtrls)){
        ctrlNames[1+(i-1)*3] <- paste0("k",numCtrls[i])
        ctrlNames[2+(i-1)*3] <- paste0("a",numCtrls[i])
        ctrlNames[3+(i-1)*3] <- paste0("c",numCtrls[i])
        offsetNames[i] <- paste0("b",numCtrls[i])
      }
      spag_Names <- c(
        ctrlNames,
        'gamma1',
        'gamma2',
        'LogPost',
        offsetNames
      )
      outputDF <- data.frame(
        matrix(
          data=NA,
          ncol=length(spag_Names), 
          nrow=(length(sampledParameters$controlNumber)/length(numCtrls))
          )
        )
      names(outputDF) <- spag_Names
      matchOnSpag <- unique(sampledParameters$parameterNumber)
      for(i in seq(along = matchOnSpag)){
        currSpag <- matchOnSpag[i]
        loopSpagData <- sampledParameters[
          sampledParameters$parameterNumber == currSpag,
        ]
        #Ones that are shared for all controls
        outputDF$gamma1[i] <- unique(loopSpagData$spagGamma1)
        outputDF$gamma2[i] <- unique(loopSpagData$spagGamma2)
        outputDF$LogPost[i] <- unique(loopSpagData$spagLogPost)
        #Converting from long to wide
        for(j in seq(along = numCtrls)){
          currCtrl <- as.character(j)
          outputDF[
            i,which(names(outputDF) == paste0("k",currCtrl))
          ] <- loopSpagData$spagActivationStage[
            loopSpagData$controlNumber == currCtrl
          ] #k
          outputDF[
            i,which(names(outputDF) == paste0("a",currCtrl))
          ] <- loopSpagData$spagCoefficient[
            loopSpagData$controlNumber == currCtrl
          ] #a
          outputDF[
            i,which(names(outputDF) == paste0("c",currCtrl))
          ] <- loopSpagData$spagExponent[
            loopSpagData$controlNumber == currCtrl
          ] #c
          #Count from the end for this one
          outputDF[
            i,which(names(outputDF) == paste0("b",currCtrl))
          ] <-loopSpagData$spagZeroFlowOffset[
            loopSpagData$controlNumber == currCtrl
          ] #b
        }
      }
      #Format data
      txt.fmt.MCMC.cooked <- function(inputNum){
        finalCharLength <- 15
        sciNotationLength <- 4 #Format as E+## or E-##
        expLength <- 2 #Length of exponent characters
        
        inputNum <- as.numeric(inputNum)
        inputFact <- 0
        while(!(abs(inputNum)<1
                &abs(inputNum) >0.09999999)
              &abs(inputNum) != 1){
          if(abs(inputNum) > 1){
            inputNum <- inputNum/10
            inputFact <- inputFact + 1
          }else{
            inputNum <- inputNum*10
            inputFact <- inputFact - 1
          }
        }
        #Test whether + or -
        if(inputFact<0){
          sign<-"-"
          inputFact <- abs(inputFact)
        }else{
          sign<-"+"
        }
        #Add 0 in front of sci notation exponent if needed
        if(nchar(inputFact)<expLength){inputFact <- paste0("0",inputFact)}
        
        #Final formatting
        spaces <- paste(
          rep(" ",
              (finalCharLength - sciNotationLength - nchar(inputNum))),
          collapse = "")
        outputNum <- paste0(spaces,inputNum,"E",sign,inputFact)
        return(outputNum)
      }
      outputDF <- apply(outputDF,c(1,2),txt.fmt.MCMC.cooked)
      #Format names
      colnames(outputDF) <- format(colnames(outputDF), trim = F, width = 15)
      write.table(outputDF, spagOutPath, row.names = F, quote = F, sep = "")
  
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
      setwd(DirBaM)
      system2(paste(DirBaM,"BaM",sep = "/")) # Linux executable

      # Read in and format model outputs ####
      Qt_Maxpost_spag <- read.table(paste0(dirConfig,QMaxpostSpagName),
                                    header = F)
      Qt_hU_env <- read.table(paste0(dirConfig,QGaugeUncEnvName),
                              header = T)
      Qt_TotalU_env <- read.table(paste0(dirConfig,QTotalUncEnvName),
                                  header = T)
      
      # Add discharge values to data in liters per second ####
      EOS_1_min_sum$csd[actualDataIdx] <- Qt_Maxpost_spag$V1*1000
      EOS_1_min_sum$uprPara[actualDataIdx] <- Qt_hU_env$q97.5*1000
      EOS_1_min_sum$lwrPara[actualDataIdx] <- Qt_hU_env$q2.5*1000
      EOS_1_min_sum$uprRemn[actualDataIdx] <- Qt_TotalU_env$q97.5*1000
      EOS_1_min_sum$uprRemn[EOS_1_min_sum$uprRemn<0] <- 0
      EOS_1_min_sum$lwrRemn[actualDataIdx] <- Qt_TotalU_env$q2.5*1000
      EOS_1_min_sum$lwrRemn[EOS_1_min_sum$lwrRemn<0] <- 0
      EOS_1_min_sum$curveID[actualDataIdx] <- currCurve$curveID
    }
    
    # Format the publication table ####
    colHeaders <- c('startDateTime',
                    'endDateTime',
                    'stationHorizontalID',
                    'curveID',
                    'regressionID',
                    'stageContinuous',
                    'stageTotalUncert',
                    'dischargeContinuous',
                    'dischargeUpperParamUncert',
                    'dischargeLowerParamUncert',
                    'dischargeUpperRemnUncert',
                    'dischargeLowerRemnUncert',
                    'dischargeCorrectionApplied',
                    'dischargeGapFilledInterpolation',
                    'dischargeGapFilledConstant',
                    'dischargeGapFilledUSGS',
                    'dischargeCorrectedShiftPre',
                    'dischargeCorrectedShiftPost',
                    'waterColumnHeightContinuous',
                    'waterColumnHeightCorrectionApplied',
                    'waterColumnHeightGapFilledInterpolation',
                    'waterColumnHeightGapFilledConstant',
                    'waterColumnHeightGapFilledTransducer',
                    'waterColumnHeightGapFilledConductivity',
                    'waterColumnHeightCorrectedShiftPre',
                    'waterColumnHeightCorrectedShiftPost',
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
    
    outFileName <- gsub("surfacewater-physical","l4discharge",uncorrectedFile)
    outFileName <- gsub("EOS_1_min_001","CSD_15_min_015",outFile)

  }else{
    # Workflow for predicting corrected discharge ####
    if(length(EOS_1_min)>0 & length(CSD_15_min)>0){
      log$info(base::paste0("CSD file is available and will passed through unmodified. FileName: ",correctedFile))
      outFileName <-gsub("csv","parquet",correctedFile)
      
    }else{
      log$error(base::paste0("No SWE or CSD data available for this date."))
      base::stop()
    }
  }
  
  write_CSD_15_min <- try(NEONprocIS.base::def.wrte.parq(
    data = CSD_15_min,
    NameFile = paste(DirOutData,
                     outFileName,
                     sep = "/"),
    Schm = SchmDataOut
    ),
    silent=TRUE
    )

  return()
}
