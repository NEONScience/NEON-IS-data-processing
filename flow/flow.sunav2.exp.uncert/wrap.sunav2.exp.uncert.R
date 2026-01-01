##############################################################################################
#' @title Wrapper for SUNA expanded uncertainty calculation

#' @author
#' Bobby Hensley \email{hensley@battelleecology.org}
#' 
#' @description Wrapper function. Calculates the expanded uncertainty for each SUNA burst.
#'
#' @param DirIn Character value. The base file path to the averaged stats and uncertainty coefficients.
#' 
#' @param DirOutBase Character value. The base file path for the output data. 
#' 
#' @param SchmStats (optional), A json-formatted character string containing the schema for the output averaged stats parquet.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Averaged stats file and quality metric file in daily parquets.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' 
#' @keywords Currently none
#' 
#' @examples
#' # Not run
# DirIn<-"~/pfs/nitrate_null_gap_ucrt/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733"
# DirOut<-"~/pfs/nitrate_null_gap_ucrt_updated/2025/06/24/nitrate_CRAM103100/sunav2/CFGLOC110733" 
# SchmStats<-base::paste0(base::readLines('~/pfs/sunav2_avro_schemas/sunav2_stats.avsc'),collapse='')
# log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#'
#'                                                                                                                                                                                          
#' @changelog
#' Bobby Hensley (2025-11-03)
#' Initial creation.
#' 
##############################################################################################
wrap.sunav2.exp.uncert <- function(DirIn,
                                      DirOutBase,
                                      SchmStats=NULL,
                                      DirSubCopy=NULL,
                                      log=NULL
){
  
  #' Start logging if not already.
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirInStats <- paste0(DirIn,"/stats")
  DirInCoeff <- paste0(DirIn,"/uncertainty_coef")
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutStats <- base::paste0(DirOut,"/stats")
  base::dir.create(DirOutStats,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=base::paste0(DirIn,'/',DirSubCopy),
                                       DirDest=DirOut,
                                       LnkSubObj=TRUE,
                                       log=log)
  }
  
  #' Read in parquet file of averaged stats.
  statsFileName<-base::list.files(DirInStats,full.names=FALSE)
  if(length(statsFileName)==0){
    log$error(base::paste0('Stats file not found in ', DirInStats)) 
    stop()
  } else {
    statsData<-base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInStats, '/', statsFileName),
                                                       log = log),silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',statsFileName))
  }
  
  #' Read in json file of uncertainty coefficients.
  coeffileName<-base::list.files(DirInCoeff,full.names=FALSE)
  if(length(coeffileName)==0){
    log$error(base::paste0('Uncertainty coefficient not found in ', DirInCoeff)) 
    stop()
  } else {
    uncertCoeff<-base::try(NEONprocIS.cal::def.read.ucrt.coef.fdas(NameFile = base::paste0(DirInCoeff, '/', coeffileName)),
                              silent = FALSE)
    log$debug(base::paste0('Successfully read in file: ',coeffileName))
  }
  
  if(length(uncertCoeff)>0){
    #' Converts uncertainty coefficient dates to POSIXct and values to numeric
    uncertCoeff$start_date <- as.POSIXct(uncertCoeff$start_date, format = "%Y-%m-%dT%H:%M:%S", tz='utc')
    uncertCoeff$end_date <- as.POSIXct(uncertCoeff$end_date, format = "%Y-%m-%dT%H:%M:%S", tz='utc')
    uncertCoeff$Value<-as.numeric(uncertCoeff$Value)
    
    #' Determines which uncertainty coefficients to be applied to each time interval.
    #' (In case there are more than one on a particular day)
    uncertCoeff<-uncertCoeff[order(uncertCoeff$start_date), ]
    uncertCoeffA1<-uncertCoeff[(uncertCoeff$Name=="U_CVALA1"),]
    statsData$uncertCoeffA1<-NA
    for (i in 1:nrow(statsData)){
      for (j in 1:nrow(uncertCoeffA1)){
        if(statsData[i,which(colnames(statsData)=="startDateTime")]>=uncertCoeffA1[j,which(colnames(uncertCoeffA1)=="start_date")]){
          statsData[i,which(colnames(statsData)=="uncertCoeffA1")]=uncertCoeffA1[j,which(colnames(uncertCoeffA1)=="Value")]}}}
    uncertCoeffA3<-uncertCoeff[(uncertCoeff$Name=="U_CVALA3"),]
    statsData$uncertCoeffA3<-NA
    for (i in 1:nrow(statsData)){
      for (j in 1:nrow(uncertCoeffA3)){
        if(statsData[i,which(colnames(statsData)=="startDateTime")]>=uncertCoeffA3[j,which(colnames(uncertCoeffA3)=="start_date")]){
          statsData[i,which(colnames(statsData)=="uncertCoeffA3")]=uncertCoeffA3[j,which(colnames(uncertCoeffA3)=="Value")]}}}
    
    #' Identify the column name with the mean, variance and number of points
    meanName<-grep("Mean",names(statsData),value=TRUE)
    varianceName<-grep("Variance",names(statsData),value=TRUE)
    pointsName<-grep("NumPts",names(statsData),value=TRUE)
    
    #' Calculates calibration uncertainty. See ATBD for more details.
    #' Concentrations <= 20 mg/L have fixed calibration uncertainty equal to coeffA1. 
    #' Concentrations greater than 20 mg/L uncertainty equals concentration times coeffA1.
    #' Note stats data concentrations are in uM so threshold needs to be converted from mg/L by dividing by 0.014 (14 g/mol / 1000 ug/mg)  
    statsData$calUncert<-NA
    for (i in 1:nrow(statsData)){
      if(is.na(statsData[i,which(colnames(statsData)==meanName)])){statsData[i,which(colnames(statsData)=="calUncert")]=NA}
      if(!is.na(statsData[i,which(colnames(statsData)==meanName)])){
        if(statsData[i,which(colnames(statsData)==meanName)]<=(20/0.014)){statsData[i,which(colnames(statsData)=="calUncert")]=statsData[i,which(colnames(statsData)=="uncertCoeffA1")]}
        if(statsData[i,which(colnames(statsData)==meanName)]>(20/0.014)){statsData[i,which(colnames(statsData)=="calUncert")]=statsData[i,which(colnames(statsData)=="uncertCoeffA3")]}
      }
    }
    
    #' Calculates the repeatability (natural variation). See ATBD for more details. 
    statsData$natVar<-NA 
    for (i in 1:nrow(statsData)){
      if(!is.na(statsData[i,which(colnames(statsData)==meanName)])){statsData[i,which(colnames(statsData)=="natVar")]=
        sqrt(statsData[i,which(colnames(statsData)==varianceName)]/statsData[i,which(colnames(statsData)==pointsName)])}
    }
    
    #' Calculates the expanded uncertainty, which is estimated as 2x the combined uncertainty. See ATBD for more details.
    statsData$surfWaterNitrateExpUncert<-NA  
    for (i in 1:nrow(statsData)){
      if(!is.na(statsData[i,which(colnames(statsData)==meanName)])){statsData[i,which(colnames(statsData)=="surfWaterNitrateExpUncert")]=
        2*sqrt(statsData[i,which(colnames(statsData)=="natVar")]+statsData[i,which(colnames(statsData)=="calUncert")])}
    }
    
    #' Removes unnecessary columns.
    statsData<-subset(statsData,select=-c(uncertCoeffA3,uncertCoeffA1,calUncert,natVar))
  }else{
    #add required columns to stats data
    statsData$surfWaterNitrateExpUncert<-NA
  }
  
  statsData$surfWaterNitrateMean[is.nan(statsData$surfWaterNitrateMean)]<-NA
  
  
  #' Write out updated stats file.  
  rptOutStats <- try(NEONprocIS.base::def.wrte.parq(data = statsData,
                                                    NameFile = base::paste0(DirOutStats,'/',statsFileName),
                                                    Schm = SchmStats),silent=TRUE)
  if(class(rptOutStats)[1] == 'try-error'){
    log$error(base::paste0('Cannot write updated stats to ',base::paste0(DirOutStats,'/',statsFileName),'. ',attr(rptOutStats, "condition")))
    stop()
  } else {
    log$info(base::paste0('Updated stats written successfully in ', base::paste0(DirOutStats,'/',statsFileName)))
  }
  
  
}



