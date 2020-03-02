
###### For Testing Only ######
calFile <- NEONprocIS.cal::def.read.cal.xml(NameFile = "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/water-quality-001/sunav2/CFGLOC23456/calibration/rawNitrateSingleCompressedStream/30000000005365_WO33177_170813.xml",
                                            Vrbs = TRUE)
calFileTestTrue <- NEONprocIS.cal::def.read.cal.xml(NameFile = "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/water-quality-001/exofdom/CFGLOC12345/calibration/fDOM/30000000016628_WO38587_197624.xml",
                                                Vrbs = FALSE)
calFileTestTrue$cal$Value[calFileTestTrue$cal$Name == "CVALA1"]


##############################

##############################################################################################
#' @title Workflow for correcting fdom for temperature and absorbance

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Workflow. Apply temperature and absorbance corrections to fdom data for the
#' water quality transition.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/named_location_group/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /exofdom/CFGLOC/data
#'         /exofdom/CFGLOC/calibration
#'         /exoconductivity/CFGLOC/data
#'         /prt/CFGLOC/data
#'         /SUNA/CFGLOC/data
#'         /SUNA/CFGLOC/calibration
#'        
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn.
#' 
#' 3. "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' 4. "FileSchmQf=value" (optional), where values is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). See below for details. 
#'
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}},
#' which uses system environment variables if available.

#' @return Corrected fdom data and associated flags for temperature and absorbance corrections.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' #TBD

#' @seealso None currently

# changelog and author contributions / copyrights
#   Kaelin Cawley (2020-01-23)
#     original creation
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

#For testing
repo <- "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/water-quality-001/"

dataNameSUNA <- "sunav2/CFGLOC23456/data/sunav2_17300_2019-01-01.avro"
dataNamefdom <- "exofdom/CFGLOC12345/data/exofdom_45831_2019-01-01.avro"
dataNameCond <- "exoconductivity/CFGLOC12345/data/exoconductivity_43601_2019-01-01.avro"
dataNameprt <- "prt/CFGLOC101580/data/prt_CFGLOC101580_2019-01-01.avro"

calNameSUNA <- "sunav2/CFGLOC23456/calibration/rawNitrateSingleCompressedStream/30000000005365_WO33177_170813.xml"
calNamefdom <- "exofdom/CFGLOC12345/calibration/fDOM/30000000023496_WO31715_162037.xml"

# String constants for CVAL files
rhoNamefdom <- "CVALA1"
pathNamefdom <- "CVALB1"
calTableNameSUNA <- "CVALTABLEA1"

#Read in  data
fdomData <- NEONprocIS.base::def.read.avro.deve(NameFile = paste0(repo,dataNamefdom),
                                                NameLib = "/home/NEON/kcawley/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")

#Default all flags to -1 then change them as the test can be performed
fdomData$fDOMTempQF <- -1
fdomData$fDOMAbsQF<- -1
fdomData$spectrumCount <- -1

#Default expanded unceratinty to NA until it can be calculated
fdomData$fDOMExpUncert <- NA

#If there isn't any fdom data, head over to the empty file pipeline?

#Try to pull prt data if we're at a stream site?

#If there isn't any prt data or we're at a buoy site (or should this just be whatever reason and we don't even need to pull NL info?), use temp on conductivity probe
tempData <- NEONprocIS.base::def.read.avro.deve(NameFile = paste0(repo,dataNameCond),
                                                NameLib = "/home/NEON/kcawley/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")

#Pull rho_fdom and pathlength from the appropriate cal file (use def.cal.meta.R and def.cal.slct.R to make that happen)
calFilefdom <- NEONprocIS.cal::def.read.cal.xml(NameFile = paste0(repo,calNamefdom),Vrbs = FALSE)
#CVAL has only on rho_fdom value for all sensors and sites
rho_fdom <- try(base::as.numeric(calFilefdom$cal$Value[gsub(" ","",calFilefdom$cal$Name) == rhoNamefdom]))
#rho_fdom may not exist for some older data
if(is.null(rho_fdom)){
  fdomData$fDOMTempQF <- 1
}else{
  #Combine fdom and temp
  fdomData$readout_time_min <- format(fdomData$readout_time, format = "%Y-%m-%d %H:%M")
  tempData$readout_time_min <- format(tempData$readout_time, format = "%Y-%m-%d %H:%M")
  fdomData <- merge(fdomData, tempData, by = "readout_time_min", all.x = TRUE, suffixes = c(".fdom",".temp"))
  
  #Populate the fdom temperature QF values
  fdomData$fDOMTempQF[!is.na(fdomData$surfaceWaterTemperature)] <- 0
  fdomData$fDOMTempQF[is.na(fdomData$surfaceWaterTemperature)] <- 1
  
  #Determine fdom temperature correction factor
  fdomData$tempFactor <- 1/(1+rho_fdom*(fdomData$surfaceWaterTemperature-20))
}

#Determine fdom absorbance correction factor
#Pull pathlength from CVAL files
pathlength <- as.numeric(calFilefdom$cal$Value[gsub(" ","",calFilefdom$cal$Name) == pathNamefdom])
#pathlength doesn't exist for some older data, so don't even try to correct it if it isn't there
if(is.null(pathlength)){
  fdomData$fDOMAbsQF <- 1
}else{
  #Ross suggested that I use code from Guy for Abs_ex and Abs_em
  
  #Populate the fdom absorbance QF values
  fdomData$fDOMAbsQF[!is.na(fdomData$Abs_ex)&!is.na(fdomData$Abs_em)] <- 0
  fdomData$fDOMAbsQF[is.na(fdomData$Abs_ex)|is.na(fdomData$Abs_em)] <- 1
  
  #Determine fdom absorbance correction factor
  fdomData$absFactor <- 10^((Abs_ex + Abs_em) * pathlength)
}

#Calulate the fdom output with the factors that exist
fdomData$fdom_out <- fdomData$rawCalibratedfDOM * fdomData$tempFactor * fdomData$absFactor

#Calculate expanded uncertainty according to the ATBD
#Uncertainty when no corrections can be applied
fdomData$fDOMExpUncert[fdomData$fDOMTempQF != 0 & fdomData$fDOMAbsQF != 0] #Cove probably has a function for this, just U_CVALA1

#Unceratinty when temperature corrections, only, are applied
fdomData$fDOMExpUncert[fdomData$fDOMTempQF == 0 & fdomData$fDOMAbsQF != 0]

#Unceratinty when absorbance corrections, only, are applied
fdomData$fDOMExpUncert[fdomData$fDOMTempQF != 0 & fdomData$fDOMAbsQF == 0]

#Unceratinty when both temperature and absorbance corrections are applied
fdomData$fDOMExpUncert[fdomData$fDOMTempQF == 0 & fdomData$fDOMAbsQF == 0]

#Write an AVRO file for data and uncertainty (which get stats)

#Write an AVRO file for the flags (which get metrics)


