
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
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /named_location_group/exofdom/CFGLOC/data
#'         /named_location_group/exofdom/CFGLOC/calibration
#'         /named_location_group/exoconductivity/CFGLOC/data
#'         /named_location_group/prt/CFGLOC/data
#'         /named_location_group/SUNA/CFGLOC/data
#'         /named_location_group/SUNA/CFGLOC/calibration
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
##### For Testing #####
DirIn = "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/"
DirOut = "/scratch/pfs/waterQuality_fdom_correction_group_test/out"
FileScmData = "/scratch/pfs/avro_schemas/dp0p/exofdom_corrected.avsc" #In flow.cal.conv these start at avro_schemas, is that better?
FileSchmQF = "/scratch/pfs/avro_schemas/dp0p/flags_correction_exofdom.avsc"
#######################

# Start logging
log <- NEONprocIS.base::def.log.init()

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c('data', 'calibration', DirSubCopy))
log$debug(base::paste0(
  'Expected subdirectories of each datum path: ',
  base::paste0(nameDirSub, collapse = ',')
))

#For testing
repo <- "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/water-quality-001/"

dataNameSUNA <- "sunav2/CFGLOC23456/data/sunav2_17300_2019-01-01.avro"
dataNamefdom <- "exofdom/CFGLOC12345/data/exofdom_45831_2019-01-01.avro"
dataNameCond <- "exoconductivity/CFGLOC12345/data/exoconductivity_43601_2019-01-01.avro"
dataNameprt <- "prt/CFGLOC101580/data/prt_CFGLOC101580_2019-01-01.avro"

calNameSUNA <- "sunav2/CFGLOC23456/calibration/rawNitrateSingleCompressedStream/30000000005365_WO33177_170813.xml"
calNamefdom <- "exofdom/CFGLOC12345/calibration/fDOM/30000000023496_WO31715_162037.xml"
# calNameprt <- Not using until we enhande this with the prt data 
calNameCond <- "exoconductivity/CFGLOC12345/uncertainty_data/exoconductivity_43601_2019-01-01_uncertaintyData.avro"

# String constants for CVAL files
rhoNamefdom <- "CVALA1" #Calibration factor for temperature correction function for fDOM
pathNamefdom <- "CVALB1" #Calibration factor for absorbance correction function for fDOM
calTableNameSUNA <- "CVALTABLEA1" #Calibration table component containing wavelength (independet variable) and reference spectrum (dependent variable)
fdomNameUncrt <- "U_CVALA1" # Combined, standard uncertainty of fluorescent dissolved organic matter; provided by CVAL
tempCorrNameUncrt <- "U_CVALA4" # Combined, standard uncertainty of temperature correction function for fDOM; provided by CVAL
absCorrNameUncrt <- "U_CVALA5" # Combined, standard uncertainty of absorbance correction function for fDOM; provided by CVAL

# String constants for subfolders?


#Read in fdom data
fdomData <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile = paste0(repo,dataNamefdom),
                                                NameLib = "/ravro.so",
                                                log = log), silent = FALSE)
#fileData <- fileData[1]
if (base::class(fdomData) == 'try-error') {
  # Generate error and stop execution
  #log$error(base::paste0('File ', idxDirData, '/', fdomData, ' is unreadable.'))
  log$error(base::paste0('File: ', fdomData, ' is unreadable.'))
  base::stop()
}

# Validate the data
valiData <-
  NEONprocIS.base::def.validate.dataframe(
    dfIn = fdomData,
    TestNameCol = base::unique(c('readout_time', 'rawCalibratedfDOM')),
    log = log
  )
if (!valiData) {
  base::stop()
}

#Default all flags to -1 then change them as the test can be performed
fdomData$fDOMTempQF <- -1
fdomData$fDOMAbsQF<- -1
fdomData$spectrumCount <- -1

#Default expanded unceratinty to NA until it can be calculated
fdomData$fDOMExpUncert <- NA

#Default temp and absorbance correction factors
fdomData$tempFactor <- NA
fdomData$Abs_ex <- NA
fdomData$Abs_em <- NA
fdomData$absFactor <- NA

# # Read in the schemas so we only have to do it once and not every
# # time in the avro writer.
# if (!base::is.null(Para$FileSchmData)) {
#   # Retrieve and interpret the output data schema
#   SchmDataOutAll <-
#     NEONprocIS.base::def.schm.avro.pars(FileSchm = Para$FileSchmData, log =
#                                           log)
#   SchmDataOut <- SchmDataOutAll$schmJson
#   SchmDataOutVar <- SchmDataOutAll$var
# } else {
#   SchmDataOut <- NULL
# }
# if (!base::is.null(Para$FileSchmQf)) {
#   SchmQf <- base::paste0(base::readLines(Para$FileSchmQf), collapse = '')
# } else {
#   SchmQf <- NULL
# }

#Try to pull prt data if we're at a stream site? Holding on this.

#Use temp on conductivity probe, this data should have the uncertainty included?
tempData <- NEONprocIS.base::def.read.avro.deve(NameFile = paste0(repo,dataNameCond),
                                                NameLib = "/ravro.so")
#fileData <- fileData[1]
if (base::class(fdomData) == 'try-error') {
  # Generate error and stop execution
  #log$error(base::paste0('File ', idxDirData, '/', fdomData, ' is unreadable.'))
  log$error(base::paste0('File: ', tempData, ' is unreadable, temp corrections will not be applied.'))
  fdomData$fDOMTempQF <- 1
}else{
  # Validate the data
  valiData <-
    NEONprocIS.base::def.validate.dataframe(
      dfIn = tempData,
      TestNameCol = base::unique(c('readout_time', 'surfaceWaterTemperature')),
      log = log
    )
  if (!valiData) {
    fdomData$fDOMTempQF <- 1
  }
}

#Pull rho_fdom and pathlength from the appropriate cal file (use def.cal.meta.R and def.cal.slct.R to make that happen)
# Directory listing of cal files for this data stream
DirCal
DirCalVar <- base::paste0(DirCal, '/', idxVar)
fileCal <- base::dir(DirCalVar)

#Get metadata for all the calibration files in the directory, saving the valid start/end dates & certificate number
metaCal <-
  NEONprocIS.cal::def.cal.meta(fileCal = base::paste0(DirCalVar, '/', fileCal),
                               log = log)

calFilefdom <- NEONprocIS.cal::def.read.cal.xml(NameFile = paste0(repo,calNamefdom),Vrbs = FALSE)

#CVAL has only on rho_fdom value for all sensors and sites
rho_fdom <- try(base::as.numeric(calFilefdom$cal$Value[gsub(" ","",calFilefdom$cal$Name) == rhoNamefdom]))
uncrt_rho_fdom <- try(base::as.numeric(calFilefdom$cal$Value[gsub(" ","",calFilefdom$cal$Name) == tempCorrNameUncrt]))

#rho_fdom may not exist for some older data
if(base::is.null(rho_fdom)){
  fdomData$fDOMTempQF <- 1
}else{
  #Combine fdom and temp
  fdomData$readout_time_min <- base::format(fdomData$readout_time, format = "%Y-%m-%d %H:%M")
  tempData$readout_time_min <- base::format(tempData$readout_time, format = "%Y-%m-%d %H:%M")
  fdomData <- base::merge(fdomData, tempData, by = "readout_time_min", all.x = TRUE, suffixes = c(".fdom",".temp"))
  
  #Populate the fdom temperature QF values
  fdomData$fDOMTempQF[!base::is.na(fdomData$surfaceWaterTemperature)] <- 0
  fdomData$fDOMTempQF[base::is.na(fdomData$surfaceWaterTemperature)] <- 1
  
  #Determine fdom temperature correction factor
  fdomData$tempFactor <- 1/(1+rho_fdom*(fdomData$surfaceWaterTemperature-20))
}

#Determine fdom absorbance correction factor
#Pull pathlength and uncertainties for absorabnce from CVAL files
pathlength <- as.numeric(calFilefdom$cal$Value[gsub(" ","",calFilefdom$cal$Name) == pathNamefdom])

#pathlength doesn't exist for some older data, so don't even try to correct it if it isn't there
if(base::is.null(pathlength)){
  fdomData$fDOMAbsQF <- 1
}else{
  #Ross suggested that I use code from Guy for Abs_ex and Abs_em
  absData <- def.wq.abs.corr(NameFileSUNA, 
                              NameCalSUNA)
  
  #Distribute the absorbance data across the fDOM data
  for(i in length(absData$readout_time)){
    
  }
  
  #Populate the fdom absorbance QF values
  fdomData$fDOMAbsQF[!base::is.na(fdomData$Abs_ex)&!base::is.na(fdomData$Abs_em)] <- 0
  fdomData$fDOMAbsQF[base::is.na(fdomData$Abs_ex)|base::is.na(fdomData$Abs_em)] <- 1
  
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
#readout_time, fDOM, fDOMExpUncert, spectrumCount
dataOut <- fdomData[,c(4,19,20,9)]
names(dataOut) <- c("readout_time", "fDOM", "fDOMExpUncert", "spectrumCount")
NEONprocIS.base::def.wrte.avro.deve(data = dataOut)

#Write an AVRO file for the flags (which get metrics)
#readout_time, fDOMTempQF, fDOMAbsQF
flagsOut <- fdomData[,c(4,7,8)]
names(flagsOut) <- c("readout_time", "fDOMTempQF", "fDOMAbsQF")
filepathout <- "exofdom/CFGLOC12345/flags/exofdom_45831_2019-01-01_flags_fDOM.avro"
NEONprocIS.base::def.wrte.avro.deve(data = flagsOut, 
                                    NameFile = paste0(repo,filepathout), 
                                    NameSchm = "/scratch/pfs/avro_schemas/dp0p/flags_correction_exofdom.avsc", 
                                    NameLib = "/ravro.so" )



