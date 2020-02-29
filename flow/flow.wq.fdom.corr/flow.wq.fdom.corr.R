
###### For Testing Only ######
calFile <- NEONprocIS.cal::def.read.cal.xml(NameFile = "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/water-quality-001/sunav2/CFGLOC23456/calibration/rawNitrateSingleCompressedStream/30000000005365_WO33177_170813.xml",
                                            Vrbs = TRUE)
calFileTestTrue <- NEONprocIS.cal::def.read.cal.xml(NameFile = "/scratch/pfs/waterQuality_fdom_correction_group_test/2019/01/01/water-quality-001/exofdom/CFGLOC12345/calibration/fDOM/30000000016628_WO38587_197624.xml",
                                                Vrbs = FALSE)
calFileTestTrue$cal$Value[calFileTestTrue$cal$Name == "CVALA1"]


##############################

##############################################################################################
#' @title Workflow for correcting fDOM for temperature and absorbance

#' @author
#' Kaelin Cawley \email{kcawley@battelleecology.org}

#' @description Workflow. Apply temperature and absorbance corrections to fDOM data for the
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

#' @return Corrected fDOM data and associated flags for temperature and absorbance corrections.

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
dataNamefDOM <- "exofdom/CFGLOC12345/data/exofdom_45831_2019-01-01.avro"
dataNameCond <- "exoconductivity/CFGLOC12345/data/exoconductivity_43601_2019-01-01.avro"
dataNameprt <- "prt/CFGLOC101580/data/prt_CFGLOC101580_2019-01-01.avro"

calNameSUNA <- "sunav2/CFGLOC23456/calibration/rawNitrateSingleCompressedStream/30000000005365_WO33177_170813.xml"
calNamefDOM <- "exofdom/CFGLOC12345/calibration/fDOM/30000000023496_WO31715_162037.xml"

# String constants for CVAL files
rhoNamefDOM <- "CVALA1"
pathNamefDOM <- "CVALB1"
calTableNameSUNA <- "CVALTABLEA1"

#Read in fDOM data
fdomData <- NEONprocIS.base::def.read.avro.deve(NameFile = paste0(repo,dataNamefDOM),
                                                NameLib = "/home/NEON/kcawley/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")
#Try to pull prt data if we're at a stream site

#If there isn't any prt data or we're at a buoy site (or should this just be whatever reason and we don't even need to pull NL info?), use temp on conductivity probe
tempData <- NEONprocIS.base::def.read.avro.deve(NameFile = paste0(repo,dataNameCond),
                                                NameLib = "/home/NEON/kcawley/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")

#Pull rho_fDOM and pathlength from the appropriate cal file (use def.cal.meta.R and def.cal.slct.R to make that happen)
calFilefDOM <- NEONprocIS.cal::def.read.cal.xml(NameFile = paste0(repo,calNamefDOM),Vrbs = FALSE)
rho_fDOM <- as.numeric(calFilefDOM$cal$Value[gsub(" ","",calFilefDOM$cal$Name) == rhoNamefDOM])
pathlength <- as.numeric(calFilefDOM$cal$Value[gsub(" ","",calFilefDOM$cal$Name) == pathNamefDOM])

#Determine fDOM temperature correction factor
fdom_tempFactor <- 1/(1-rho_fdom*(temp-20))

#Determine fDOM absorbance correction factor

fdom_absFactor <- 10^((Abs_ex + Abs_em) * pathlength)

#Combine corrections if they both are created
if(!is.null(fdom_tempFactor) & !is.null(fdom_absFactor)){
  fdom_out <- fdom * fdom_tempFactor * fdom_absFactor
}else if(!is.null(fdom_tempFactor)){
  fdom_out <- fdom * fdom_tempFactor
  #Create flags for absorbance corrections
}else if(!is.null(fdom_absFactor)){
  fdom_out <- fdom * fdom_absFactor
  #Create flags for temp corrections?
  #Check for other source of temp data?
}else{
  fdom_out <- fdom_out
  #Create all the flags
  
}


#Create the uncertainty data and read in other unceratinty to output the final combined uncertainty

#Maybe we should have two functions and one flow that calls them rather than a big old flow
#What is the overhead associated with multiple functions versus one larger function?
#It is harder for other to read/work with larger functions?