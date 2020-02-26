###### For Testing Only ######

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

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut"),
                                      NameParaOptn=c("FileSchmData","FileSchmQf"),
                                      log=log)
# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve output schema for data
FileSchmDataOut <- Para$FileSchmData
log$debug(base::paste0('Output schema for data: ',base::paste0(FileSchmDataOut,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmDataOut) || FileSchmDataOut == 'NA'){
  SchmDataOut <- NA
} else {
  SchmDataOut <- base::paste0(base::readLines(FileSchmDataOut),collapse='')
}

# Retrieve output schema for flags
FileSchmQfOut <- Para$FileSchmQf
log$debug(base::paste0('Output schema for flags: ',base::paste0(FileSchmQfOut,collapse=',')))

# Read in the schema 
if(base::is.null(FileSchmQfOut) || FileSchmQfOut == 'NA'){
  SchmQfOut <- NULL
} else {
  SchmQfOut <- base::paste0(base::readLines(FileSchmQfOut),collapse='')
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'data'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,'data','threshold')))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub,log=log)

#Read in the L0, regularized fDOM data
file.exists("/scratch/pfs/avro_schemas/dp0p")
test <- NEONprocIS.base::def.read.avro.deve(NameFile = "/scratch/pfs/data_source/prt/3119/2019/01/01/3119.avro",
                                            NameLib = "/home/NEON/kcawley/NEON-IS-data-processing/pack/NEONprocIS.base/ravro.so")

#Sub-directories that we expect

#Apply temperature corrections (equation 7 in the ATBD)
#rho_fdom comes from CVAL
#temp data comes from PRT or EXO2 conductivity probe (need location context to decide which way to go)

fdom_tempFactor <- 1/(1-rho_fdom*(temp-20))

#Apply absorbance corrections (equation 6 in the ATBD)
#pathlength comes from CVAL
#Absorbance comes from SUNA cal table and SUNA L0 data

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


#Maybe we should have two functions and one flow that calls them rather than a big old flow
#What is the overhead associated with multiple functions versus one larger function?
#It is harder for other to read/work with larger functions?