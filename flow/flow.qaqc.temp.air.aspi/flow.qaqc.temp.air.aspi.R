##############################################################################################
#' @title Sensor-specific QA/QC for Single/Triple-Aspirated Air Temperature data products.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Sensor-specific QA/QC for Single/Triple-Aspirated Air Temperature data products. 
#' Evaluates heater status and adequate flow through the aspirated shield considering the turbine speed of 
#' the fan in the aspirated shield and the ambient wind speed. 
#' 
#' This script is run at the command line with 6+ arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the  path to input data directory (see below)
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are (at a minimum) folders with the same name as the the sensors indicated in 
#' input arguments SensTermTemp, SensTermTurb, SensTermHeat, and SensTermWindX. For example, if the 
#' temperature sensor is called 'prt', the heater called 'heater', two wind speed sensors, 'csat3' and 
#' 'windobserverii', are respectively indicated in SensTermWind1 and SensTermWind2, and the turbine is called 
#' 'dualfan', then the following subdirectories will be present in DirIn (or a child of DirIn):
#'         /prt
#'         /heater
#'         /dualfan
#'         /csat3
#'         /windobserverii
#'         
#' Within each of these folders must be a subdirectory for the location id of the sensor, and within that 
#' folder will be a subdirectory called 'data', in which a single data file resides. The only sensor folder
#' within which may exist multiple location ID folders or may exist multiple data files within a single 
#' location ID folder is the temperature sensor. In that case, each will be quality-checked against the 
#' single locations available for each the turbine, wind, and heater sensors. 
#' 
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "SensTermTemp=value", where the value is
#' 
#' 4. "SensTermTurb=value", where the value is
#' 
#' 5. "AvelTurbMin=value" (optional), where the value is . If not provided, the default value is 
#' 
#' 6. "SensTermWindX=value", where the value is
#' X is an integer indicating the priority order of the wind measurement to be used. In priority order (1 
#' being first priority), the sensor and associated term(s) will be checked for availability. If unavailable, 
#' the windspeed from the next available sensor-term set will be used.
#' If only one term is provided, it will be taken as the net horizontal wind speed. 
#' If two or more terms are provided, the will be treated as orthogonal vector components of the windspeed, 
#' for which their vector sum will be computed. At least SensTermWind1 is required, and there is a maximum 
#' of SensTermWind100. 
#' 
#' 7. "VeloWindMin=value" (optional), where the value is . If not provided, the default value is 
#' 
#' 8. "SensTermHeat=value", where the value is
#' 
#' 9.  "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN 
#' ORDER OF THE INPUT DATA.
#'  
#' 10. "FileSchmQf=value" (optional), where value is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). See below for details. 
#' 
#' 11. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the data folder in the input path that are to be copied with a 
#' symbolic link to the output path. 
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Filtered data and quality flags output in AVRO format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. Note that the 
#' 
#' If no output schema is provided for the flags, the output column/variable names will be 
#' readout_time, flowQF, heaterQF, in that order. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS 
#' MATCHES THIS ORDER OF THE INPUT ARGUMENTS. Otherwise, they will be labeled incorrectly.
#' 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007
#' NEON.DOC.000646 Revision E: ALGORITHM THEORETICAL BASIS DOCUMENT (ATBD) SINGLE ASPIRATED AIR TEMPERATURE
#' NEON.DOC.000654 Revision D: ALGORITHM THEORETICAL BASIS DOCUMENT (ATBD) TRIPLE ASPIRATED AIR TEMPERATURE


#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript 

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-10-09)

##############################################################################################
options(digits.secs = 3)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut","SensTermTemp","SensTermHeat",
                                                             "SensTermTurb","SensTermWind1"),
                                      NameParaOptn=c("AvelTurbMin","VeloWindMin","FileSchmQf","FileSchmQf",
                                                     "DirSubCopy",base::paste0("SensTermWind",2:100)),
                                      ValuParaOptn=base::list(AvelTurbMin=400,VeloWindMin=12),
                                      TypePara = base::list(AvelTurbMin='numeric',VeloWindMin='numeric'),
                                      log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve the sensor and term for temperature
SensTermTemp <- base::list(sens=Para$SensTermTemp[1],term=Para$SensTermTemp[2])
if(base::sum(base::is.na(base::unlist(SensTermTemp)))>0){
  log$fatal('Invalid parameter formulation for SensTermTemp. Check inputs and documentation.')
  stop()
}
------------------------START HERE ---------------------------------
#arg<-c("DirIn=/scratch/pfs/tempAirSingle_related_location_group_test/prt/","DirOut=/scratch/pfs/out","SensTermTemp=prt:temp","SensTermTurb=dualfan:turbineSpeed","SensTermWind1=windobserverii:uVectorComponent:vVectorComponent","SensTermHeat=heater:heaterFlag")

  
  
  # Retrieve the sensor and term for heater
SensTermTemp <- base::list(sens=Para$SensTermTemp[1],term=Para$SensTermTemp[2])
if(base::sum(base::is.na(base::unlist(SensTermTemp)))>0){
  log$fatal('Invalid parameter formulation for SensTermTemp. Check inputs and documentation.')
  stop()
}
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
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'stat'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
if("expUncert" %in% stat){
  dirSub <- c('data','uncertainty')
} else {
  dirSub <- c('data')
}
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,dirSub)))
log$debug(base::paste0('Minimum expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=nameDirSub)
if(base::length(DirIn) == 0){
  log$warn(base::paste0('No datums found for processing in parent directory ',DirBgn))
} else {
  log$info(base::paste0('Preparing to process ',base::length(DirIn),' datum(s).'))
}

# Create the binning for each aggregation interval
timeDmmyBgn <- base::as.POSIXlt('1970-01-01',tz='GMT')
timeDmmyEnd <- timeDmmyBgn + base::as.difftime(1,units='days') 
timeBgnDiff <- list()
timeEndDiff <- list()
for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
  
  # Time series of aggregation windows 
  timeBgnSeq <- base::as.POSIXlt(base::seq.POSIXt(from=timeDmmyBgn,to=timeDmmyEnd,by=base::format(WndwAgr[idxWndwAgr])))
  timeBgnSeq <- utils::head(timeBgnSeq,n=-1) # Lop off the last one at timeEnd 
  timeEndSeq <- timeBgnSeq + WndwAgr[idxWndwAgr] 
  if(utils::tail(timeEndSeq,n=1) != timeDmmyEnd){
    log$fatal(base::paste0('The aggregation interval must be an even divisor of one day. An aggregation interval of ',
                           WndwAgr[idxWndwAgr],' does not fit this requirement. Check inputs.'))
    stop()
  }
  
  timeBgnDiff[[idxWndwAgr]] <- timeBgnSeq - timeDmmyBgn # Add to timeBgn of each day to represent the starting time sequence
  timeEndDiff[[idxWndwAgr]] <- timeEndSeq - timeDmmyBgn # Add to timeBgn of each day to represent the end time sequence
} # End loop around aggregation intervals


# Process each datum path
for(idxDirIn in DirIn){
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Get directory listing of input directory. Expect subdirectories for data
  idxDirData <- base::paste0(idxDirIn,'/data')
  fileData <- base::dir(idxDirData)
  if(base::length(fileData) > 1){
    log$warn(base::paste0('There is more than one data file in path: ',idxDirIn,'... Computing statistics for them all!'))
  }
  
  # Gather info about the input directory (including date) and create the output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- timeBgn + base::as.difftime(1,units='days')
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)
  idxDirOutStat <- base::paste0(idxDirOut,'/stats')
  base::dir.create(idxDirOutStat,recursive=TRUE)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    base::suppressWarnings(NEONprocIS.base::def.copy.dir.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut))
    log$info(base::paste0('Unmodified subdirectories ',base::paste0(DirSubCopy,collapse=','),' of ',idxDirIn, ' copied to ',idxDirOut))
  }  
  
  # Are we computing uncertainty? If so, load the uncertainty file (there should be only 1)
  if("expUncert" %in% stat){
    idxDirUcrt <- base::paste0(idxDirIn,'/uncertainty')
    
    fileUcrt <- base::dir(idxDirUcrt)
    if(base::length(fileUcrt) != 1){
      log$warn(base::paste0('There are either zero or more than one uncertainty files in path: ',idxDirUcrt,'... Uncertainty stats will all be NA!'))
      ucrt <- base::list()
    } else {
      nameFileUcrt <- base::paste0(idxDirUcrt,'/',fileUcrt) # Full path to file
      
      # Open the uncertainty file
      ucrt  <- base::try(rjson::fromJSON(file=nameFileUcrt,simplify=TRUE),silent=FALSE)
      if(base::class(ucrt) == 'try-error'){
        # Generate error and stop execution
        log$error(base::paste0('File: ', nameFileUcrt, ' is unreadable.')) 
        stop()
      }
      # Turn times to POSIX
      ucrt <- base::lapply(ucrt,FUN=function(idxUcrt){
        idxUcrt$start_date <- base::strptime(idxUcrt$start_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        idxUcrt$end_date <- base::strptime(idxUcrt$end_date,format='%Y-%m-%dT%H:%M:%OSZ',tz='GMT')
        return(idxUcrt)
      })
    }
    
    # Is there a folder for fdas uncertainty data? If so, read it in!
    dataUcrtFdas <- NULL
    nameVarUcrtFdas <- NULL
    if('uncertainty_fdas' %in% base::dir(idxDirIn)){
      idxDirUcrtFdas <- base::paste0(idxDirIn,'/uncertainty_fdas')
      log$info(base::paste0('Detected FDAS uncertainty folder in ',idxDirUcrtFdas,'. Will attemp to to include FDAS uncertainty in expUncert for applicable variables.'))
      
      fileUcrtFdas <- base::dir(idxDirUcrtFdas)
      if(base::length(fileUcrtFdas) != 1){
        log$warn(base::paste0('There are either zero or more than one FDAS uncertainty files in path: ',idxDirUcrt,'... uncertainty for FDAS-applicable variables will be NA!'))
      } else {
        nameFileUcrtFdas <- base::paste0(idxDirUcrtFdas,'/',fileUcrtFdas) # Full path to file
        
        # Open the FDAS uncertainty file
        dataUcrtFdas  <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile=nameFileUcrtFdas,NameLib='/ravro.so'),silent=FALSE)
        if(base::class(dataUcrtFdas) == 'try-error'){
          log$error(base::paste0('File ', fileUcrtFdas,' is unreadable.')) 
          stop()
        } else {
          log$debug(base::paste0('Successfully read FDAS uncertainty data in file: ',fileUcrtFdas))
        }
        nameColUcrtFdas <- base::names(dataUcrtFdas) # Column names
        
        # Get names of variables that FDAS uncertainty applies
        nameColVarUcrtFdas <- base::unlist(base::lapply(base::strsplit(utils::tail(nameColUcrtFdas,n=-1),'_'),utils::head,n=1)) # for each column, minus the first (readout_time)
        nameVarUcrtFdas <- base::unique(nameColVarUcrtFdas) # the vars themselves (no duplicates)

      }
    } # End if statement around FDAS uncertainty
  } # End if statement around expUncert
    
  # Run through each file
  for(idxFileData in fileData){
    
    # Load in data file in AVRO format into data frame 'data'.  
    fileIn <- base::paste0(idxDirData,'/',idxFileData)
    data  <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile=fileIn,NameLib='/ravro.so'),silent=FALSE)
    if(base::class(data) == 'try-error'){
      log$error(base::paste0('File ', fileIn,' is unreadable.')) 
      stop()
    } else {
      log$debug(base::paste0('Successfully read in data file: ',fileIn))
    }
    nameVarIn <- base::names(data)
    
    # Pull out the time variable
    if(!('readout_time' %in% nameVarIn)){
      log$error(base::paste0('Variable "readout_time" is required, but cannot be found in file: ',fileIn)) 
      stop()
    }
    timeMeas <- base::as.POSIXlt(data$readout_time)
    
    # For each of the variables in the data file, go through and remove the FDAS uncertainties for any time points that were filtered (e.g. from the QA/QC step)
    if(base::sum(nameVarIn %in% nameVarUcrtFdas) > 0){
      for(idxVar in nameVarIn){
        # Do we have FDAS uncertainty data for this variable? If so, create NAs where they exist in the data
        dataUcrtFdas[base::is.na(data[[idxVar]]),base::unlist(base::lapply(base::strsplit(nameColUcrtFdas,'_'),utils::head,n=1))==idxVar] <- NA
      }      
    }

    
    # Run through each aggregation interval, creating the daily time series of windows
    for(idxWndwAgr in base::seq_len(base::length(WndwAgr))){
      
      log$debug(base::paste0('Computing stats for aggregation interval: ',WndwAgr[idxWndwAgr], ' minute(s)'))
      
      # Create start and end time sequences
      timeAgrBgn <- timeBgn + timeBgnDiff[[idxWndwAgr]]
      timeAgrEnd <- timeBgn + timeEndDiff[[idxWndwAgr]]
      timeBrk <- c(base::as.numeric(timeAgrBgn),base::as.numeric(utils::tail(timeAgrEnd,n=1))) # break points for .bincode
      
      # Intialize the output
      rpt <- base::data.frame(startDateTime=timeAgrBgn,endDateTime=timeAgrEnd)
      rpt[,3:(base::length(nameTermStat)+2)] <- base::as.numeric(NA)
      base::names(rpt)[3:(base::length(nameTermStat)+2)] <- nameTermStat
      
      # Allocate data points to aggregation windows
      setTime <- base::.bincode(base::as.numeric(timeMeas),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      
      # Allocate FDAS uncertainty data points to aggregation windows
      if(!base::is.null(dataUcrtFdas)){
        setTimeUcrtFdas <- base::.bincode(base::as.numeric(base::as.POSIXlt(dataUcrtFdas$readout_time)),timeBrk,right=FALSE,include.lowest=FALSE) # Which time bin does each measured value fall within?
      } else {
        setTimeUcrtFdas <- numeric(0)
      }
      
      
      # Run through the stats
      for(idxStat in base::names(statTerm)){
        
        log$debug(base::paste0('Computing ',idxStat, ' for terms: ', base:: paste0(statTerm[[idxStat]], collapse=',')))
        
        # Set up the output for the terms we'll compute this stat for
        statIdx <- base::subset(rpt,select=nameStatTerm[[idxStat]])
        
        # Run through the time bins
        for(idxWndwTime in base::unique(setTime)){

          # Rows to pull
          dataWndwTime <- base::subset(data,subset=setTime==idxWndwTime)  
          
          # Compute dependency stats we need for the chosen stats
          if(idxStat %in% c('numPts','stdEr','variance','expUncert')){
            numPts <- base::as.list(base::colSums(x=!base::is.na(base::subset(dataWndwTime,select=base::unique(c(statTerm[['numPts']],statTerm[['stdEr']],statTerm[['expUncert']])))),na.rm=FALSE))
            vari <- base::as.list(base::apply(X=base::subset(dataWndwTime,select=base::unique(c(statTerm[['variance']],statTerm[['stdEr']],statTerm[['expUncert']]))),MARGIN=2,FUN=stats::var,na.rm=TRUE))
            stdEr <- base::as.list(base::sqrt(base::unlist(vari[base::unique(c(statTerm[['stdEr']],statTerm[['expUncert']]))]))/base::sqrt(base::unlist(numPts[base::unique(c(statTerm[['stdEr']],statTerm[['expUncert']]))])))
          }
          
          # For uncertainty, we need the correct coefficients for each term
          if(idxStat %in% c('expUncert')){
            
            # Calibration uncertainty
            coefUcrtCal <- base::lapply(statTerm[['expUncert']],FUN=function(idxTerm){
              # Which uncertainty entries match this term, time period, and the uncertainty coef we want (U_CVALA3)
              mtch <- base::unlist(base::lapply(ucrt,FUN=function(idxUcrt){idxUcrt$term == idxTerm && idxUcrt$Name == 'U_CVALA3' && 
                  idxUcrt$start_date < timeAgrEnd[idxWndwTime] && idxUcrt$end_date > timeAgrBgn[idxWndwTime]}))
              
              # Pull the uncertainty coeffiecient
              if(base::sum(mtch) == 0){
                # If there are zero, the coef will be NA
                coefUcrtIdx <- base::as.numeric(NA)
              } else {
                # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
                coefUcrtIdx <- base::max(base::as.numeric(base::unlist(base::lapply(ucrt[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
              }
              return(coefUcrtIdx)
            })
            
            # FDAS uncertainty
            dataUcrtFdasWndwTime <- base::subset(dataUcrtFdas,subset=setTimeUcrtFdas==idxWndwTime) 
            ucrtFdasValu <- base::lapply(statTerm[['expUncert']],FUN=function(idxTerm){
              
              
              # Which uncertainty entries match this term, time period, and the uncertainty coef we want (U_CVALV3 or U_CVALR3 - only one should be populated)
              mtch <- base::unlist(base::lapply(ucrt,FUN=function(idxUcrt){idxUcrt$term == idxTerm && 
                  (idxUcrt$Name == 'U_CVALV3' || idxUcrt$Name == 'U_CVALR3') && 
                  idxUcrt$start_date < timeAgrEnd[idxWndwTime] && idxUcrt$end_date > timeAgrBgn[idxWndwTime]}))
              
              # Pull the uncertainty coefficient
              if(base::sum(mtch) == 0){
                # If there are zero, the coef will be 0
                coefUcrtFdas <- NA
              } else {
                # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
                coefUcrtFdas <- base::max(base::as.numeric(base::unlist(base::lapply(ucrt[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
              }

              # Which uncertainty entries match this term, time period, and the uncertainty coef we want (U_CVALV3 or U_CVALR3 - only one should be populated)
              mtch <- base::unlist(base::lapply(ucrt,FUN=function(idxUcrt){idxUcrt$term == idxTerm && 
                  (idxUcrt$Name == 'U_CVALV4' || idxUcrt$Name == 'U_CVALR4') && 
                  idxUcrt$start_date < timeAgrEnd[idxWndwTime] && idxUcrt$end_date > timeAgrBgn[idxWndwTime]}))
              
              # Pull the uncertainty coefficient
              if(base::sum(mtch) == 0){
                # If there are zero, the coef will be 0
                coefUcrtFdasOfst <- NA
              } else {
                # If there are more than 1, indicating that the averaging period spans two uncertainty application ranges, the coef will be the larger of the two
                coefUcrtFdasOfst <- base::max(base::as.numeric(base::unlist(base::lapply(ucrt[mtch],FUN=function(idxUcrt){idxUcrt$Value}))))
              }
              
              # Do some error checking
              if(base::is.na(coefUcrtFdas) && base::is.na(coefUcrtFdasOfst)){
                # No FDAS uncertainty applies
                ucrtFdasValuIdxTerm <- 0
                return(ucrtFdasValuIdxTerm)
              } else if (base::is.na(coefUcrtFdas) || base::is.na(coefUcrtFdasOfst)){
                # One of the terms is not present (but should be). Send back NA so the uncertainty is NA
                log$warn(base::paste0('One of the expected FDAS uncertainty coeffiecients is not present. Setting uncertainty to NA for term: ',idxTerm, ', averaging window ',idxWndwTime))
                ucrtFdasValuIdxTerm <- NA
                return(ucrtFdasValuIdxTerm)
              }
              
              # Do we have FDAS uncertainty data for this term?
              if (idxTerm %in% nameVarUcrtFdas){
                # Construct the column names of the output we want
                nameColRawIdx <- base::paste0(idxTerm,'_raw')
                nameColDervCalIdx <- base::paste0(idxTerm,'_dervCal')
                nameColUcrtCombIdx <- base::paste0(idxTerm,'_ucrtComb')
                
                # Find the index of the max combined standard measurement uncertainty 
                idxMax <- utils::head(base::which(dataUcrtFdasWndwTime[[nameColUcrtCombIdx]] == base::max(dataUcrtFdasWndwTime[[nameColUcrtCombIdx]],na.rm=TRUE)),n=1)
                
                # Compute the FDAS uncertainty
                if(!is.na(idxMax) && base::length(idxMax)==1){
                  ucrtFdasValuIdxTerm <-base::abs(dataUcrtFdasWndwTime[[nameColDervCalIdx]][idxMax])*(coefUcrtFdas*dataUcrtFdasWndwTime[[nameColRawIdx]][idxMax] + coefUcrtFdasOfst)
                } else {
                  ucrtFdasValuIdxTerm <- NA
                }
              } else if (!base::is.na(coefUcrtFdas+coefUcrtFdasOfst)){
                # We have FDAS uncertainty coefficients for this term, but there's no FDAS uncertainty data. Turn FDAS uncertainty to NA.
                ucrtFdasValuIdxTerm <- NA
              } else {
                # FDAS uncertainty not computed
                ucrtFdasValuIdxTerm <- 0
              }
              return(ucrtFdasValuIdxTerm)
              
            })     
            
          }
          
          # Compute the stat for this time window, for all the terms that need it. We will get a named vector, where the names correspond to the terms
          statIdx[idxWndwTime,] <- switch(idxStat,
                            mean=base::apply(X=base::subset(dataWndwTime,select=statTerm[['mean']]),MARGIN=2,FUN=base::mean,na.rm=TRUE),
                            median=base::apply(X=base::subset(dataWndwTime,select=statTerm[['median']]),MARGIN=2,FUN=stats::median,na.rm=TRUE),
                            minimum=base::suppressWarnings(base::apply(X=base::subset(dataWndwTime,select=statTerm[['minimum']]),MARGIN=2,FUN=base::min,na.rm=TRUE)),
                            maximum=base::suppressWarnings(base::apply(X=base::subset(dataWndwTime,select=statTerm[['maximum']]),MARGIN=2,FUN=base::max,na.rm=TRUE)),
                            sum=base::apply(X=base::subset(dataWndwTime,select=statTerm[['sum']]),MARGIN=2,FUN=base::sum,na.rm=TRUE),
                            variance=base::unlist(vari[statTerm[['variance']]]),
                            stdDev=base::apply(X=base::subset(dataWndwTime,select=statTerm[['stdDev']]),MARGIN=2,FUN=stats::sd,na.rm=TRUE),
                            stdEr=base::unlist(stdEr[statTerm[['stdEr']]]),
                            numPts=base::as.integer(base::unlist(numPts[statTerm[['numPts']]])),
                            expUncert=2*base::sqrt(base::unlist(stdEr[statTerm[['expUncert']]])^2 + base::unlist(coefUcrtCal[statTerm[['expUncert']]])^2 + base::unlist(ucrtFdasValu[statTerm[['expUncert']]])^2)
                    )
          
        } # End loop through time windows
        
        # Dole out these stats to the proper output
        rpt[,nameStatTerm[[idxStat]]] <- statIdx
        
      } # End loop through stats
      
      # Fix the Inf/-Inf results for max/min, respectively, when there is no non-NA data
      rpt[rpt==Inf] <- NA
      rpt[rpt==-Inf] <- NA
      
      
      # Write out the file for this aggregation interval
      fileStatOutSplt <- base::strsplit(idxFileData,'[.]')[[1]] # Try to grab the file name without extension
      if(base::length(fileStatOutSplt) > 1){
        NameFileOutStat <- base::paste0(idxDirOutStat,'/',base::paste0(fileStatOutSplt[-base::length(fileStatOutSplt)],collapse='.'),'_basicStats_',Para$WndwAgr[idxWndwAgr],'.',utils::tail(fileStatOutSplt,1))
      } else {
        NameFileOutStat <- base::paste0(idxDirOutStat,'/',idxFileData,'_basicStats_',Para$WndwAgr[idxWndwAgr])
      }
      rptWrte <- base::try(NEONprocIS.base::def.wrte.avro.deve(data=rpt,NameFile=NameFileOutStat,NameFileSchm=NULL,Schm=SchmStat,NameLib='/ravro.so'),silent=TRUE)
      if(base::class(rptWrte) == 'try-error'){
        log$error(base::paste0('Cannot write basic statistics file ', NameFileOutStat,'. ',attr(rptWrte,"condition"))) 
        stop()
      } else {
        log$info(base::paste0('Basic statistics written successfully in file: ',NameFileOutStat))
      }

      
    } # End loop around aggregation intervals
    
  } # End loop around files

} # End loop around datum paths 
  