##############################################################################################
#' @title Wrapper for barometric pressure flag and barometric pressure conversion

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Wrapper function. Calculates converted pressure and flags/removes pressure data if barometric pressure final QF is 1. 
#'
#'
#' @param DirIn Character value. The input path to the data from a single group ID, structured as follows: 
#' #/pfs/BASE_REPO/yyyy/mm/dd/group/#, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day.
#'
#' 
#' Nested within this path are the folders:
#'         /leveltroll400
#'         /leveltroll400/data
#'         /leveltroll400/flags
#'         /leveltroll400/location
#'         /pressure-air-buoy_*
#'         /pressure-air-buoy_*/data
#'         /pressure-air-buoy_*/group
#'         /pressure-air-buoy_*/location
#'         
#' For example:
#' Input path = pfs/subsurfMoorTempCond_group_path/2022/06/15/subsurf-moor-temp-cond_PRPO103100 with nested folders:
#'         /leveltroll400
#'         /leveltroll400/data
#'         /leveltroll400/flags
#'         /leveltroll400/location
#'         /pressure-air-buoy_PRPO103100
#'         /pressure-air-buoy_PRPO103100/data
#'         /pressure-air-buoy_PRPO103100/group
#'         /pressure-air-buoy_PRPO103100/location
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#'
#' @param SchmQfOut (optional) A json-formatted character string containing the schema for the flags output
#' by this function. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#'
#' @param SchmUcrtOut (optional) A json-formatted character string containing the schema for the uncertainty output
#' by this function. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#' 
#' @return Corrected pressure data and associated flags for missing bouy barometric pressure data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data', 'flags', and 'uncertainty' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' SchmDataOut <- base::paste0(base::readLines('~/pfs/leveltroll400_avro_schemas/leveltroll400/leveltroll400_baro_corrected.avsc'),collapse='')
#' wrap.troll.cond.conv <- function(DirIn="~/pfs/subsurfMoorTempCond_group_path/2022/06/15/subsurf-moor-temp-cond_PRPO103100",
#'                               DirOutBase="~/pfs/out",
#'                               SchmDataOut=SchmDataOut,
#'                               SchmQfOut=SchmQfOut,
#'                               SchmUcrt=SchmUcrtOut,
#'                               DirSubCopy=NULL,
#'                               log=log)
#'                               
#' 
#' @seealso None currently
#' 
##############################################################################################
wrap.troll.cond.conv <- function(DirIn,
                                 DirOutBase,
                                 SchmDataOut=NULL,
                                 SchmQfOut=NULL,
                                 SchmUcrtOut=NULL,
                                 DirSubCopy=NULL,
                                 log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  
  TrollDirIn <-
    def.dir.in.partial(DirBgn = DirIn,
                       nameDirSubPartial = 'leveltroll400',
                       log = log)
  DirInTrollCFGLOC <-
    NEONprocIS.base::def.dir.in(DirBgn = TrollDirIn,
                                nameDirSub = 'data',
                                log = log)
  DirInTrollData <- fs::path(DirInTrollCFGLOC,'data')
  DirInTrollFlags <- fs::path(DirInTrollCFGLOC,'flags')
  DirInTrollLoc <- fs::path(DirInTrollCFGLOC,'location')
  DirInTrollUcrt <- fs::path(DirInTrollCFGLOC,'uncertainty_data')
  DirInTrollUcrtCoef <- fs::path(DirInTrollCFGLOC,'uncertainty_coef')
  if(length(DirInTrollData)==0){
    # Generate error and stop execution
    log$error(base::paste0('No Troll data found in ', DirIn))
    base::stop()
  }
  InfoDirInTroll <- NEONprocIS.base::def.dir.splt.pach.time(DirInTrollCFGLOC)
  timeBgn <-  InfoDirInTroll$time # Earliest possible start date for the data
  fileOutSplt <- base::strsplit(DirInTrollCFGLOC,'[/]')[[1]] # Separate underscore-delimited components of the file name
  CFGLOC<-tail(x=fileOutSplt,n=1)
  
  BaroDirIn <-
    def.dir.in.partial(DirBgn = DirIn,
                       nameDirSubPartial = 'pressure',
                       log = log)
  DirInBaroData <- fs::path(BaroDirIn,'data')
  
  
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutPressure <- base::paste0(DirOut,'/pressure/',CFGLOC)
  base::dir.create(DirOutPressure,recursive=TRUE)
  DirOutData <- base::paste0(DirOutPressure,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirOutFlags <- base::paste0(DirOutPressure,'/flags')
  base::dir.create(DirOutFlags,recursive=TRUE)
  DirOutUcrt <- base::paste0(DirOutPressure,'/uncertainty_data')
  base::dir.create(DirOutUcrt,recursive=TRUE)
  
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=DirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }   
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirInTrollLoc) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirInTrollLoc),
                                       DirDest=DirOutPressure,
                                       LnkSubObj=FALSE,
                                       log=log)
  }   
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirInTrollUcrtCoef) > 0){
    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirInTrollUcrtCoef),
                                       DirDest=DirOutPressure,
                                       LnkSubObj=FALSE,
                                       log=log)
  }  
  
  # The flags folder is already populated from the calibration module. Copy over any existing files.
  fileCopy <- base::list.files(DirInTrollFlags,recursive=TRUE) # Files to copy over
  # Symbolically link each file
  for(idxFileCopy in fileCopy){
    cmdCopy <- base::paste0('ln -s ',base::paste0(DirInTrollFlags,'/',idxFileCopy),' ',base::paste0(DirOutFlags,'/',idxFileCopy))
    rptCopy <- base::system(cmdCopy)
  }
  # copy over uncertainty files
  fileCopy <- base::list.files(DirInTrollUcrt,recursive=TRUE) # Files to copy over
  # Symbolically link each file
  for(idxFileCopy in fileCopy){
    cmdCopy <- base::paste0('ln -s ',base::paste0(DirInTrollUcrt,'/',idxFileCopy),' ',base::paste0(DirOutUcrt,'/',idxFileCopy))
    rptCopy <- base::system(cmdCopy)
  }
  
  # Take stock of our data files. 
  fileTrollData <- base::list.files(DirInTrollData,full.names=FALSE)
  fileBaroData <- base::list.files(DirInBaroData,full.names=FALSE)
  
  # --------- Load the data ----------
  # Load in troll data file in parquet format into data frame 'data'. Grab the first file only, since there should only be one.
  fileTrollData <- fileTrollData[1]
  TrollData  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInTrollData, '/', fileTrollData),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(TrollData) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('Troll data file ', DirInTrollData, '/', fileTrollData, ' is unreadable.'))
    base::stop()
  }
  
  
  
  # Load in the barometric pressure 30min data file in parquet format
  fileBaroData <- fileBaroData[grepl('030',fileBaroData)][1]
  BaroData  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInBaroData, '/', fileBaroData),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(BaroData) == 'try-error')) {
    # Generate error and stop execution
    log$info(base::paste0('Baro data file for ', DirIn, ' does not exist or is unreadable.'))
    TrollData$baroPressure<-NA
    TrollData$baroPresExpUncert<-NA
    TrollData$baroPresQF<-1
  }else{
    
    # Check that files have same readout times
    if (!all(TrollData$readout_time %in% BaroData$startDateTime)) {
      log$error(base::paste0('Error: Troll and Baro data have different readout times for ', DirIn))
      stop()
    }
    
    # Create a match index for efficient joining
    match_idx <- match(TrollData$readout_time, BaroData$startDateTime)
    
    # Check for any unmatched times
    if (any(is.na(match_idx))) {
      log$warn(base::paste0('Warning: Some Troll times not found in Baro data for ', DirIn))
    }
    
    # Keep relevant BARO data using vectorized assignment
    TrollData$baroPressure <- BaroData$staPresMean[match_idx]
    TrollData$baroPresExpUncert <- BaroData$staPresExpUncert[match_idx]
    TrollData$baroPresQF <- BaroData$staPresFinalQF[match_idx]
    TrollData$baroPresQF[is.na(TrollData$baroPresQF)] <- 0
    
    # The surface water pressure is determined by subtracting the air pressure 
    # of the atmosphere from the measured pressure.
    TrollData$pressure_raw <- TrollData$pressure
    TrollData$pressure[!is.na(TrollData$baroPressure)] <- as.numeric(TrollData$pressure_raw[!is.na(TrollData$baroPressure)]) - as.numeric(TrollData$baroPressure[!is.na(TrollData$baroPressure)])
    TrollData$pressure[is.na(TrollData$baroPressure)] <- NA
  }
  
  #Create dataframe for output data
  dataOut <- TrollData
  dataCol <- c("source_id","readout_time","pressure","temperature")
  dataOut <- dataOut[,dataCol]
  
  #Create dataframe for just flags
  QFCol <- c("readout_time", "baroPresQF")
  flagsOut <- TrollData[,QFCol]
  
  #Create dataframe for just ucrt
  UcrtCol <- c("readout_time", "baroPresExpUncert")
  ucrtOut <- TrollData[,UcrtCol]
  
  #Turn necessary outputs to integer
  colInt <- c("baroPresQF") 
  flagsOut[colInt] <- base::lapply(flagsOut[colInt],base::as.integer) # Turn flags to integer
  
  # Write out data
  NameFileOutData <- base::paste0(DirOutData,"/leveltroll400_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),".parquet")
  rptDataOut <-
    base::try(NEONprocIS.base::def.wrte.parq(data = dataOut,NameFile = NameFileOutData,Schm = SchmDataOut),
              silent = TRUE)
  if (base::any(base::class(rptDataOut) == 'try-error')) {
    log$error(base::paste0('Cannot write Calibrated data to ',NameFileOutData,'. ',attr(rptDataOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Calibrated data written successfully in ', NameFileOutData))
  }
  
  #Write out flags
  NameFileOutFlags <- base::paste0(DirOutFlags,"/leveltroll400_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_flagsSpecific_baro.parquet")
  rptQfOut <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut,NameFile = NameFileOutFlags,Schm = SchmQfOut),silent=TRUE)
  if(base::any(base::class(rptQfOut) == 'try-error')){
    log$error(base::paste0('Cannot write flags to ',NameFileOutFlags,'. ',attr(rptQfOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', NameFileOutFlags))
  }
  
  #Write out ucrt
  NameFileOutUcrt <- base::paste0(DirOutUcrt,"/leveltroll400_",CFGLOC,"_",format(timeBgn,format = "%Y-%m-%d"),"_expn_ucrt_baro.parquet")
  rptUcrtOut <- try(NEONprocIS.base::def.wrte.parq(data = ucrtOut,NameFile = NameFileOutUcrt,Schm = SchmUcrtOut),silent=TRUE)
  if(base::any(base::class(rptUcrtOut) == 'try-error')){
    log$error(base::paste0('Cannot write barometric pressure uncertainty to ',NameFileOutUcrt,'. ',attr(rptUcrtOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Barometric pressure uncertainty written successfully in ', NameFileOutUcrt))
  }
  
} # End loop around datum paths

