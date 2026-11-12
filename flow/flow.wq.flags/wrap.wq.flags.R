##############################################################################################
#' @title Wrapper for low conductivity and buoy profiling

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Wrapper function. Flags all sensor streams for water quality when pressure is below one. 
#' Updates step flags for profiling.
#'
#'
#' @param DirIn Character value. The input path is structured as follows: #/pfs/BASE_REPO/yyyy/mm/dd/GROUP/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmQf (optional) A json-formatted character string containing the schema for the flags output
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
#' @return zeroConductivityQF and buoyProfilingQF.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.wq.flags <- function(DirIn="~/Git/pfs/waterQuality_temp_group/2025/09/04/water-quality_POSE102100",
#'                               DirOutBase="~/Git/pfs/out",
#'                               SchmQf=NULL,
#'                               DirSubCopy=NULL,
#'                               log=log)
#'                               
#' 
#' @seealso None currently
#' 
#' # changelog and author contributions / copyrights
#   Nora Catolico (2026-11-11)
#     original creation
##############################################################################################
wrap.wq.flags <- function(DirIn,
                          DirOutBase,
                          DirSubCopy=NULL,
                          SchmQf=NULL,
                          log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  DirOut <- base::paste0(DirOutBase, InfoDirIn$dirRepo)
  
  # ========== Load reference conductivity data ==========
  DirInCondData <- file.path(DirIn, "exo2conductivity")
  
  if(!dir.exists(DirInCondData)){
    log$error(base::paste0('Conductivity directory not found: ', DirInCondData))
    base::stop()
  }
  
  # Find conductivity config subdirectory (should be exactly one)
  cfgDirs <- base::list.files(DirInCondData, full.names = FALSE)
  cfgDirs <- cfgDirs[!base::grepl("^\\.", cfgDirs)]
  DirInCondDataFiles <- file.path(DirInCondData, cfgDirs[1], "data")
  
  
  # Load conductivity data file (should be exactly one)
  filesCond <- base::list.files(DirInCondDataFiles, pattern = "\\.parquet$|\\.parq$", 
                                full.names = FALSE)  
  if(length(filesCond) == 0){
    log$error(base::paste0('No parquet files found in ', DirInCondDataFiles))
    base::stop()
  }  
  if(length(filesCond) > 1){
    log$warn(base::paste0('Multiple conductivity files found; using first: ', filesCond[1]))
  }
  
  condData <- 
    base::try(NEONprocIS.base::def.read.parq(NameFile = file.path(DirInCondDataFiles, filesCond[1]),
                                              log = log),
                silent = FALSE)
  if (base::any(base::class(condData) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', file.path(DirInCondDataFiles, filesCond[1]), ' is unreadable.'))
    base::stop()
  } 

  # Validate required columns
  if(!("readout_time" %in% base::names(condData))){
    log$error('Conductivity data missing required column: readout_time')
    base::stop()
  }
  
  if(!("specificConductance" %in% base::names(condData))){
    log$error('Conductivity data missing required column: specificConductance')
    base::stop()
  }
  
  # Handle NA/NaN values explicitly (assign -1 for missing/invalid)
  na_mask <- base::is.na(condData$specificConductance) | base::is.nan(condData$specificConductance)
  condData$zeroConductivityQF[na_mask] <- base::as.integer(-1)
  
  # Apply threshold: conductance <= 1 = flag 1, > 1 = flag 0
  condData$zeroConductivityQF[!na_mask & condData$specificConductance <= 1] <- base::as.integer(1)
  condData$zeroConductivityQF[!na_mask & condData$specificConductance > 1] <- base::as.integer(0)
  
  # ========== Process all sensors ==========
  sensorDirs <- base::list.files(DirIn, full.names = FALSE)
  sensorDirs <- sensorDirs[!base::grepl("^\\.", sensorDirs)]  # Exclude hidden files
  
  for(sensor in sensorDirs){
    DirInSensor <- file.path(DirIn, sensor)
    
    # Find sensor config subdirectory (should be exactly one)
    cfgDirsSensor <- base::list.files(DirInSensor, full.names = FALSE)
    cfgDirsSensor <- cfgDirsSensor[!base::grepl("^\\.", cfgDirsSensor)]
    
    DirInFlags <- file.path(DirInSensor, cfgDirsSensor[1], "flags")
    DirOutFlags <- file.path(DirOut, sensor, cfgDirsSensor[1], "flags")
    base::dir.create(DirOutFlags, recursive = TRUE)
      
    # The flags folder is already populated from the calibration module. Copy over any existing files.
    fileCopy <- base::list.files(DirInFlags,recursive=TRUE) # Files to copy over
    # Symbolically link each file
    for(idxFileCopy in fileCopy){
      cmdCopy <- base::paste0('ln -s ',base::paste0(DirInFlags,'/',idxFileCopy),' ',base::paste0(DirOutFlags,'/',idxFileCopy))
      rptCopy <- base::system(cmdCopy)
    }
      
    #Load in plausibility flag file
    fileFlags <- base::list.files(DirInFlags,full.names=FALSE)
    filePlau <- fileFlags[grepl("Plausibility",fileFlags)]
    sensorFlags  <-
        base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(DirInFlags, '/', filePlau),
                                                log = log),
                  silent = FALSE)
    if (base::any(base::class(sensorFlags) == 'try-error')) {
        log$error(base::paste0('File ', DirInFlags, '/', filePlau, ' is unreadable.'))
        base::stop()
    }

    # Match and apply conductivity flags using keyed join on readout_time
    # Use match() for safe, explicit key-based join
    idxMatch <- base::match(sensorFlags$readout_time, condData$readout_time, nomatch = NA_integer_)
    
    # Initialize flags to -1 (no data)
    sensorFlags$zeroConductivityQF <- base::as.integer(-1)
    
    # Apply matched flags (where both have corresponding times)
    idxMatched <- !base::is.na(idxMatch)
    sensorFlags$zeroConductivityQF[idxMatched] <- 
      condData$zeroConductivityQF[idxMatch[idxMatched]]
    
    log$info(base::paste0('Sensor ', sensor, ': matched ', base::sum(idxMatched, na.rm = TRUE), 
                          ' of ', base::nrow(sensorFlags), ' records with conductivity data'))
    
    # Ensure flag column is integer type
    sensorFlags$zeroConductivityQF <- base::as.integer(sensorFlags$zeroConductivityQF)
    
    #Write out flags
    pathOutFlagFile <- base::paste0(DirOutFlags,"/",filePlau)

    rptQfOut <- try(NEONprocIS.base::def.wrte.parq(data = sensorFlags,NameFile = pathOutFlagFile,Schm = SchmQf),silent=FALSE)
    if(base::any(base::class(rptQfOut) == 'try-error')){
      log$error(base::paste0('Cannot write flags to ',DirOutFlags,'. ',attr(rptQfOut, "condition")))
      stop()
    } else {
      log$info(base::paste0('Flags written successfully in ', DirOutFlags))
    }   
    
  } # End sensor loop

} # End loop around datum paths
  
