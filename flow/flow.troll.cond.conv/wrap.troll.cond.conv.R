##############################################################################################
#' @title Wrapper for Missing Temp Flag and Conductivity Conversion

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Wrapper function. Flags conductivity for the Aqua Troll 200 when temperature stream is missing. 
#' Calculates specific conductance when temperature stream is available.
#'
#'
#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' 
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'         
#' For example:
#' Input path = pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/01/23681 with nested folders:
#'         /data
#'         /flags
#'         /uncertainty_coef
#'         /uncertainty_data
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#' 
#' @param SchmDataOut (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
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
#' @return Corrected conductivity data and associated flags for missing temperature data.
#' Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link. 
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' log <- NEONprocIS.base::def.log.init(Lvl = "debug")
#' wrap.troll.cond.conv <- function(DirIn="~/pfs/aquatroll200_calibration_group_and_convert/aquatroll200/2020/01/01/23681",
#'                               DirOutBase="~/pfs/out",
#'                               SchmDataOut="~/R/NEON-IS-avro-schemas/dp0p/aquatroll200_cond_corrected.avsc",
#'                               SchmQf="~/R/NEON-IS-avro-schemas/dp0p/flags_troll_specific_temp.avsc",
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
                               SchmQf=NULL,
                               DirSubCopy=NULL,
                               log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory (including date), and create base output directory
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  dirInData <- fs::path(DirIn,'data')
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  DirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  DirOutData <- base::paste0(DirOut,'/data')
  base::dir.create(DirOutData,recursive=TRUE)
  DirInFlags <- base::paste0(DirIn,'/flags')
  DirOutFlags <- base::paste0(DirOut,'/flags')
  base::dir.create(DirOutFlags,recursive=TRUE)
  
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- c('uncertainty_coef','uncertainty_data')
  if(base::length(DirSubCopy) > 0){
    
    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=DirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  
  # The flags folder is already populated from the calibration module. Copy over any existing files.
  fileCopy <- base::list.files(DirInFlags,recursive=TRUE) # Files to copy over
  # Symbolically link each file
  for(idxFileCopy in fileCopy){
    cmdCopy <- base::paste0('ln -s ',base::paste0(DirInFlags,'/',idxFileCopy),' ',base::paste0(DirOutFlags,'/',idxFileCopy))
    rptCopy <- base::system(cmdCopy)
  }
  
  # Take stock of our data files. 
  fileData <- base::list.files(dirInData,full.names=FALSE)
  
  # --------- Load the data ----------
  # Load in data file in parquet format into data frame 'data'. Grab the first file only, since there should only be one.
  fileData <- fileData[1]
  trollData  <-
    base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(dirInData, '/', fileData),
                                             log = log),
              silent = FALSE)
  if (base::any(base::class(data) == 'try-error')) {
    # Generate error and stop execution
    log$error(base::paste0('File ', dirData, '/', fileData, ' is unreadable.'))
    base::stop()
  }
  
  #create missing temperature flag; default all flags to -1 then change them as the test can be performed
  trollData$missingTempQF <- -1
  trollData$missingTempQF[!is.na(trollData$temperature) && trollData$temperature!="NA" && trollData$temperature!="NaN"]<-0
  trollData$missingTempQF[is.na(trollData$temperature)|trollData$temperature=="NA"|trollData$temperature=="NaN"]<-1
  source_id<-trollData$source_id[1]
  
  #convert actual conductivity to specific conductance
  trollData$specCond <- NA
  trollData$specCond <- trollData$conductivity/(1+0.0191*(trollData$temperature-25))
  trollData$specCond[trollData$missingTempQF>0]<-NA #If no temp stream, then do not output specific conductance. Could potentially report acutal conductivity in future. 
  
  #Create dataframe for output data
  dataOut <- trollData
  dataOut$raw_conductivity <- dataOut$conductivity #need to keep for later calculations
  dataOut$conductivity <- dataOut$specCond #replace actual conductivity with specific conductance
  dataCol <- c("source_id","site_id","readout_time","pressure","pressure_data_quality","temperature","temperature_data_quality","raw_conductivity","conductivity_data_quality","conductivity","internal_battery")
  dataOut <- dataOut[,dataCol]
  
  #Create dataframe for just flags
  QFCol <- c("readout_time", "missingTempQF")
  flagsOut <- trollData[,QFCol]
  
  #Turn necessary outputs to integer
  colInt <- c("missingTempQF") 
  flagsOut[colInt] <- base::lapply(flagsOut[colInt],base::as.integer) # Turn flags to integer
  

  # Write out data
  NameFileOutData <- base::paste0(DirOutData,"/aquatroll200_",source_id,"_",format(timeBgn,format = "%Y-%m-%d"),".parquet")
  rptDataOut <-
    base::try(NEONprocIS.base::def.wrte.parq(data = dataOut,NameFile = NameFileOutData,Schm = SchmDataOut),
    silent = FALSE)
  if (base::any(base::class(rptDataOut) == 'try-error')) {
    log$error(base::paste0('Cannot write Calibrated data to ',NameFileOutData,'. ',attr(rptDataOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Calibrated data written successfully in ', NameFileOutData))
  }
  
  #Write out flags
  NameFileOutFlags <- base::paste0(DirOutFlags,"/aquatroll200_",source_id,"_",format(timeBgn,format = "%Y-%m-%d"),"_flagsSpecificQc_Temp.parquet")
  rptQfOut <- try(NEONprocIS.base::def.wrte.parq(data = flagsOut,NameFile = NameFileOutFlags,Schm = SchmQf),silent=FALSE)
  if(class(rptQfOut) == 'try-error'){
    log$error(base::paste0('Cannot write flags to ',NameFileOutFlags,'. ',attr(rptQfOut, "condition")))
    stop()
  } else {
    log$info(base::paste0('Flags written successfully in ', NameFileOutFlags))
  }
  
} # End loop around datum paths
  
