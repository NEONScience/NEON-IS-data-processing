##############################################################################################
#' @title Regularization module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Regularization module for NEON IS data processing. 
#' This script is run at the command line with 6 to 9 arguments. Each argument must be a string in 
#' the format "Para=value", where "Para" is the intended parameter name and "value" is the value of 
#' the parameter. Note: If the "value" string begins with a $ (e.g. $DIR_IN), the value of the 
#' parameter will be assigned from the system environment variable matching the value string.
#'
#' The arguments are: 
#' 
#' 1. "DirIn=value", where value is the path to the input data directory. NOTE: This path must be a 
#' parent of the terminal directory where the data to be regularized reside. See argument "DirRglr" 
#' below to indicate the terminal directory.
#' 
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any 
#' number of parent and child directories of any name, so long as they are not 'pfs', the same name 
#' as the terminal directory indicated in argument "DirRglr", or recognizable as the 'yyyy/mm/dd' 
#' structure which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained 
#' in the folder. 
#'   
#' For example:
#' Input path = /scratch/pfs/proc_group/soilprt/27134/2019/01/01 
#'    
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3. "DirRglr=value", where value is the name of the terminal directory where the data to be 
#' regularized resides. This will be one or more child levels away from "DirIn". All files in the 
#' terminal directory will be regularized. The value may also be a vector of terminal directories, 
#' separated by pipes (|). All terminal directories must be present and at the same directory level.
#' For example, "DirRglr=data|flags" indicates to regularize the data files within each the data 
#' and flags directories.
#' 
#' 4. "FileSchmRglr=value", where value is the full path to schema for regularized data output by 
#' this workflow. The value may be NA, in which case the output schema will be the same as the input 
#' data. The value may be a single path, in which case it will apply to all 
#' regularized output, or a pipe-delimited vector the same length as the  DirRglr argument above, in 
#' which case each value will apply to the corresponding terminal directory of the DirRglr argument. 
#' NOTE: With the exception of the readout_time variable, any non-numeric data stream will be dropped 
#' in the regularized data. Ensure that the output schema reflects this constraint.
#' 
#' 5. "FreqRglr=value", where value is the regularization frequency in Hz. The value may be a single 
#' number, in which case it will apply to all terminal directories specified in the DirRglr argument, 
#' or it may be a pipe-delimited vector of values the same length as DirRglr, correpsonding to each 
#' respective terminal directory. 
#' 
#' 6. "MethRglr=value", where value is the regularization method (per the choices in 
#' eddy4R.base::def.rglr for input parameter MethRglr). The value may be a single string, in which 
#' case it will apply to all terminal directories specified in the DirRglr argument, or it may be a 
#' pipe-delimited vector of strings the same length as DirRglr, correpsonding to each respective 
#' terminal directory.
#' 
#' 7. "WndwRglr=value" (optional), where value is the windowing method (per the choices in 
#' eddy4R.base::def.rglr for input parameter WndwRglr). Do not include if the parameter is unused. The 
#' value may be a single string, in which case it will apply to all terminal directories specified in 
#' the DirRglr argument, or it may be a pipe-delimited vector of strings the same length as DirRglr, 
#' correpsonding to each respective terminal directory.
#' 
#' 8. "IdxWndw=value" (optional), where value is the index allocation method (per the choices in 
#' eddy4R.base::def.rglr for input parameter IdxWndw).  Do not include if the parameter is unused. The 
#' value may be a single string, in which case it will apply to all terminal directories specified in 
#' the DirRglr argument, or it may be a pipe-delimited vector of strings the same length as DirRglr, 
#' correpsonding to each respective terminal directory.
#' 
#' 9. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the regularization folder in the input path that are to be copied with a 
#' symbolic link to the output path.
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Regularized data output in AVRO format in DirOut, where DirOut directory 
#' replaces BASE_REPO but otherwise retains the child directory structure of the input path. 

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.rglr.R "DirIn=/pfs/prt_calibration/prt/2019/01/01" "DirOut=/pfs/out" "DirRglr=data" "FileSchmRglr=/pfs/avro_schemas/data_regularized.avsc" "FreqRglr=0.1" "MethRglr=CybiEc" "WndwRglr=Trlg" "IdxWndw=IdxWndwMin" 
#' 
#' Using environment variables for input directory and output file schema
#' Sys.setenv(DIR_IN='/pfs/prt_calibration/prt/2019/01/01')
#' Sys.setenv(FILE_SCHEMA_RGLR='/pfs/avro_schemas/data_regularized.avsc')
#' Rscript flow.rglr.R "DirIn=$DIR_IN" "DirOut=/pfs/out" "DirRglr=data" "FileSchmRglr=$FILE_SCHEMA_RGLR" "FreqRglr=0.1" "MethRglr=CybiEc" "WndwRglr=Trlg" "IdxWndw=IdxWndwMin" 


#' @seealso \code{\link[eddy4R.base]{def.rglr}}

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-02-16)
#     original creation with read/write in csv until AVRO R-package available
#   Cove Sturtevant (2019-02-20)
#     generalized script to accept command line arguments for input/output directory 
#       and regularization frequency
#   Cove Sturtevant (2019-03-21)
#     Read/write in AVRO format.
#     Output to same directory structure as input path.
#   Cove Sturtevant (2019-05-01)
#     add looping through datums, and added some additional logging
#   Cove Sturtevant (2019-05-10)
#     add hierarchical logging
#   Cove Sturtevant (2019-05-15)
#     Made detection of repos to process more flexible, and adjusted associated input(s)
#   Cove Sturtevant (2019-09-13)
#     added reading of input path from environment variables
#     simplified fatal errors 
#     adjusted the arguments to indicate the output schema for regularized data
#   Cove Sturtevant (2019-09-30)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#     allow regularizing of more than one directory, each with the same or separate regularization options
#   Cove Sturtevant (2019-10-01)
#     return regularized data as the same class it came in as
##############################################################################################
# Start logging
log <- NEONprocIS.base::def.log.init()

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <- NEONprocIS.base::def.arg.pars(arg=arg,NameParaReqd=c("DirIn","DirOut","DirRglr","FileSchmRglr","FreqRglr","MethRglr"),
                                      NameParaOptn=c("WndwRglr","IdxWndw","DirSubCopy"),TypePara=base::list(FreqRglr="numeric"),
                                      log=log)

# Retrieve datum path. 
DirBgn <- Para$DirIn # Input directory. 
log$debug(base::paste0('Input directory: ',DirBgn))

# Retrieve base output path
DirOut <- Para$DirOut
log$debug(base::paste0('Output directory: ',DirOut))

# Retrieve terminal directories to regularize
DirRglr <- Para$DirRglr
log$debug(base::paste0('Terminal Directories to regularize: ',base::paste0(DirRglr,collapse=',')))
numDirRglr <- base::length(DirRglr)

# Retrieve output schema(s)
FileSchmRglr <- Para$FileSchmRglr
log$debug(base::paste0('Output schema(s) for regularized data: ',base::paste0(FileSchmRglr,collapse=',')))

# Apply logic for extending single output schema to all directories, and error check
if(base::length(FileSchmRglr)==1 && numDirRglr > 1){
  FileSchmRglr=base::rep(x=FileSchmRglr,times=numDirRglr)
} else if(base::length(FileSchmRglr) != numDirRglr){
  log$fatal(base::paste0('Input argument FileSchmRglr must be of length 1 or the same length as the DirRglr argument.')) 
  base::stop()
}

# Read in the schema(s) 
SchmRglr <-list()
for(idxSchmRglr in 1:numDirRglr) {
  if(FileSchmRglr[idxSchmRglr] == 'NA'){
    SchmRglr[[idxSchmRglr]] <- NA
  } else {
    SchmRglr[[idxSchmRglr]] <- base::paste0(base::readLines(FileSchmRglr[idxSchmRglr]),collapse='')
  }
}
base::names(SchmRglr) <- DirRglr

# Retrieve regularization frequency
FreqRglr <- Para$FreqRglr
log$debug(base::paste0('Regularization frequency(ies): ',base::paste0(FreqRglr,collapse=',')))

# Apply logic for extending single value to all directories, and error check
if(base::length(FreqRglr)==1 && numDirRglr > 1){
  FreqRglr=base::rep(x=FreqRglr,times=numDirRglr)
} else if(base::length(FreqRglr) != numDirRglr){
  log$fatal(base::paste0('Input argument FreqRglr must be of length 1 or the same length as the DirRglr argument.')) 
  base::stop()
}
if(!base::is.null(FreqRglr)){
  base::names(FreqRglr) <- DirRglr
}

# Check that frequency is readable
if(base::sum(base::is.na(FreqRglr))>0){
  log$fatal('Cannot interpret regularization frequency as numeric') 
  stop()
}

# Retrieve regularization method
MethRglr <- Para$MethRglr
log$debug(base::paste0('Regularization method(s): ',base::paste0(MethRglr,collapse=',')))

# Apply logic for extending single value to all directories, and error check
if(base::length(MethRglr)==1 && numDirRglr > 1){
  MethRglr=base::rep(x=MethRglr,times=numDirRglr)
} else if(base::length(MethRglr) != numDirRglr){
  log$fatal(base::paste0('Input argument MethRglr must be of length 1 or the same length as the DirRglr argument.')) 
  base::stop()
}
if(!base::is.null(MethRglr)){
  base::names(MethRglr) <- DirRglr
}

# Retrieve windowing parameter
WndwRglr <- Para$WndwRglr
log$debug(base::paste0('Windowing parameter(s): ',base::paste0(WndwRglr,collapse=',')))

# Apply logic for extending single value to all directories, and error check
if(base::length(WndwRglr)==1 && numDirRglr > 1){
  WndwRglr=base::rep(x=WndwRglr,times=numDirRglr)
} else if(base::length(WndwRglr) != numDirRglr){
  log$fatal(base::paste0('Input argument WndwRglr must be of length 1 or the same length as the DirRglr argument.')) 
  base::stop()
}
if(!base::is.null(WndwRglr)){
  base::names(WndwRglr) <- DirRglr
}

# Retrieve Index allocation method
IdxWndw <- Para$IdxWndw
log$debug(base::paste0('Index allocation method(s): ',base::paste0(IdxWndw,collapse=',')))

# Apply logic for extending single value to all directories, and error check
if(base::length(IdxWndw)==1 && numDirRglr > 1){
  IdxWndw=base::rep(x=IdxWndw,times=numDirRglr)
} else if(base::length(IdxWndw) != numDirRglr){
  log$fatal(base::paste0('Input argument IdxWndw must be of length 1 or the same length as the DirRglr argument.')) 
  base::stop()
}
if(!base::is.null(IdxWndw)){
  base::names(IdxWndw) <- DirRglr
}

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,DirRglr))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(c(DirSubCopy,DirRglr))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=DirBgn,nameDirSub=DirRglr,log=log)

# Process each datum
for(idxDirIn in DirIn){

  log$info(base::paste0('Processing datum path: ',idxDirIn))
  
  # Gather info about the input directory (including date) and create the output directory. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(DirOut,InfoDirIn$dirRepo)

  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut,log=log)
  }  

  # Run through each directory to regularize
  for(idxDirRglr in DirRglr) {
    
    # Get directory listing of input directory
    idxDirInRglr <-  base::paste0(idxDirIn,'/',idxDirRglr)
    fileData <- base::dir(idxDirInRglr)
    if(base::length(fileData) > 1){
      log$warn(base::paste0('There is more than one data file in path: ',idxDirIn,'... Regularizing them all!'))
    }

    # Create output directory
    idxDirOutRglr <- base::paste0(idxDirOut,'/',idxDirRglr)
    base::dir.create(idxDirOutRglr,recursive=TRUE)
    
    # Regularize each file
    for(idxFileData in fileData){
      
      # Load in data file in AVRO format into data frame 'data'.  
      fileIn <- base::paste0(idxDirInRglr,'/',idxFileData)
      data  <- base::try(NEONprocIS.base::def.read.avro.deve(NameFile=fileIn,NameLib='/ravro.so',log=log),silent=FALSE)
      if(base::class(data) == 'try-error'){
        log$error(base::paste0('File ', fileIn,' is unreadable.')) 
        stop()
      }
      nameVarIn <- base::names(data)
        
      # Pull out the time variable
      if(!('readout_time' %in% nameVarIn)){
        log$error(base::paste0('Variable "readout_time" is required, but cannot be found in file: ',fileIn)) 
        stop()
      }
      timeMeas <- base::as.POSIXlt(data$readout_time)
      idxTime <- base::which(nameVarIn == 'readout_time')
      
      # Regularize the data
      BgnRglr <- base::as.POSIXlt(timeBgn)
      EndRglr <- base::as.POSIXlt(timeBgn+base::as.difftime(1,units='days'))
      dataRglr <- eddy4R.base::def.rglr(timeMeas=timeMeas,dataMeas=base::subset(data,select=-idxTime),
                                        BgnRglr=BgnRglr,EndRglr=EndRglr,FreqRglr=FreqRglr[idxDirRglr],
                                        MethRglr=MethRglr[idxDirRglr],WndwRglr=WndwRglr[idxDirRglr],
                                        IdxWndw=IdxWndw[idxDirRglr])
      
      # Make sure we return the regularized data as the same class it came in with
      for(idxVarRglr in base::names(dataRglr$dataRglr)){
        base::class(dataRglr$dataRglr[[idxVarRglr]]) <- base::class(data[[idxVarRglr]])
      }
      
      # Compile the regularized data
      rpt <- base::data.frame(readout_time=dataRglr$timeRglr,stringsAsFactors=FALSE)
      rpt <- base::cbind(rpt,dataRglr$dataRglr)
      
      # Remove any data points outside this day
      rpt <- rpt[rpt$readout_time >= BgnRglr & rpt$readout_time < EndRglr,]
      
      # If no schema was provided, use the same schema as the input data
      if(base::is.na(SchmRglr[[idxDirRglr]])){
        
        # Use the same schema as the input data to write the output data. 
        idxSchmRglr <- base::attr(data,'schema')

        # Columns may be missing (character columns are dropped in regularized output) and/or 
        # out of order. Add dummy columns for those that we're missing 
        nameVarAdd <- nameVarIn[!(nameVarIn %in% base::names(rpt))]
        numData <- base::nrow(rpt)
        for(idxVarAdd in nameVarAdd){
          rpt[[idxVarAdd]] <- base::rep(x=base::as.character(NA),times=numData)
        }
        # Rearrange to the original columns order
        rpt <- rpt[,nameVarIn]
      } else {
        idxSchmRglr <- SchmRglr[[idxDirRglr]]
      }

      # Write the output
      fileOut <- base::paste0(idxDirOutRglr,'/',idxFileData)
      rptWrte <- base::try(NEONprocIS.base::def.wrte.avro.deve(data=rpt,NameFile=fileOut,NameFileSchm=NULL,Schm=idxSchmRglr,NameLib='/ravro.so'),silent=TRUE)
      if(base::class(rptWrte) == 'try-error'){
        log$error(base::paste0('Cannot write regularized data in file ', fileOut,'. ',attr(rptWrte,"condition"))) 
        stop()
      } else {
        log$info(base::paste0('Regularized data written successfully in file: ',fileOut))
      }

      
    } # End loop around files to regularize
  } # End loop around directories to regularize
  
}
