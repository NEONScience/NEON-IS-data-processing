##############################################################################################
#' @title Basic QA/QC (plausibility) module for NEON IS data processing.

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Workflow. Basic QA/QC (plausibility) module for NEON IS data processing. Includes tests for 
#' null, gap, range, step, spike, and persistence. See eddy4R.qaqc package functions for details on each test.
#' 
#' This script is run at the command line with 5+ arguments. Each argument must be a string in 
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
#' Nested within this path are the folders:
#'         /data 
#'         /threshold
#'         
#' The data folder holds any number of daily data files corresponding to the date in the input 
#' path and surrounding days. Names of data files MUST include the data date in the format %Y-%m-%d 
#' (YYYY-mm-dd). It does not matter where in the filename the date is denoted, so long as it is unambiguous.
#' 
#' The threshold folder holds a single file with QA/QC threshold information. 
#'        
#' 2. "DirOut=value", where the value is the output path that will replace the #/pfs/BASE_REPO portion 
#' of DirIn. 
#' 
#' 3.  "FileSchmData=value" (optional), where values is the full path to the avro schema for the output data 
#' file. If this input is not provided, the output schema for the data will be the same as the input data
#' file. If a schema is provided, ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE DATA MATCHES THE COLUMN ORDER OF 
#' THE INPUT DATA.
#' 
#' 4. "FileSchmQf=value" (optional), where values is the full path to the avro schema for the output flags file. 
#' If this input is not provided, the output schema for the flags will be auto-generated from the output data 
#' frame. ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS (test 
#' nested within term/variable). See below for details. 
#' 
#' 5-N. "TermTestX=value", where X is a number beginning at 1 and value contains the (exact) names of the tests 
#' to be run for each term/variable. Begin each argument with the term name (e.g. temp), followed by a colon (:),
#' and then the tests to be run, delimited by pipes (|).  Test options are: null, gap, range, step, spike, 
#' persistence. Each of the test names can be appended with "(rmv)" to indicate that failures of the test 
#' remove the affected data point(s) (i.e. turn them to NA). Make sure there are no spaces between the term 
#' name, colon, test names and the optional (rmv) argument, nor between pipes. For example, 
#' "TermTest1=temp:range(rmv)|step|persistence" indicates that the range, step, and persistence tests are to be 
#' run for the term/variable temp. Failures of the range test will turn data values that fail this test to NA, 
#' while the step and persistence tests will retain the data values for which the tests fail (although still 
#' flagging them). There may be multiple assignments of TermTestX, specified by incrementing the 
#' number X by 1 with each additional argument. TermTest1 must be an input, and there is
#' a limit of X=100 for additional TermTestX arguments. 
#' 
#' N+1. "VarAddFileQf=value" (optional), where value contains the names of any variables in the input data file,
#' separated by pipes (|) that should be copied over to the output flags files. Do not include readout_time.
#' In normal circumstances there should be none, as flags files should only contain a timestamp and flags, 
#' but in rare cases additional variables may desired to be included in the flags files (such as source ID, site, 
#' or additional timing information produced from upstream modules). Defaults to empty. Note that these will be 
#' tacked on to the end of the output columns produced by the selections in TermTestX, and any output schema 
#' should account for this.
#' 
#' N+2. "DirSubCopy=value" (optional), where value is the names of additional subfolders, separated by 
#' pipes, at the same level as the data & threshold folders in the input path that are to be copied with a 
#' symbolic link to the output path. Be sure to specify 'DirSubCopy=threshold" if you want to retain this 
#' and any other directories in the output directory structure.
#' 
#' Note: This script implements logging described in \code{\link[NEONprocIS.base]{def.log.init}}, 
#' which uses system environment variables if available.
#' 
#' @return Filtered data and quality flags output in Parquet format in DirOut, where the terminal directory 
#' of DirOut replaces BASE_REPO but otherwise retains the child directory structure of the input path. 
#' Directories 'data' and 'flags' are automatically populated in the output directory, where the files 
#' for data and flags will be placed, respectively. The data and flags folders will include only the 
#' data/flags for the date indicated in the directory structure. Any other folders specified in argument
#' DirSubCopy will be copied over unmodified with a symbolic link.
#' 
#' The flags file will contain a column for the readout time followed by columns for quality flags grouped by 
#' variable/term in the same order as the variables/terms and the tests were provided in the input arguments 
#' (test nested within term), followed by additional variables, if any, specified in argument VarAddFileQf.
#'  
#' If no output schema is provided for the flags, the variable names will be a camelCase combination of the term, 
#' the test, and the characters "QF", in that order. For example, if the input arguments 5-6 are 
#' "TermTest1=temp:null|range(rmv)" and "TermTest1=resistance:spike|gap" and the argument VarAddFileQf is omitted,
#' the output columns will be readout_time, tempNullQF, tempRangeQF, resistanceSpikeQF, resistanceGapQF, in that order. 
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS. 
#' Otherwise, they will be labeled incorrectly.
#' 
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # From command line:
#' Rscript flow.qaqc.plau.R "DirIn=/pfs/proc_group/prt/2019/01/01/CFGLOC112083" "DirOut=/pfs/out" "FileSchmData=/outputDataSchema.avsc" "FileSchmQf=/outputQfSchema.avsc" "TermTest1=temp:null|gap|range(rmv)|step(rmv)|spike(rmv)|persistence"

#' @seealso 
#' \code{\link[eddy4R.qaqc]{def.plau}} \cr
#' \code{\link[eddy4R.qaqc]{def.dspk.wndw}} \cr

# changelog and author contributions / copyrights
#   Cove Sturtevant (2019-08-27)
#     original creation 
#   Cove Sturtevant (2019-10-01)
#     re-structured inputs to be more human readable
#     added arguments for output directory and optional copying of additional subdirectories
#   Cove Sturtevant (2019-10-02)
#     made inputs FileSchmData and FileSchmQf optional
#     fix bug returning all padded data
#   Cove Sturtevant (2019-11-27)
#     fix bug when only one test selected
#   Cove Sturtevant (2020-04-22)
#     switch read/write data from avro to parquet
#   Cove Sturtevant (2020-07-08)
#     Adjust code to accommodate new (faster!) despike algorithm and efficient plausibility code
#   Cove Sturtevant (2020-10-09)
#     Add check that terms slated for QA/QC are in the data
#   Cove Sturtevant (2021-02-04)
#     Add option to copy one or more variables found in the input file to the output flags file
#   Cove Sturtevant (2021-01-20)
#     Applied internal parallelization
##############################################################################################
library(foreach)
library(doParallel)

# Start logging
log <- NEONprocIS.base::def.log.init()

# Use environment variable to specify how many cores to run on
numCoreUse <- base::as.numeric(Sys.getenv('PARALLELIZATION_INTERNAL'))
numCoreAvail <- parallel::detectCores()
if (base::is.na(numCoreUse)){
  numCoreUse <- 1
} 
if(numCoreUse > numCoreAvail){
  numCoreUse <- numCoreAvail
}
log$debug(paste0(numCoreUse, ' of ',numCoreAvail, ' available cores will be used for internal parallelization.'))

# Pull in command line arguments (parameters)
arg <- base::commandArgs(trailingOnly=TRUE)

# Parse the input arguments into parameters
Para <-
  NEONprocIS.base::def.arg.pars(
    arg = arg,
    NameParaReqd = c("DirIn", "DirOut", "TermTest1"),
    NameParaOptn = c(
      "FileSchmData",
      "FileSchmQf",
      base::paste0("TermTest", 2:100),
      "VarAddFileQf",
      "DirSubCopy"
    ),
    log = log
  )

# Echo arguments
log$debug(base::paste0('Input directory: ', Para$DirIn))
log$debug(base::paste0('Output directory: ', Para$DirOut))



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

# Parse the terms and associated QA/QC tests to run, and whether test failures result in data removal 
nameParaTermTest <- base::names(Para)[names(Para) %in% base::paste0("TermTest",1:100)]
numTermTest<- base::length(nameParaTermTest)

spltTest <- Para[nameParaTermTest]
ParaTest <- base::lapply(spltTest,FUN=function(argSplt){
  base::list(term=argSplt[1],
             test=base::sub(pattern='(rmv)',replacement='',x=utils::tail(x=argSplt,n=-1),fixed=TRUE),
             rmv=base::grepl(pattern='(rmv)',x=utils::tail(x=argSplt,n=-1),fixed=TRUE))
})
termTest <- base::unlist(base::lapply(ParaTest,FUN=function(idx){idx$term})) # Pull out the terms to test
names(ParaTest) <- termTest

# Create mapping between the names of the quality tests and their corresponding flags 
mapNameQf <- base::data.frame(nameTest=c('null','gap','range','step','spike','persistence'),
                              nameQf=c('qfNull','qfGap','qfRng','qfStep','qfSpk','qfPers'),stringsAsFactors = FALSE)

# Retrieve variables to ignore in the flags files
VarAddFileQf <- setdiff(Para$VarAddFileQf,'readout_time')
log$debug(base::paste0('Variables from input files to be added to the output flags files: ',
                       base::paste0(VarAddFileQf,collapse=',')))

# Retrieve optional subdirectories to copy over
DirSubCopy <- base::unique(base::setdiff(Para$DirSubCopy,'data'))
log$debug(base::paste0('Additional subdirectories to copy: ',base::paste0(DirSubCopy,collapse=',')))

# What are the expected subdirectories of each input path
nameDirSub <- base::as.list(base::unique(c(DirSubCopy,'data','threshold')))
log$debug(base::paste0('Expected subdirectories of each datum path: ',base::paste0(nameDirSub,collapse=',')))

# Find all the input paths (datums). We will process each one.
DirIn <- NEONprocIS.base::def.dir.in(DirBgn=Para$DirIn,nameDirSub=nameDirSub,log=log)

# Process each datum path
doParallel::registerDoParallel(numCoreUse)
foreach::foreach(idxDirIn = DirIn) %dopar% {
  
  log$info(base::paste0('Processing path to datum: ',idxDirIn))
  
  # Get directory listing of input directory. Expect subdirectories for data and threshold
  idxDirData <- base::paste0(idxDirIn,'/data')
  idxDirThsh <- base::paste0(idxDirIn,'/threshold')
  fileData <- base::dir(idxDirData)
  fileThsh <- base::dir(idxDirThsh)
  
  # Gather info about the input directory (including date) and create the output directories. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  idxDirOut <- base::paste0(Para$DirOut,InfoDirIn$dirRepo)
  idxDirOutData <- base::paste0(idxDirOut,'/data')
  base::dir.create(idxDirOutData,recursive=TRUE)
  idxDirOutQf <- base::paste0(idxDirOut,'/flags')
  base::dir.create(idxDirOutQf,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn,'/',DirSubCopy),idxDirOut,log=log)
  }  
  
  
  # Load in the data files and string together. 
  # Note: The data files are simply loaded in and sorted. There is no checking whether there are missing files
  # or gaps. This should be done in previous steps, along with any desired regularization.
  for (idxFileData in fileData){
    # Load in data file 
    dataIdx  <- base::try(NEONprocIS.base::def.read.parq(NameFile=base::paste0(idxDirData,'/',idxFileData),log=log),silent=FALSE)
    if(base::any(base::class(dataIdx) == 'try-error')){
      log$error(base::paste0('File ', fileIn,' is unreadable.')) 
      stop()
    }
    
    # Initialize the data frame with the first data file
    if(idxFileData == fileData[1]){
      data <- dataIdx
    } else {
      data <- base::rbind(data,dataIdx)
    }
    
  } # End for loop around reading data files
  
  # Check that the data has the terms we are planning to do QA/QC on
  valiData <-
    NEONprocIS.base::def.validate.dataframe(dfIn = data,
                                            TestNameCol = base::unique(c(
                                              'readout_time', 
                                              termTest,
                                              VarAddFileQf
                                            )),
                                            log = log)
  if(valiData != TRUE){
    stop()
  }
  
  # Sort the data by readout_time
  data <- data[base::order(data$readout_time),]
  dataOut <- data # initialize output
  
  # Read in the thresholds file (read first file only, there should only be 1)
  if(base::length(fileThsh) > 1){
    fileThsh <- fileThsh[1]
    log$info(base::paste0('There is more than one threshold file in ',idxDirThsh,'. Using ',fileThsh))
  }
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df((NameFile=base::paste0(idxDirThsh,'/',fileThsh)))
  
  # Verify that the terms listed in the input parameters are included in the threshold files
  exstThsh <- termTest %in% base::unique(thsh$term_name) # Do the terms exist in the thresholds
  if(base::sum(exstThsh) != base::length(termTest)){
    log$error(base::paste0('Thresholds for term(s): ',base::paste(termTest[!exstThsh],collapse=','),' do not exist in the thresholds file. Cannot proceed.')) 
    stop()
  }
  
  # Intialize output
  qf <- base::list()
  
  # Test each term
  for(idxTerm in termTest){
    
    # Check that the tests to run are wholly contained in the tests run by this code
    chkTest <- ParaTest[[idxTerm]]$test %in% mapNameQf$nameTest
    if(base::sum(!chkTest) > 0){
      log$fatal(base::paste0('Requested tests: ',base::paste0(ParaTest[[idxTerm]]$test[!chkTest],collapse=','),' are not tests run by this code. Aborting.')) 
      stop()
    }
    
    # Filter to thresholds only for this term
    thshIdxTerm <- thsh[thsh$term_name == idxTerm,]
    
    # Initialize the arguments for plausibility and spike testing (these are run by separate codes)
    argsPlau <- base::list(data=base::subset(data,select=idxTerm),time=base::as.POSIXlt(data$readout_time))
    argsSpk <- base::list(data=base::subset(data,select=idxTerm))

    # Argument(s) for null test
    if('null' %in% ParaTest[[idxTerm]]$test){
      
      argsPlau$TestNull <- TRUE
      
    }
    
    # Argument(s) for gap test
    if('gap' %in% ParaTest[[idxTerm]]$test){
      
      argsPlau$NumGap <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Gap Test value - # missing points']
      
      # Check that thresholds exist for this test
      if(base::length(argsPlau$NumGap) == 0){
        log$error(base::paste0('"Gap Test value - # missing points" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
    }    
    
    # Argument(s) for range test
    if('range' %in% ParaTest[[idxTerm]]$test){
      
      argsPlau$RngMin <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Range Threshold Hard Min']
      argsPlau$RngMax <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Range Threshold Hard Max']
      
      # Check that thresholds exist for this test
      if(base::length(argsPlau$RngMin) == 0){
        log$error(base::paste0('"Range Threshold Hard Min" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      if(base::length(argsPlau$RngMax) == 0){
        log$error(base::paste0('"Range Threshold Hard Max" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
    }    
    
    # Argument(s) for step test
    if('step' %in% ParaTest[[idxTerm]]$test){
      
      argsPlau$DiffStepMax <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Step Test value']
      
      # Check that thresholds exist for this test
      if(base::length(argsPlau$DiffStepMax) == 0){
        log$error(base::paste0('"Step Test value" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
    }   
    
    # Argument(s) for persistence test
    if('persistence' %in% ParaTest[[idxTerm]]$test){
      
      argsPlau$DiffPersMin <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Persistence (change)']
      argsPlau$WndwPers <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Persistence (time - seconds)']
      
      # Check that thresholds exist for this test
      if(base::length(argsPlau$DiffPersMin) == 0){
        log$error(base::paste0('"Persistence (change)" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      if(base::length(argsPlau$WndwPers) == 0){
        log$error(base::paste0('"Persistence (time - seconds)" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      
      # Convert persistence window to difftime object
      argsPlau$WndwPers <- base::as.difftime(argsPlau$WndwPers,units='secs') # Create difftime object so the code knows the window is in seconds
    }   
    
    # Argument(s) for spike test
    if('spike' %in% ParaTest[[idxTerm]]$test){
      
      SpkMeth <- thshIdxTerm$string_value[thshIdxTerm$threshold_name == 'Despiking Method']
      SpkMad <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking MAD']
      SpkWndw <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking window size - points']
      SpkWndwStep <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking window step - points.']
      SpkNumPtsGrp <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking maximum consecutive points (n)']
      SpkNaFracMax <- thshIdxTerm$number_value[thshIdxTerm$threshold_name == 'Despiking maximum (%) missing points per window']
      
      # Check that thresholds exist for this test
      if(base::length(SpkMeth) == 0){
        log$error(base::paste0('"Despiking Method" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      if(base::length(SpkMad) == 0){
        log$error(base::paste0('"Despiking MAD" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      if(base::length(SpkWndw) == 0){
        log$error(base::paste0('"Despiking window size - points" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      if(base::length(SpkWndwStep) == 0){
        log$error(base::paste0('"Despiking window step - points." not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      if(base::length(SpkNumPtsGrp) == 0){
        log$error(base::paste0('"Despiking maximum consecutive points (n)" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      if(base::length(SpkNaFracMax) == 0){
        log$error(base::paste0('"Despiking maximum (%) missing points per window" not found in thresholds for term: ',idxTerm,'. Cannot proceed.')) 
        stop()
      }
      
      # If the spike window is even, add 1 to make it odd.
      if(SpkWndw %% 2 == 0){
        SpkWndw <- SpkWndw + 1
      }
      
      # Turn SpkNaFracMax to fraction (from %)
      SpkNaFracMax <- SpkNaFracMax/100
      
    }   
    
    # Initialize quality flag output
    qf[[idxTerm]] <- NULL
    
    # Run the plausibility tests - get quality flags values for all tests
    if(base::sum(c('null','gap','range','step','persistence') %in% ParaTest[[idxTerm]]$test) > 0){
      
      # Set some additional arguments
      argsPlau$Vrbs=TRUE # Outputs quality flag values instead of vector positions
      
      # Run the tests
      log$debug('Running plausibility tests (may include null, gap, range, step, persistence)')
      qf[[idxTerm]] <- base::do.call(eddy4R.qaqc::def.plau, argsPlau)[[idxTerm]]
      
    }
    
    # Run the despike test - get quality flags
    if('spike' %in% ParaTest[[idxTerm]]$test){
      
      # Run the spike test
      log$debug('Running spike test')
      qfSpk <- NEONprocIS.qaqc::def.spk.mad(data=data[[idxTerm]],Meth=SpkMeth,ThshMad=SpkMad,Wndw=SpkWndw,WndwStep=SpkWndwStep,WndwFracSpkMin=0.1,NumGrp=SpkNumPtsGrp,NaFracMax=SpkNaFracMax,log=log)
      names(qfSpk) <- 'qfSpk'

      if(base::is.null(qf[[idxTerm]])){
        qf[[idxTerm]] <- qfSpk
      } else {
        qf[[idxTerm]] <- base::cbind(qf[[idxTerm]],qfSpk)
      }
    }
    
    # Retain the output from the requested tests and order the flags in the order they came in from the arguments 
    # (so that it is apparent how the output schema should be ordered), and so we know which failed tests result in NA data.
    setTest <- base::unlist(base::lapply(ParaTest[[idxTerm]]$test, base::grep,x=mapNameQf$nameTest,fixed=TRUE))
    qf[[idxTerm]] <- base::subset(x=qf[[idxTerm]],select=mapNameQf$nameQf[setTest])
    
    # Remove data (turn to NA) for failed test results if requested
    dataOut[[idxTerm]][base::apply(X=base::subset(x=qf[[idxTerm]],select=ParaTest[[idxTerm]]$rmv),MARGIN=1,FUN=base::sum) > 0] <- NA
    
    # prep the column names for final output (term name as prefix)
    names(qf[[idxTerm]])<- base::paste0(base::paste(base::toupper(base::substr(mapNameQf$nameTest[setTest],1,1)),
                                                    base::substr(mapNameQf$nameTest[setTest],2,base::nchar(mapNameQf$nameTest[setTest])),sep=""),"QF")
  }
  
  # Combine the output for all terms into a single data frame - this will insert the name of the term in the column name
  qf <- base::do.call(base::rbind.data.frame, base::list(qf,make.row.names = FALSE,stringsAsFactors=FALSE))
  base::names(qf) <- base::sub(pattern='.',replacement='',x=base::names(qf),fixed=TRUE) # Get rid of the '.' between the term name and the flag name
  
  # Use as.integer in order to write out as integer with the avro schema
  qf <- base::apply(X=qf,MARGIN=2,FUN=base::as.integer)
  
  # Add in the time variable and any variables we want to copy into the flags files
  qf <- base::cbind(data['readout_time'],qf,base::subset(data,select=VarAddFileQf))

  # Retain only the flags and data for the data date we are interested in
  setKeep <- qf$readout_time >= timeBgn & qf$readout_time < timeBgn+base::as.difftime(1,units='days')
  qf <- qf[setKeep,]
  dataOut <- dataOut[setKeep,]
  
  
  # Determine the input filename we will base our output filename on - it is the filename with this data day embedded
  fileDataOut <- fileData[base::grepl(pattern=base::format(timeBgn,'%Y-%m-%d'),x=fileData)] 
  
  # Error if we cannot interpret the date from the file name, otherwise issue a warning if the correct file is ambiguous
  if(base::length(fileDataOut) == 0){
    log$error(base::paste0('There are no input data file names that contain the datum date in the file name. more than one input data filename matching the datum date. ',idxTerm,'. Cannot proceed.')) 
    stop()
  } else if (base::length(fileDataOut) > 1){
    fileDataOut <- fileDataOut[1]
    log$warn(base::paste0('There is more than one input data filename matching the datum date. Patterning the output file on the first: ',fileDataOut))
  }
  
  # If no schema was provided for the data, use the same schema as the input data
  if(base::is.na(SchmDataOut)){
    
    # Use the same schema as the input data to write the output data. 
    idxSchmDataOut <- base::attr(data,'schema')
    
  } else {
    idxSchmDataOut <- SchmDataOut
  }
  
  # Write the data
  NameFileOut <- base::paste0(idxDirOutData,'/',fileDataOut)
  rptData <- base::try(NEONprocIS.base::def.wrte.parq(data=dataOut,NameFile=NameFileOut,NameFileSchm=NULL,Schm=idxSchmDataOut),silent=TRUE)
  if(base::any(base::class(rptData) == 'try-error')){
    log$error(base::paste0('Cannot write Quality controlled data in file ', NameFileOut,'. ',attr(rptData,"condition"))) 
    stop()
  } else {
    log$info(base::paste0('Quality controlled data written successfully in ',NameFileOut))
  }
  
  # Write out the flags 
  NameFileOutQf <- NEONprocIS.base::def.file.name.out(nameFileIn = fileDataOut, prfx=base::paste0(idxDirOutQf,'/'), sufx='_flagsPlausibility')
  rptQf <- base::try(NEONprocIS.base::def.wrte.parq(data=qf,NameFile=NameFileOutQf,NameFileSchm=NULL,Schm=SchmQfOut),silent=TRUE)
  if(base::any(base::class(rptQf) == 'try-error')){
    log$error(base::paste0('Cannot write plausibility flags  in file ', NameFileOutQf,'. ',attr(rptQf,"condition"))) 
    stop()
  } else {
    log$info(base::paste0('Basic plausibility flags written successfully in ',NameFileOutQf))
  }
  
  return()
} # End loop around datum paths


