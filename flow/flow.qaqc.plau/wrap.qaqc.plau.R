##############################################################################################
#' @title Basic QA/QC (plausibility) module for NEON IS data processing

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description Wrapper function. Basic QA/QC (plausibility) module for NEON IS data processing. Includes tests for 
#' null, gap, range, step, spike, and persistence. See eddy4R.qaqc package functions for details on each test.
#' General code workflow:
#'      Error-check input parameters
#'      Read regularization frequency from location file if expected
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Loop through all data files
#'        Regularize data in each file
#'        Write out the regularized data
#'
#' @param DirIn Character value. The path to parent directory where the data and thresholds exist. 
#' The input path is structured as follows: #/pfs/BASE_REPO/#/yyyy/mm/dd/#, where # indicates any number of 
#' parent and child directories of any name, so long as they are not 'pfs', the same name as subdirectories 
#' expected at the terminal directory (see below)), or recognizable as the 'yyyy/mm/dd' structure 
#' which indicates the 4-digit year, 2-digit month, and 2-digit day of the data contained in the folder.
#' 
#' Nested within the parent path must be the folders:
#'         /data 
#'         /threshold
#'         
#' The data folder holds any number of daily data files corresponding to the date in the input 
#' path and surrounding days. Names of data files MUST include the data date in the format %Y-%m-%d 
#' (YYYY-mm-dd). It does not matter where in the filename the date is denoted, so long as it is unambiguous.
#' 
#' The threshold folder holds a single file with QA/QC threshold information.
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param ParaTest A named list for each variable/term that quality tests will be applied to. The list element 
#' for each term is another list with the following named vectors: \cr
#' \code{term} Character vector. The name of the term to be tested. Must be present in the input data.
#' \code{test} Character vector. The name of the quality tests to run. Options are: 'null','gap','range','step','spike','persistence'
#' \code{rmv} Logical vector of same length as \code{test}, indicating for each corresponding test whether test failures should 
#' result in data removal (NA values). 
#' 
#' @param SchmDataOut (Optional). A json-formatted character string containing the schema for the output data. May be NULL (default), 
#' in which case the schema will be created automatically from the output data frame with the same variable names 
#' as the input data frame.
#' 
#' @param SchmQf (Optional). A json-formatted character string containing the schema for the output quality flags. May be NULL (default), 
#' in which case the  the variable names will be a camelCase combination of the term, 
#' the test, and the characters "QF", in that order. For example, if the input arguments 5-6 are 
#' "TermTest1=temp:null|range(rmv)" and "TermTest1=resistance:spike|gap" and the argument VarAddFileQf is omitted,
#' the output columns will be readout_time, tempNullQF, tempRangeQF, resistanceSpikeQF, resistanceGapQF, in that order. 
#' ENSURE THAT ANY PROVIDED OUTPUT SCHEMA FOR THE FLAGS MATCHES THE ORDER OF THE INPUT ARGUMENTS. 
#' Otherwise, they will be labeled incorrectly.
#' 
#' @param VarAddFileQf (Optional). Character vector. The names of any variables in the input data file
#' that should be copied over to the output flags files. Do not include readout_time.
#' In normal circumstances there should be none, as output flags files should only contain a timestamp and flags, 
#' but in rare cases additional variables may desired to be included in the flags files (such as source ID, site, 
#' or additional variables in the input file that already act as a quality flag. Defaults to NULL. Note that these will be 
#' tacked on to the end of the output columns produced by the selections in TermTest, and any output schema 
#' should account for this.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log (optional) A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

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

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run
#' ParaTest <- list(relativeHumidity=list(term='relativeHumidity',
#'                                        test=c("null","gap","range","step","spike","persistence"),
#'                                        rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
#'                                        ),
#'                  temperature=list(term='temperature',
#'                                   test=c("null","gap","range","step","spike","persistence"),
#'                                   rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
#'                                   ),
#'                  dewPoint=list(term='dewPoint',
#'                                test=c("null","gap","range","step","spike","persistence"),
#'                                rmv=c(FALSE,FALSE,TRUE,TRUE,FALSE,TRUE)
#'                                )
#'                  )
#' wrap.qaqc.plau(DirIn="~/pfs/relHumidity_padded_timeseries_analyzer/hmp155/2020/01/01/CFGLOC101252",
#'                DirOutBase="~/pfs/out",
#'                ParaTest=ParaTest,
#'                VarAddFileQf='errorState'
#' )

#' @seealso None currently

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-10-11)
#     Convert flow script to wrapper function
##############################################################################################
wrap.qaqc.plau <- function(DirIn,
                           DirOutBase,
                           ParaTest,
                           SchmDataOut=NULL,
                           SchmQf=NULL,
                           VarAddFileQf=NULL,
                           DirSubCopy=NULL,
                           log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Get directory listing of input directory. Expect subdirectories for data and threshold
  dirData <- base::paste0(DirIn,'/data')
  dirThsh <- base::paste0(DirIn,'/threshold')
  fileData <- base::dir(dirData)
  fileThsh <- base::dir(dirThsh)
  
  # Gather info about the input directory (including date) and create the output directories. 
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  timeBgn <-  InfoDirIn$time # Earliest possible start date for the data
  dirOut <- base::paste0(DirOutBase,InfoDirIn$dirRepo)
  dirOutData <- base::paste0(dirOut,'/data')
  base::dir.create(dirOutData,recursive=TRUE)
  dirOutQf <- base::paste0(dirOut,'/flags')
  base::dir.create(dirOutQf,recursive=TRUE)
  
  # Copy with a symbolic link the desired subfolders 
  if(base::length(DirSubCopy) > 0){
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn,'/',DirSubCopy),dirOut,log=log)
  }  
  
  # Preliminaries
  termTest <- base::names(ParaTest)
  # Create mapping between the names of the quality tests and their corresponding flags 
  mapNameQf <- base::data.frame(nameTest=c('null','gap','range','step','spike','persistence'),
                                nameQf=c('qfNull','qfGap','qfRng','qfStep','qfSpk','qfPers'),stringsAsFactors = FALSE)
  
  # Load in the data files and string together. 
  # Note: The data files are simply loaded in and sorted. There is no checking whether there are missing files
  # or gaps. This should be done in previous steps, along with any desired regularization.
  for (idxFileData in fileData){
    # Load in data file
    dataIdx  <- base::try(NEONprocIS.base::def.read.parq(NameFile=base::paste0(dirData,'/',idxFileData),log=log),silent=FALSE)
    if(base::any(base::class(dataIdx) == 'try-error')){
      log$error(base::paste0('File ', base::paste0(dirData,'/',idxFileData),' is unreadable.')) 
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
    log$info(base::paste0('There is more than one threshold file in ',dirThsh,'. Using ',fileThsh))
  }
  thsh <- NEONprocIS.qaqc::def.read.thsh.qaqc.df((NameFile=base::paste0(dirThsh,'/',fileThsh)))
  
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
      log$debug(base::paste0('Running plausibility tests: [',
                             base::paste0(ParaTest[[idxTerm]]$test,collapse=', '),
                             '] for term: ',
                             idxTerm))
      qf[[idxTerm]] <- base::do.call(eddy4R.qaqc::def.plau, argsPlau)[[idxTerm]]
      
    }
    
    # Run the despike test - get quality flags
    if('spike' %in% ParaTest[[idxTerm]]$test){
      
      # Run the spike test
      log$debug(base::paste0('Running spike test for term: ',idxTerm))
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
  qf <- base::do.call(base::cbind.data.frame, base::list(qf,stringsAsFactors=FALSE))
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
  if(base::is.null(SchmDataOut)){
    
    # Use the same schema as the input data to write the output data. 
    SchmDataOut <- base::attr(data,'schema')
    
  } 
  
  # Write the data
  NameFileOut <- base::paste0(dirOutData,'/',fileDataOut)
  rptData <- base::try(NEONprocIS.base::def.wrte.parq(data=dataOut,NameFile=NameFileOut,NameFileSchm=NULL,Schm=SchmDataOut),silent=TRUE)
  if(base::any(base::class(rptData) == 'try-error')){
    log$error(base::paste0('Cannot write Quality controlled data in file ', NameFileOut,'. ',attr(rptData,"condition"))) 
    stop()
  } else {
    log$info(base::paste0('Quality controlled data written successfully in ',NameFileOut))
  }
  
  # Write out the flags 
  NameFileOutQf <- NEONprocIS.base::def.file.name.out(nameFileIn = fileDataOut, prfx=base::paste0(dirOutQf,'/'), sufx='_flagsPlausibility')
  rptQf <- base::try(NEONprocIS.base::def.wrte.parq(data=qf,NameFile=NameFileOutQf,NameFileSchm=NULL,Schm=SchmQf),silent=TRUE)
  if(base::any(base::class(rptQf) == 'try-error')){
    log$error(base::paste0('Cannot write plausibility flags  in file ', NameFileOutQf,'. ',attr(rptQf,"condition"))) 
    stop()
  } else {
    log$info(base::paste0('Basic plausibility flags written successfully in ',NameFileOutQf))
  }
  
}
