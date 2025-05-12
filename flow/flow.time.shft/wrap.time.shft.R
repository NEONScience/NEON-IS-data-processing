##############################################################################################
#' @title Combine padded data output and shift time

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr
#' Teresa Burlingame \email{tburlingame@battelleecology.org} \cr


#' @description Wrapper function. Merge multiple L0 data files generated for three days. If in-situ
#' data processing delays output of values, data can be back corrected to real time. 
#' Multiple days of data are needed to correct offsets. Only saves the central day.
#'

#' @param DirIn Character value. The input path to the data from a single source ID, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/source-id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The source-id is the unique identifier of the sensor. \cr
#'
#' Nested within this path is the folder:
#'         /data
#' The data folder holds any number of data files from kafka with the naming format:
#' SOURCETYPE_SOURCEID_YYYY-MM-DD.parquet
#' 
#' For example:
#' Input path = /scratch/pfs/pluvio_data_parser/pluvio/2023/04/02/11346/data/ with nested files:
#'    pluvio_55221_2025-04-01.parquet
#'    pluvio_55221_2025-04-02.parquet
#'    pluvio_55221_2025-04-03.parquet
#'    manifest.txt
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param TimeShft numeric value. The amount of time to shift datums 
#' 
#' @param TimeShft character value. The unit of time shift
#' #' 
#' @param TimeShftDir character value. Constrained to Pos or Neg. Indicates direction of time shift. 
#' eg Neg 5 mins would change 00:00:00 to 23:55:00

#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the data folder(s) in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. carried through as-is). Note that the 'data' directory is automatically
#' populated in the output and cannot be included here.

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A repository in DirOutBase containing the time shifted l0 output, where DirOutBase replaces BASE_REPO 
#' of argument \code{DirIn} but otherwise retains the child directory structure of the input path. 
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' DirIn <- '/scratch/pfs/pluvio_data_parser/pluvio/2023/03/02/27733'
#' DirOutBase <- '/scratch/pfs/out'
#' FileSchmL0 <- '~/R/avro_schemas/schemas/pluvio/pluvio_parsed.avsc' # L0 schema
#' wrap.time.shft(DirIn,DirOutBase,FileSchmL0, TimeShft, TimeUnit, TimeShftDir)

#' @seealso Currently none

# changelog and author contributions / copyrights
#  Teresa Burlingame (2025-04-10)
#   Code modified from wrap.kfka.comb

##############################################################################################
wrap.time.shft <- function(DirIn,
                           DirOutBase,
                           TimeShft,
                           TimeUnit,
                           TimeShftDir,
                           DirSubCopy=NULL,
                           log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  if(!(is.numeric(TimeShft) | is.na(TimeShft))){
    log$error(base::paste0('TimeShft must be a numeric or integer unit to proceed')) 
    stop()
  }
  
  #not robust to totally biffing the time unit # TODO
  if(!(TimeUnit %in% c( "secs", "mins", "hours","days", "weeks"))) {
    log$error('Time Shift Unit invalid, must be "secs", "mins", "hours","days" or "weeks"')
    stop()
  }
  
  #not robust to totally biffing the time unit # TODO
  if(!(TimeShftDir %in% c( "Pos", "Neg"))) {
    log$error('Time Shift direction invalid, must be "Pos" or "Neg"')
    stop()
  }

  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  timeBgn <- InfoDirIn$time
  dirInData <- fs::path(DirIn,'data')
  dirOut <- fs::path(DirOutBase,InfoDirIn$dirRepo)
  dirOutData <- fs::path(dirOut,'data')
  NEONprocIS.base::def.dir.crea(DirBgn = dirOut,
                                DirSub = 'data',
                                log = log)
  
  # Copy with a symbolic link the desired subfolders 
  DirSubCopy <- base::unique(base::setdiff(DirSubCopy,'data'))
  if(base::length(DirSubCopy) > 0){

    NEONprocIS.base::def.dir.copy.symb(DirSrc=fs::path(DirIn,DirSubCopy),
                                       DirDest=dirOut,
                                       LnkSubObj=FALSE,
                                       log=log)
  }    
  

  # Take stock of our data files. 
  fileData <-  base::list.files(dirInData,pattern='.parquet',full.names=FALSE)

  # Check for a manifest file. Ensures full pad.
  fileManifest <- base::list.files(dirInData,pattern='manifest',full.names=TRUE)
  
  if(length(fileManifest) == 0){
    log$warn('No manifest file. Cannot continue.')
    return()
  } else {
    if(TimeShftDir == "Neg"){
      dayExpc <- format(timeBgn + as.difftime(c(0,1), units = 'days'),format="%Y-%m-%d")
    } else if (TimeShftDir == "Pos"){
      dayExpc <-format(timeBgn + as.difftime(c(-1,0), units = 'days'),format="%Y-%m-%d")
    }
    dayHave <- unlist(lapply(fileData,FUN=function(fileIdx){
      mtch <- regexec(pattern='[0-9]{4}-[0-1]{1}[0-9]{1}-[0-3]{1}[0-9]{1}',fileIdx)[[1]]
      if(mtch != -1){
        dayHaveIdx <- substr(fileIdx,mtch,mtch+attr(mtch,"match.length")-1)
        return(dayHaveIdx)
      } else {
        return(NULL)
      }
    }))
    dayChk <- dayExpc %in% dayHave
    if(!all(dayChk)){
      log$warn(paste0('Timeseries pad incomplete. Missing the following days: ',
                      paste0(dayExpc[!dayChk],collapse=', ')))
      return()
      
    }
    
  }

  # Read, combine, filter, and sort the dataset 
  data <- NEONprocIS.base::def.read.parq.ds(fileIn=fs::path(dirInData,fileData),
                                            VarTime='readout_time',
                                            Df=TRUE, 
                                            log=log)

  #combine data, shift time 5 mins, pull out center day
  timeDiff <- as.difftime(TimeShft, units = TimeUnit)

  if (TimeShftDir == 'Neg'){
      dataShift <- data %>% 
        dplyr::arrange(readout_time) %>% 
        dplyr::mutate(readout_time = readout_time - timeDiff) %>% 
        dplyr::filter(base::as.Date(readout_time) == base::as.Date(timeBgn))
  } else if (TimeShftDir == 'Pos'){
      dataShift <- data %>% 
        dplyr::arrange(readout_time) %>% 
        dplyr::mutate(readout_time = readout_time + timeDiff) %>% 
        dplyr::filter(base::as.Date(readout_time) == base::as.Date(timeBgn))
  } else {
    log$error("TimeShftDir must be Pos or Neg")
    stop()
  }
  
  #get file name based on date of data in directory
  nameFileOut <- fileData[base::which(base::grepl(fileData, pattern = base::as.Date(timeBgn)))]
  
  # Write out the time shifted dataset to file
  fileOut <- fs::path(dirOutData,nameFileOut)
  
  rptWrte <-
    base::try(NEONprocIS.base::def.wrte.parq(
        data = dataShift,
        NameFile = fileOut,
        log=log
    ),
    silent = TRUE)
  if ('try-error' %in% base::class(rptWrte)) {
    log$error(base::paste0(
      'Cannot write output to ',
      fileOut,
      '. ',
      attr(rptWrte, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0(
      'Shifted time output written to file ',
      fileOut
      ))
  }

  return()
} 
