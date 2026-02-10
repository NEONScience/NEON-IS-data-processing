##############################################################################################
#' @title Gap filling module for non-regularized data in NEON IS data processing.

#' @author
#' Nora Catolico \email{ncatolico@battelleecology.org}

#' @description Wrapper function.
#' General code workflow:
#'    Parse input parameters
#'    Read in output schemas if indicated in parameters
#'    Determine datums to process (set of files/folders to process as a single unit)
#'    For each datum:
#'      Create output directories and copy (by symbolic link) unmodified components
#'      Loop through all data files and fill gaps
#'        Write out the gap filled data
#'
#'
#' @param DirIn Character value. The input path to the data from a single sensor or location, structured as follows: 
#' #/pfs/BASE_REPO/#/yyyy/mm/dd/#/id, where # indicates any number of parent and child directories 
#' of any name, so long as they are not 'pfs' or recognizable as the 'yyyy/mm/dd' structure which indicates 
#' the 4-digit year, 2-digit month, and' 2-digit day. The id is the unique identifier of the sensor or location. \cr
#'
#' Nested within this path are the folders:
#'         /data
#'         /flags
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param DirFill List of the terminal directories where the data to be
#' gap filled resides. This will be one or more child levels away from "DirIn". All files in the
#' terminal directory will be gap filled. The value may also be a vector of terminal directories,
#' separated by pipes (|). All terminal directories must be present and at the same directory level.
#' For example, "DirFill=data|flags" indicates to gap fill the data files within each the data
#' and flags directories.
#' 
#' @param FileSchm Character value (optional), where value is the full path to schema for data output by
#' this workflow. The value may be NA, in which case the output schema will be the same as the input
#' data. The value may be a single file, in which case it will apply to all output, or
#' multiple values in which case the argument is formatted as dir:value|dir:value...
#' where dir is one of the directories specified in DirFill and value is the path to the schema file
#' for the output of that directory. Multiple dir:value pairs are separated by pipes (|).
#' For example, "FileSchm=data:/path/to/schemaData.avsc|flags:NA" indicates that the
#' output from the data directory will be written with the schema /path/to/schemaData.avsc and the
#' output from the flags directory will be the same as the input files found in that
#' directory.
#' 
#' @param WndwFill Character value. The window in minutes in which data are expected. It is formatted as a 3 character sequence,
#'  representing the number of minutes over which any number of measurements are expected. 
#' For example, "WndwFill=015" refers to a 15-minute interval, while "WndwAgr=030" refers to a 
#' 30-minute  interval. 
#'  
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is).

#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.

#' @return Gap filled data output in Parquet format in DirOutBase, where DirOutBase directory
#' replaces BASE_REPO of DirIn but otherwise retains the child directory structure of the input path.

#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples
#' # Not run

#' @seealso None currently

# changelog and author contributions / copyrights
#   Nora Catolico (2025-12-4)
#     original creation
##############################################################################################
wrap.gap.fill.nonrglr <- function(DirIn,
                      DirOutBase,
                      DirFill,
                      WndwFill,
                      SchmFill,
                      DirSubCopy=NULL,
                      log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Gather info about the input directory (including date) and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn,log=log)
  dirOut <- base::paste0(DirOutBase, InfoDirIn$dirRepo)
  
  timeBgn <-InfoDirIn$time # Earliest possible start date for the data
  timeEnd <- InfoDirIn$time + base::as.difftime(1, units = 'days')
  # All minute window start times in [timeBgn, timeEnd)
  all_starts <- seq(timeBgn, timeEnd - WndwFill*60, by = WndwFill*60)
  
  # Helper to floor readout_times to window starts
  floor_15m <- function(x) {
    as.POSIXct(floor(as.numeric(x) / (WndwFill*60)) * (WndwFill*60),
               origin = "1970-01-01", tz = attr(x, "tzone"))
  }
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn, '/', DirSubCopy), 
                                       dirOut, 
                                       log = log)
  }
  
  
  # --------- loop through the directories ----------
  for (i in 1:length(DirFill)){
    
    subDir<-DirFill[i] 
    
    # Take stock of our files. 
    subDirIn <- fs::path(DirIn,subDir)
    files <- base::list.files(subDirIn,full.names=FALSE)
    
    #loop through files in directory
    for (j in 1:length(files)){
      fileName <- files[j]
      
      # Load in file in parquet format into data frame
      df  <-
        base::try(NEONprocIS.base::def.read.parq(NameFile = base::paste0(subDirIn, '/', fileName),
                                                 log = log),
                  silent = FALSE)
      if (base::any(base::class(df) == 'try-error')) {
        # Generate error and stop execution
        log$error(base::paste0('File ', subDirIn, '/', fileName, ' is unreadable.'))
        base::stop()
      }
      df$readout_time <- base::as.POSIXlt(df$readout_time)
      
      # Windows that already have at least one observation
      present <- unique(floor_15m(df$readout_time))
      
      # Missing windows
      missing <- all_starts[!all_starts %in% present]
      
      # Build blank rows for missing windows
      blanks <- data.frame(readout_time = missing)
      
      # Combine and sort
      df_filled <- bind_rows(df, blanks)
      df_filled <- df_filled[order(df_filled$readout_time), ]
      
      #add in source id if needed
      if("source_id" %in% colnames(df_filled)){
        source_id<-unique(df_filled$source_id[!is.na(df_filled$source_id)])
        if(length(source_id>0)){
          df_filled$source_id[is.na(df_filled$source_id)]<-source_id[1]
        }else{
          df_filled$source_id[is.na(df_filled$source_id)]<-"99999"
        }
      }
      
      # create output directories
      subDirOut <- paste0(dirOut,'/',subDir,'/')
      base::dir.create(subDirOut,recursive=TRUE)
      
      # select output schema
      if(!is.na(SchmFill)){
        FileSchmFill<-SchmFill$FileSchmFill[grepl(subDir,SchmFill$DirFill)]
        if(length(FileSchmFill)>1){
          #specific to suna for now. can be updated if needed down the road
          if(grepl("log",fileName,ignore.case = TRUE)){
            FileSchmFill<-FileSchmFill[grepl("log",FileSchmFill,ignore.case = TRUE)]
          }
          if(grepl("cal",fileName,ignore.case = TRUE)){
            FileSchmFill<-FileSchmFill[grepl("cal",FileSchmFill,ignore.case = TRUE)]
          }
        }
        if (base::is.na(FileSchmFill)|FileSchmFill=="NA"|length(FileSchmFill)>1) {
          # use the output data to generate a schema
          idxSchmFill <- base::attr(df_filled, 'schema')
        } else {
          idxSchmFill <- SchmFill$SchmFill[SchmFill$FileSchmFill==FileSchmFill]
        }
      }else{
        # use the output data to generate a schema
        idxSchmFill <- base::attr(df_filled, 'schema')
      }
      
      
      # write out data
      rptOut <- try(NEONprocIS.base::def.wrte.parq(data = df_filled,
                                                   NameFile = base::paste0(subDirOut,fileName),Schm = idxSchmFill),silent=TRUE)
      if(class(rptOut)[1] == 'try-error'){
        log$error(base::paste0('Cannot write file to ',base::paste0(subDirOut,fileName),'. ',attr(rptOut, "condition")))
        stop()
      } else {
        log$info(base::paste0('File written successfully in ', base::paste0(subDirOut,fileName)))
      }
  
    }
  }

}
