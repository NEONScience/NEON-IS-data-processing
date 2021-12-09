##############################################################################################
#' @title Merge the contents of multiple data files that share a common time variable

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org} \cr

#' @description Wrapper function. Merge the contents of multiple data files that share a common time
#' variable but different data columns. Note that if the same column name (other than the
#' time variable) is found in more than one input file, only the first instance will be retained
#' for output. Any missing timestamps among the files will be filled with NA values for the affected 
#' columns. Optionally select and/or rearrange columns for output.

#' @param DirIn Character value. The input path to the directory where the child directories indicated
#' in input argument \code{DirComb} reside. This path must be the direct parent of those directories. 
#' The input path is structured as follows: #/pfs/BASE_REPO/##, where # indicates any
#' number of parent and child directories of any name, so long as they are not 'pfs' or the same name
#' as any of the terminal directories indicated in argument \code{DirComb}.
#'
#' For example:
#' DirIn = "/scratch/pfs/proc_group/soilprt/27134/2019/01/01"
#'
#' @param DirOutBase Character value. The output path that will replace the #/pfs/BASE_REPO portion of DirIn. 
#'
#' @param DirComb Character vector. The name(s) of the directories (direct children of \code{DirIn})
#' where the data to be combined resides. All files in these directories will be combined into a single file.
#' For example, if DirComb=c("data","flags"), all files found in the "data" and "flags" directories will be
#' combined into a single file.
#'
#' @param NameVarTime Character string. The name of the time variable common across all
#' files. Note that any missing timestamps among the files will have their respective columns filled 
#' with NA values.
#'
#' @param ColKeep Character vector. The names, in desired order, of the input columns
#' that should be copied over to the combined output file. The column names indicated here must be a
#' full or partial set of the union of the column names found in the input files. Use the output
#' schema in argument \code{SchmCombList} to rename them as desired. Note that column names may be listed
#' more than once here. In that case the same data will be duplicated in the indicated columns, but
#' the column name of all instances after the first will default to appending an index to the end of the column name.
#' Use the output schema in argument \code{SchmCombList} to rename them as desired.
#' If this argument is omitted or NULL, all columns found in the input files for each directory will be included
#' in the output file in the order they are encountered in the input files.
#'
#' @param NameDirCombOut Character value. The name of the output directory that will be created to
#' hold the combined file. It may be the same as one of \code{DirComb}, but note that the original directory
#' with that same name may not be copied through to the output in argument DirSubCopy.
#' !!!CLARIFY THIS BEHAVIOR IF OVERLAP WITH DirSubCopy!!!
#'
#' @param NameFileSufx Character string. A character suffix to add to the output
#' file name before any extension. The base output file name is the shortest file name found among the input files.
#' For example, if the shortest file name found in the input files is "prt_CFGLOC12345_2019-01-01.parquet", and the 
#' input argument is "NameFileSufx=_stats_100", then the output file will be 
#' "prt_CFGLOC12345_2019-01-01_stats_100.parquet". Default is NULL, indicating no suffix to be added.
#'  
#' @param SchmCombList (optional) The list output from parsing the schema for the combined output, as generated
#' from NEONprocIS.base::def.schm.avro.pars. If not input or NULL, the schema will be constructed from the output 
#' data frame, as controlled by argument \code{ColKeep}.
#' 
#' @param DirSubCopy (optional) Character vector. The names of additional subfolders at 
#' the same level as the location folder in the input path that are to be copied with a symbolic link to the 
#' output path (i.e. not combined but carried through as-is). May not overlap with the output directory named in 
#' argument \code{NameDirCombOut}.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function. See NEONprocIS.base::def.log.init
#' for more details.
#'
#' @return A single file containined the merged data in DirOut, where DirOut replaces BASE_REPO of argument
#' \code{DirIn} but  otherwise retains the child directory structure of the input path. The file name will be the same
#' as the shortest file name found in the input files, with any suffix indicated in argument \code{NameFileSufx} 
#' inserted in the file name prior to the file extension (if present). The ordering of the columns will follow that in 
#' the description of argument \code{ColKeep}.
#'
#' @references
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples Currently none

#' @seealso Currently none.

# changelog and author contributions / copyrights
#   Cove Sturtevant (2021-07-27)
#     Convert flow script to wrapper function
##############################################################################################
wrap.data.comb.ts <- function(DirIn,
                              DirOutBase,
                              DirComb,
                              NameVarTime,
                              ColKeep=NULL,
                              NameDirCombOut,
                              NameFileSufx=NULL,
                              SchmCombList=NULL,
                              DirSubCopy=NULL,
                              log=NULL
){
  
  # Start logging if not already
  if(base::is.null(log)){
    log <- NEONprocIS.base::def.log.init()
  } 
  
  # Error check that there is no overlap between DirSubCopy and NameDirCombOut
  if(base::any(DirSubCopy %in% NameDirCombOut)){
    log$warn(base::paste0('The directory: ',
                          paste0(DirSubCopy[DirSubCopy %in% NameDirCombOut],collapse=','),
                          ' indicated in argument DirSubCopy is the same as that named in argument ',
                          'NameDirCombOut, which is not allowed. Its original contents will not be ',
                          'copied through to the output.')
    )
    DirSubCopy <-
      base::unique(base::setdiff(DirSubCopy, NameDirCombOut))
  }
  
  # Get directory listing of input director(ies). We will combine these files.
  file <- base::list.files(base::paste0(DirIn, '/', DirComb))
  filePath <- base::list.files(base::paste0(DirIn, '/', DirComb), 
                               full.names = TRUE)
  
  # Gather info about the input directory and create the output directory.
  InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(DirIn)
  idxDirOut <- base::paste0(DirOutBase, InfoDirIn$dirRepo)
  idxDirOutComb <- base::paste0(idxDirOut, '/', NameDirCombOut)
  NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                DirSub = NameDirCombOut,
                                log = log)
  
  # Copy with a symbolic link the desired subfolders
  if (base::length(DirSubCopy) > 0) {
    NEONprocIS.base::def.dir.copy.symb(base::paste0(DirIn, '/', DirSubCopy),
                                       idxDirOut, log = log)
  }
  
  # Combine the data files
  data <- NEONprocIS.base::def.file.comb.ts(file = filePath,
                                            nameVarTime = NameVarTime,
                                            log = log)
  
  # Take stock of the combined data
  nameCol <- base::names(data)
  log$debug(base::paste0(
    'Columns found in the combined data files: ',
    base::paste0(nameCol, collapse = ',')
  ))
  
  # Filter and re-order the output columns
  if (!base::is.null(ColKeep)) {
    # Check whether the desired columns to keep are found in the combined data
    chkCol <- ColKeep %in% nameCol
    if (base::any(!chkCol)) {
      log$error(
        base::paste0(
          'Columns: ',
          base::paste0(ColKeep[!chkCol], collapse = ','),
          ' were not found in the input data. Check ColKeep input argument and input data files.'
        )
      )
      stop()
    }
    
    # Reorder and filter the output columns
    data <- data[ColKeep]
    
    # Turn any periods in the column names to underscores
    base::names(data) <- base::sub(pattern='[.]',replacement='_',x=base::names(data))

    
    if(base::is.null(SchmCombList$schmJson)){
      log$debug(base::paste0(
        'Filtered and re-ordered output columns : ',
        base::paste0(base::names(data), collapse = ',')
      ))
    } else {
      log$debug(base::paste0(
        'Filtered and re-ordered input columns: ',
        base::paste0(ColKeep, collapse = ','),
        ' will be respectively output with column names: ',
        base::paste0(SchmCombList$var$name, collapse = ',')
      ))      
    }
    
  }
  
  # Write out the file. Take the shortest file name and tag on the suffix.
  fileBase <-
    file[base::nchar(file) == base::min(base::nchar(file))][1]
  fileOut <-
    NEONprocIS.base::def.file.name.out(nameFileIn = fileBase,
                                       sufx = NameFileSufx,
                                       log = log)
  nameFileOut <- base::paste0(idxDirOutComb, '/', fileOut)
  
  rptWrte <-
    base::try(NEONprocIS.base::def.wrte.parq(
      data = data,
      NameFile = nameFileOut,
      NameFileSchm = NULL,
      Schm = SchmCombList$schmJson,
      log=log
    ),
    silent = TRUE)
  if (base::class(rptWrte) == 'try-error') {
    log$error(base::paste0(
      'Cannot write combined file ',
      nameFileOut,
      '. ',
      attr(rptWrte, "condition")
    ))
    stop()
  } else {
    log$info(base::paste0('Combined data written successfully to file: ',
                          nameFileOut))
  }
  
  return()
} # End loop around datum paths
