data_combine <- function(DirIn,
                         DirOut,
                         DirComb,
                         NameDirCombOut,
                         NameVarTime,
                         FileSchmComb = NULL,
                         ColKeep = NULL,
                         DirSubCopy = NULL,
                         NameFileSufx = NULL) {
  
  #uncomment below line for debugging on SOM server. 
  #browser()
  log <- NEONprocIS.base::def.log.init()
  # Echo arguments
  log$debug(base::paste0('Input directory: ', DirIn))
  log$debug(base::paste0('Output directory: ', DirOut))
  log$debug(
    base::paste0(
      'All files found in the following directories will be combined: ',
      base::paste0(DirComb, collapse = ',')
    )
  )
  log$debug(
    base::paste0(
      'A single combined data file will be populated in the directory: ',
      NameDirCombOut
    )
  )
  log$debug(base::paste0('Common time variable expected in all files: ', NameVarTime))
  
  # Read in the output schema
  log$debug(base::paste0(
    'Output schema: ',
    base::paste0(FileSchmComb, collapse = ',')
  ))
  if (base::is.null(FileSchmComb) || FileSchmComb == 'NA') {
    SchmComb <- NULL
  } else {
    SchmComb <-
      base::paste0(base::readLines(FileSchmComb), collapse = '')
    
    # Parse the avro schema for output variable names
    nameVarSchmComb <- NEONprocIS.base::def.schm.avro.pars(Schm=SchmComb,log=log)$var$name
  }
  
  # Echo more arguments
  log$debug(
    base::paste0(
      'Input columns (and their order) to populate in the combined output file (all if empty): ',
      base::paste0(ColKeep, collapse = ',')
    )
  )
  
  
  # Retrieve optional subdirectories to copy over
  DirSubCopy <-
    base::unique(base::setdiff(DirSubCopy, NameDirCombOut))
  log$debug(base::paste0(
    'Additional subdirectories to copy: ',
    base::paste0(DirSubCopy, collapse = ',')
  ))
  
  # What are the expected subdirectories of each input path
  log$debug(base::paste0(
    'Minimum expected subdirectories of each datum path: ',
    base::paste0(DirComb, collapse = ',')
  ))
  
  # Find all the input paths (datums). We will process each one.
  DirIn <-
    NEONprocIS.base::def.dir.in(DirBgn = DirIn,
                                nameDirSub =  DirComb,
                                log = log)
  
  
  # Process each datum path
  for (idxDirIn in DirIn) {
    log$info(base::paste0('Processing path to datum: ', idxDirIn))
    
    # Get directory listing of input director(ies). We will combine these files.
    file <-
      base::list.files(base::paste0(idxDirIn, '/', DirComb))
    filePath <-
      base::list.files(base::paste0(idxDirIn, '/', DirComb), full.names =
                         TRUE)
    
    # Gather info about the input directory (including date) and create the output directory.
    InfoDirIn <- NEONprocIS.base::def.dir.splt.pach.time(idxDirIn)
    idxDirOut <- base::paste0(DirOut, InfoDirIn$dirRepo)
    idxDirOutComb <- base::paste0(idxDirOut, '/', NameDirCombOut)
    NEONprocIS.base::def.dir.crea(DirBgn = idxDirOut,
                                  DirSub = NameDirCombOut,
                                  log = log)
    
    # Copy with a symbolic link the desired subfolders
    if (base::length(DirSubCopy) > 0) {
      NEONprocIS.base::def.dir.copy.symb(base::paste0(idxDirIn, '/', DirSubCopy),
                                         idxDirOut, log = log)
    }
    
    # Combine data files
    data <- NULL
    data <-
      NEONprocIS.base::def.file.avro.comb.ts(file = filePath,
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
            base::paste0(nameCol[!chkCol], collapse = ','),
            'were not found in the input data. Check ColKeep input argument.'
          )
        )
        stop()
      }
      
      # Reorder and filter the output columns
      data <- data[ColKeep]
      
      # Turn any periods in the column names to underscores
      base::names(data) <- base::sub(pattern='[.]',replacement='_',x=base::names(data))
      
      
      if(base::is.null(SchmComb)){
        log$debug(base::paste0(
          'Filtered and re-ordered output columns : ',
          base::paste0(base::names(data), collapse = ',')
        ))
      } else {
        log$debug(base::paste0(
          'Filtered and re-ordered input columns: ',
          base::paste0(ColKeep, collapse = ','),
          ' will be respectively output with column names: ',
          base::paste0(nameVarSchmComb, collapse = ',')
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
      base::try(NEONprocIS.base::def.wrte.avro.deve(
        data = data,
        NameFile = nameFileOut,
        NameFileSchm = NULL,
        Schm = SchmComb,
        NameLib = '/ravro.so'
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
      log$info(base::paste0('Combined data written successfully in file: ',
                            nameFileOut))
    }
    
  } # End loop around datum paths
  
  
  }