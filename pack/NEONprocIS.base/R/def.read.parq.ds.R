##############################################################################################
#' @title Read in a parquet dataset 

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Read in multiple parquet files with the same schema into an arrow dataset. 
#' Optionally limit to selected variables and order by time. 

#' @param fileIn Character vector. Names (including relative or absolute path) of parquet files. 
#' These files must all have the same parquet schema. This can also be a directory, in which case
#' all files in the directory will be read into the dataset.
#' @param Var Optional. Character vector. Names of the fields/variables in the parquet dataset
#' to retain, ordered the same as Var. Only those present in the dataset will be output.
#' @param VarTime Optional. Character value of the time variable to order by. If NULL, no ordering
#' is performed.
#' @param RmvDupl Optional. Boolean. TRUE to remove duplicated rows in the data (as defined by the Var columns). Defaults to FALSE.
#' @param Df Optional. Boolean. TRUE to return a data frame with the combined dataset. Defaults to FALSE.
#' 
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.
#' 
#' @return If Df is TRUE, a dataframe of the combined output. Any time variables are converted to POSIXct
#' and converted to GMT. If Df is FALSE, an object of class arrow_dplyr_query, which can be further 
#' manipulated using dplyr. Note that no time zone conversion is performed for the arrow_dplyr_query
#' object.

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' # NOT RUN
#' fileIn <- c('path/to/file1.parquet','path/to/file2.parquet')
#' data <- NEONprocIS.base::def.read.parq.ds(fileIn,VarTime='readout_time',Df=TRUE)

#' @seealso Currently none

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2023-03-07)
#     original creation
#   Cove Sturtevant (2023-11-13)
#     add option to remove duplicated rows
#   Cove Sturtevant (2026-02-19)
#     Handling for non-hashable data columns when deduplicating
##############################################################################################
def.read.parq.ds <- function(fileIn,
                             Var=NULL,
                             VarTime=NULL,
                             RmvDupl=FALSE,
                             Df=FALSE,
                             log=NULL
){
  
  library(dplyr)
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }

  # Build a normalized schema for mixed string/large_string fields across files.
  # This avoids Arrow merge failures when dataset fragments disagree on string width.
  build_schema_for_string_conflicts <- function(fileIn, log) {
    files <- fileIn
    if (base::length(fileIn) == 1 && base::dir.exists(fileIn)) {
      files <- base::list.files(
        path = fileIn,
        pattern = "\\.parquet$",
        recursive = TRUE,
        full.names = TRUE
      )
    }

    if (base::length(files) == 0) {
      return(NULL)
    }

    # Read schemas from parquet footers
    sch_list <- base::lapply(files, FUN = function(f) {
      try(arrow::read_parquet(f, as_data_frame = FALSE)$schema, silent = TRUE)
    })
    sch_list <- sch_list[!base::vapply(sch_list, inherits, logical(1), "try-error")]

    if (base::length(sch_list) == 0) {
      return(NULL)
    }

    all_names <- unique(unlist(base::lapply(sch_list, base::names), use.names = FALSE))
    if (base::length(all_names) == 0) {
      return(NULL)
    }

    fields_out <- base::vector(mode = "list", length = base::length(all_names))

    for (i in base::seq_along(all_names)) {
      nm <- all_names[[i]]

      fld_list <- base::lapply(sch_list, FUN = function(sch) {
        if (nm %in% base::names(sch)) sch$GetFieldByName(nm) else NULL
      })
      fld_list <- fld_list[!base::vapply(fld_list, base::is.null, logical(1))]
      if (base::length(fld_list) == 0) {
        next
      }

      type_str <- unique(base::vapply(fld_list, FUN = function(f) f$type$ToString(), character(1)))

      # Promote mixed string widths to large_utf8
      if (base::all(type_str %in% c("string", "large_string")) && base::length(type_str) > 1) {
        type_use <- arrow::large_utf8()
        log$debug(base::paste0("Promoting field ", nm, " to large_utf8 due to mixed types: ", base::paste(type_str, collapse = ", ")))
      } else {
        type_use <- fld_list[[1]]$type
      }

      nullable_use <- base::any(base::vapply(fld_list, FUN = function(f) isTRUE(f$nullable), logical(1)))
      fields_out[[i]] <- arrow::field(nm, type_use, nullable = nullable_use)
    }

    fields_out <- fields_out[!base::vapply(fields_out, base::is.null, logical(1))]
    if (base::length(fields_out) == 0) {
      return(NULL)
    }

    base::do.call(arrow::schema, fields_out)
  }
  
  # Load data files into an arrow dataset
  schmIn <- build_schema_for_string_conflicts(fileIn = fileIn, log = log)
  if (base::is.null(schmIn)) {
    data <- arrow::open_dataset(fileIn, unify_schemas = TRUE)
  } else {
    data <- arrow::open_dataset(fileIn, schema = schmIn, unify_schemas = TRUE)
  }
  schm <- data$schema
  varData <- base::names(schm)
  
  # Pull the columns matching Var, sort by readout_time
  if(base::length(Var) == 0){
    Var <- varData
  } 
  varMtch <- Var[Var %in% varData]
  dataMtch <- data %>% 
    dplyr::select(dplyr::all_of(varMtch))
 
  # Order by time, if selected
  if(!base::is.null(VarTime)){
    dataMtch <- dataMtch %>%
      dplyr::arrange(!! rlang::sym(VarTime))
  }
  
  # Get rid of duplicates if selected
  if(RmvDupl == TRUE){
    # NOTE: current limitation of the Arrow dplyr backend: distinct() evaluates 
    #   using hash-based keys, and Arrow doesn’t support using a list<…> column 
    #   as a key. Only solution for Arrow 19 is to collect() first. 
    # So we need to determine if there are any non-hashable columns.
    
    # Helper to get list-typed column names from an Arrow Dataset/Table schema
    # Input is the arrow schema
    non_key_columns <- function(sch, exclude_struct_map = TRUE) {
      fields <- sch$fields
      is_bad_key <- vapply(
        fields,
        function(f) {
          is_list <- inherits(f$type, "ListType") ||
            inherits(f$type, "LargeListType") ||
            inherits(f$type, "FixedSizeListType")
          if (exclude_struct_map) {
            is_struct_map <- inherits(f$type, "StructType") ||
              inherits(f$type, "MapType")
            is_list || is_struct_map
          } else {
            is_list
          }
        },
        logical(1)
      )
      sch$names[is_bad_key]
    }
    
    
    # Derive key columns (exclude all list-like columns)
    bad_cols <- non_key_columns(schm, exclude_struct_map = TRUE)

    if (length(bad_cols) > 0) {
      log$warn(base::paste0(
        "Arrow-dplyr integration does not support lazy deduplication when there ",
        "are non-hashable columns (including lists) in the dataset. Need to collect() ",
        " first. A data frame will be returned."
      ))
      
      dataMtch <- dataMtch %>%
        collect()
      Df <- TRUE # Return data frame (i.e. attach schema and attach time zones)
    } 

    dataMtch <- dataMtch %>%
      distinct(.keep_all = TRUE)
      
  }
  
  # Return a data frame, if selected. Otherwise return arrow table.
  if(Df == TRUE){
    dataMtch <- dataMtch %>% 
      dplyr::collect() 
    
    dataMtch <- base::data.frame(dataMtch)
    base::attr(dataMtch,'schema') <- schm
    
    # Assign timezone for POSIX variables
    clssVar <- base::lapply(X=dataMtch,FUN=base::class)
    setTime <- base::unlist(base::lapply(X=clssVar,FUN=function(idxClss){base::any(base::grepl(pattern='POSIX',x=idxClss))}))
    for (idxVar in base::names(dataMtch)[setTime]){
      base::attr(dataMtch[[idxVar]],'tzone') <- 'GMT'
    }
    
    return(dataMtch)
    
  } else {
    
    return(dataMtch)
    
  }
  
}
