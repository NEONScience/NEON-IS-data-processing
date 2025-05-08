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
#   Cove Sturtevant (2025-05-07)
#     add workaround converting to duckdb when RmvDupl= TRUE to support some array data types
##############################################################################################
def.read.parq.ds <- function(fileIn,
                             Var=NULL,
                             VarTime=NULL,
                             RmvDupl=FALSE,
                             Df=FALSE,
                             log=NULL
){
  
  library(dplyr,quietly=TRUE)
  require(duckdb,quietly=TRUE)
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Load data files into an arrow dataset
  data <- arrow::open_dataset(fileIn,unify_schemas = TRUE)
  varData <- base::names(data$schema)
  
  # Pull the columns matching Var, sort by readout_time
  if(base::length(Var) == 0){
    Var <- varData
  } 
  varMtch <- Var[Var %in% varData]
  dataMtch <- data %>% 
    dplyr::select(dplyr::all_of(varMtch))
 
  # Get rid of duplicates if selected
  # Note: There is a bug for some array data types (e.g. enviroscan) in which the use of distinct()
  # alters the data type of the array unless .keep_all=TRUE is used, but .keep_all is not supported by arrow. 
  # Found a workaround to convert to duckdb first, per the following StackOverflow:
  # https://stackoverflow.com/questions/78240769/alternatives-for-distinct-keep-all-true-in-arrow
  # If and when arrow supports .keep_all=TRUE, the extra conversions to and from duckdb and subsequent
  # return to an arrow_dplyr_query can be removed.
  if(RmvDupl == TRUE){
    dataMtch <- dataMtch %>%
      # dplyr::distinct()
      arrow::to_duckdb() %>% # swap to duckdb to use distinct with .keep_all = TRUE, required to work properly for array data types
      dplyr::distinct(.keep_all = TRUE) %>%
      arrow::to_arrow() %>% # back to arrow
      dplyr::select(dplyr::all_of(varMtch)) # Back to arrow_dplyr_query
  }
  
  # Order by time, if selected
  if(!base::is.null(VarTime)){
    dataMtch <- dataMtch %>%
      dplyr::arrange(!! rlang::sym(VarTime))
  }
  
  # Return a data frame, if selected. Otherwise return arrow table.
  if(Df == TRUE){
    dataMtch <- dataMtch %>% 
      dplyr::collect() 
    
    dataMtch <- base::data.frame(dataMtch)
    
    # Adjust the schema for selected variables and attach to data frame
    schm <- data$schema
    if(!base::all(varData %in% varMtch)){
      nameFld <- base::unlist(base::lapply(schm$fields,FUN=function(fld){fld$name}))
      fldNew <- schm$fields[nameFld %in% varMtch]
      schm <- base::do.call(arrow::schema, fldNew)
      schm$metadata <- data$schema$metadata # re-attach metadata
    }
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
