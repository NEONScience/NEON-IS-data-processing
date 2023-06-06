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
##############################################################################################
def.read.parq.ds <- function(fileIn,
                             Var=NULL,
                             VarTime=NULL,
                             Df=FALSE,
                             log=NULL
){
  
  library(dplyr)
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  # Load data files into an arrow dataset
  data <- arrow::open_dataset(fileIn,unify_schemas = TRUE)
  varData <- base::names(data$schema)
  
  # Pull the columns matching the L0 schema, sort by readout_time
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
  
  # Return a data frame, if selected. Otherwise return arrow table.
  if(Df == TRUE){
    dataMtch <- dataMtch %>% 
      dplyr::collect() 
    
    dataMtch <- base::data.frame(dataMtch)
    base::attr(dataMtch,'schema') <- data$schema
    
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
