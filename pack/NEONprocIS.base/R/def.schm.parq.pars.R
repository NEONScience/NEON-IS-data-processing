##############################################################################################
#' @title Parse a parquet schema to retrieve data types of table columns

#' @author
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description
#' Definition function. Get the data types of each data column from a parquet schema.

#' @param schm Schema object of parquet file retrieved from e.g. arrow::read_parquet(...)$schema
#' @param log A logger object as produced by NEONprocIS.base::def.log.init to produce structured log
#' output. Defaults to NULL, in which the logger will be created and used within the function.

#' @return A data frame of: \cr
#' \code{name} Character. Name of data column
#' \code{type} Characer. Data type
#' \code{nullable} Boolean. Whether the field values are allowed to be null or NA

#' @references Currently none

#' @keywords Currently none

#' @examples 
#' # Not run
#' # schm <- arrow::read_parquet(file='/path/to/file',as_data_frame=FALSE)$schema
#' NEONprocIS.base::def.schm.parq.pars(schm)

#' @seealso \link[NEONprocIS.base]{def.read.parq}
#' @seealso \link[NEONprocIS.base]{def.wrte.parq}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2020-04-02)
#     original creation
#   Cove Sturtevant (2024-03-20)
#     add whether the field is nullable
##############################################################################################
def.schm.parq.pars <- function(schm, log = NULL) {
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  rpt <-
    base::lapply(
      schm$fields,
      FUN = function(idxFld) {
        base::data.frame(name = idxFld$name, 
                         type = idxFld$type$ToString(),
                         nullable=idxFld$nullable,
                         stringsAsFactors = FALSE)
      }
    ) 
  
  rpt <- base::do.call(base::rbind,rpt)
  
}
