##############################################################################################
#' @title Get R data type for a given Arrow data type

#' @author 
#' Cove Sturtevant \email{csturtevant@battelleecology.org}

#' @description 
#' Definition function. Given an arrow data type object, return the equivalent R data type object.

#' @param typeArrow Arrow data type object
#' @param log Optional. A logger object as produced by NEONprocIS.base::def.log.init to produce structured log 
#' output in addition to standard R error messaging. Defaults to NULL, in which the logger will be 
#' created for use within this function. 
#' 
#' @return An R data type object

#' @references 
#' License: (example) GNU AFFERO GENERAL PUBLIC LICENSE Version 3, 19 November 2007

#' @keywords Currently none

#' @examples 
#' 
#' # Integer type → integer()
#' def.r.type.from.arrow.type(arrow::int32())
#' #> integer()
#'
#' # Floating point type → numeric()
#' def.r.type.from.arrow.type(arrow::float64())
#' #> numeric()


#' @seealso \link[arrow]{data-type}

#' @export

# changelog and author contributions / copyrights
#   Cove Sturtevant (2026-03-16)
#     original creation
##############################################################################################
def.r.type.from.arrow.type <- function(typeArrow,log=NULL) {
  
  # initialize logging if necessary
  if (base::is.null(log)) {
    log <- NEONprocIS.base::def.log.init()
  }
  
  if (inherits(typeArrow, "Int8")  ||
      inherits(typeArrow, "Int16") ||
      inherits(typeArrow, "Int32") ||
      inherits(typeArrow, "Int64") ||
      inherits(typeArrow, "UInt8") ||
      inherits(typeArrow, "UInt16") ||
      inherits(typeArrow, "UInt32") ||
      inherits(typeArrow, "UInt64")) {
    return(integer())   # R has only one integer type
  }
  
  if (inherits(typeArrow, "Float") || 
      inherits(typeArrow, "Float64") ||
      inherits(typeArrow, "Float32") ||
      inherits(typeArrow, "Float16") ||
      inherits(typeArrow, "Double")) {
    return(numeric())
  }
  
  if (inherits(typeArrow, "String") || 
      inherits(typeArrow, "LargeString") ||
      inherits(typeArrow, "Utf8") || 
      inherits(typeArrow, "LargeUtf8")) {
    return(character())
  }
  
  if (inherits(typeArrow, "Boolean")) {
    return(logical())
  }
  
  if (inherits(typeArrow, "Binary") || 
      inherits(typeArrow, "LargeBinary")) {
    return(list(raw()))   # R has only raw vectors, not binary scalars
  }
  
  if (inherits(typeArrow, "Date32") || 
      inherits(typeArrow, "Date64")) {
    return(as.Date(character()))
  }
  
  if (inherits(typeArrow, "Timestamp")) {
    tz <- typeArrow$timezone()
    if (tz == ""){
      return(as.POSIXct(character(), tz = "GMT"))
    } else {
      return(as.POSIXct(character(), tz = tz))
    }
  }
  
  if (inherits(typeArrow, "ListType")) {
    # recursive: list type → R list of objects matching element type
    return(list(NEONprocIS.base::def.r.type.from.arrow.type(typeArrow$value_type)))
  }
  
  if (inherits(typeArrow, "StructType")) {
    # create a named empty list with each field’s corresponding R type
    fields <- typeArrow$fields()
    out <- lapply(fields, function(f) NEONprocIS.base::def.r.type.from.arrow.type(f$type))
    names(out) <- unlist(lapply(fields, function(f){f$name}))
    return(out)
  }
  
  log$warn(base::paste0("Unsupported Arrow type: ",typeArrow$ToString()))
  return(NULL)
}