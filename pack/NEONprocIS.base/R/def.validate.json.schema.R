###################################################################################################
#' @title validate if the JSON content is valid against the json schema

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate an input json against the schema to check it is valid.
#' Run validate.json before validate.json.schema
#' Returns TRUE if the json is vaild . FALSE if invalid or error.

#' @param jsonIn Input json to be validated

#' @param jsonSchemaIn Input json schema to be validated against

#' @return Boolean. TRUE if the input json is valid against the schema. FALSE if not valid. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples NEONprocIS.base::def.validate.json.schema (jsonIn, jsonSchemaIn)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-01-27)
#     original creation
##############################################################################################
#library(jsonvalidate)

def.validate.json.schema <-
  function(jsonIn, jsonSchemaIn, log = NULL) {
    #  Initialize log if not input
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    #set the default to FALSE
    d = FALSE
    
    tryCatch(
      (jsonvalidate::json_validate(jsonIn, jsonSchemaIn)),
      error = function(cond) {
        log$error(base::paste0(jsonin, ' error in the JSON schema validation '))
        stop
      }
    )
    #
    # TRUE if jsonIn is a valid JSON against the schema. FALSE if invalid or error
    #
    log$debug(
      base::paste0(
        'Validate.json.schema:  Checking to see if the JSON conforms to the schema.'
      )
    )
    if (jsonvalidate::json_validate(jsonIn, jsonSchemaIn)) {
      d = TRUE
      log$info(base::paste0(jsonIn, ' conforms to the schema  '))
    }
    else
    {
      log$warn(base::paste0(jsonIn, ' does not conform to the schema  '))
    }
    return (d)
  }
