###################################################################################################
#' @title validate if the xml content is valid against the xml schema

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate an input xml against the schema to check it is valid.
#' Returns TRUE if the xml is vaild . FALSE otherwise.

#' @param xmlIn Input xml to be validated

#' @param xmlSchemaIn Input xml schema to be validated against

#' @return Boolean. TRUE if the input xml is valid against the schema. FALSE if not valid. \cr

#' @references Currently none

#' @keywords Currently none

#' @examples NEONprocIS.base::def.validate.json.schema (jsonIn, jsonSchemaIn)

#' @seealso \link[NEONprocIS.cal]{def.read.cal.xml}
#'
#' @export

# changelog and author contributions / copyrights
#   Mija Choi (2020-02-28)
#     original creation
##############################################################################################

def.validate.xml.schema <-
  function(xmlIn, xmlSchemaIn, log = NULL) {
    #  Initialize log if not input
    if (base::is.null(log)) {
      log <- NEONprocIS.base::def.log.init()
    }
    #set the default to FALSE
    d = FALSE
    
    xml <- try(XML::xmlParse(xmlIn), silent = TRUE)
    
    if (class(xml)[1] == "try-error") {
      log$warn(base::paste0(xmlIn, ' does not exist or is unreadable  '))
    }
    else {
      xmlFile <- xml2::read_xml(xmlIn)
      xmlFileXsd <- xml2::read_xml(xmlSchemaIn)
      
      #
      # TRUE if xmlIn is a valid xml against the schema. FALSE if invalid or error
      #
      log$info(
        base::paste0(
          'Validate.xml.schema:  Checking to see if the xml conforms to the schema.'
        )
      )
      if (xml2::xml_validate(xmlFile, xmlFileXsd)) {
        d = TRUE
        log$info(base::paste0(xmlIn, ' conforms to the schema  '))
      }
      else
      {
        log$warn(base::paste0(xmlIn, ' does not conform to the schema  '))
      }
    }
    return (d)
  }
