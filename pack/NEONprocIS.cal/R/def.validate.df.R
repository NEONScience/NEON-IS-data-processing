#########################################################################################################
#' @title Validate data frame

#' @author
#' Mija Choi \email{choim@battelleecology.org}

#' @description
#' Validate a data frame to check it is empty or has invalid values.
#' Returns True if valid. FALSE otherwise.
########################################################################################################

def.validate.df <- function(dfIn) {
  b = TRUE
  
  if (nrow(dfIn) == 0) {
   
    b = FALSE
  }
  
  else  if (any(is.na(dfIn))) {
    
    b = FALSE
  }
  
  return (b)
}