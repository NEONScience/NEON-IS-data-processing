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
    cat ("\n #################### validating data frame::: input data frame is empty \n\n")
    b = FALSE
  }
  
  else  if (any(is.na(dfIn))) {
    cat ("\n #################### validating data frame::: input data frame has invalid or missing values\n")
    
    b = FALSE
  }
  
  return (b)
}