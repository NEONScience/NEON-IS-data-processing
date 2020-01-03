

def.validate.df <- function(dfIn) {
  b = TRUE
  
  if (nrow(dfIn) == 0) {
    cat ("\n ####### validating data frame::: input data frame is empty \n\n")
    print(dfIn)
    b = FALSE
  }
  
  else  if (is.na(dfIn)) {
    cat ("\n ####### validating data frame::: input data frame has invalid or missing values \n\n")
    print(dfIn)
    b = FALSE
  }
  return (b)
}