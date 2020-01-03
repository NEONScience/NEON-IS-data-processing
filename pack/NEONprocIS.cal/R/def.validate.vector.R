

def.validate.vector <- function(vectIn) {
  a = TRUE
  
  if (length(vectIn) == 0) {
    cat ("\n ####### validating vector::: input vector is empty \n\n")
        a = FALSE
  }
  
  else  if (is.na(vectIn) | is.nan(vectIn)) {
    cat ("\n ####### validating vector:::  input vector has invalid or missing values \n\n")
        a = FALSE
  }
  return (a)
}