#library(testthat)
#source("R/def.df.renm.R")

test_that("Renaming the dataframe",
          {
            original_df <- data.frame(X=c(1,2,3),Y=c(4,5,6))
            mappNameVar <- data.frame(nameVarIn=c('X','Y'), nameVarOut=c('NewA','NewB'), stringsAsFactors=FALSE)
            returned_df <- def.df.renm(df = original_df, mappNameVar = mappNameVar)
            testthat::expect_true(is.list(returned_df))
            if (!(length(returned_df) == 0)) {
              testthat::equals (returned_df$NewA[1], 1)
              testthat::equals (returned_df$NewB[3], 6)
              
            }
            
          }

)

