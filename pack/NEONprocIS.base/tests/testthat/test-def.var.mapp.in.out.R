library(testthat)
source("R/def.var.mapp.in.out.R")


test_that("when length of nameVarIn is not same as nameVarOut, throw an error",
          {
            nameVarIn <- c("varIn1", "varIn2")
            nameVarOut <- c("nameOut1","nameOut2")
            report <- try(NEONprocIS.base::def.var.mapp.in.out(nameVarIn = nameVarIn, nameVarOut = nameVarOut), silent = TRUE)
            testthat::expect_true((class(report)[1] == "try-error"))

          })

# test_that("when NameCol is not 2, throw an error",
#           {
#             vect <- c('key1','1.5')
#             KeyExpc <- c('key1','key2')
#             ValuDflt <- 3
#             NameCol <- c('MyKey')
#             Type <- c('character','numeric')
#             rptt <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
#             testthat::expect_true((class(rptt)[1] == "try-error"))
# 
# 
#           })

