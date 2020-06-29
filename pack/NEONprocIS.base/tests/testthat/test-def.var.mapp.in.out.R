library(testthat)
source("R/def.var.mapp.in.out.R")


# test_that("when length of nameVarIn is not same as nameVarOut, throw an error",
#           {
#             nameVarIn <- c("varIn1", "varIn2")
#             nameVarOut <- c("nameOut1","nameOut2", "nameOut3")
#             nameVarDfltSame=c('nameSame1','nameSame2')
#             report <- try(NEONprocIS.base::def.var.mapp.in.out(nameVarIn = nameVarIn, nameVarOut = nameVarOut), silent = TRUE)
#             testthat::expect_true((class(report)[1] == "try-error"))
# 
#           })

# test_that("when nameVarDfltSame has different value is sent in the parameters",
#           {
#             nameVarIn <- c("varIn1", "varIn2")
#             nameVarOut <- c("nameOut1","nameOut2")
#             nameVarDfltSame <- c('nameSame1','nameSame2')
#             output <- try(NEONprocIS.base::def.var.mapp.in.out(nameVarIn = nameVarIn, nameVarOut = nameVarOut, nameVarDfltSame = nameVarDfltSame), silent = TRUE)
#             testthat::expect_false((class(report)[1] == "try-error"))
#             testthat::expect_equal(2, length(output))
#             testthat::expect_equal(output$nameVarIn [1], "varIn1")
#             testthat::expect_equal(output$nameVarOut[2], "nameOut2")
#             testthat::expect_equal(output$nameVarOut[3], "nameSame1")
#             testthat::expect_equal(output$nameVarIn[4], "nameSame2")
#           })

# test_that("when nameVarDfltSame has different number of values compared to nameVarIn and nameVarOut is sent in the parameters",
#           {
#             nameVarIn <- c("varIn1", "varIn2")
#             nameVarOut <- c("nameOut1","nameOut2")
#             nameVarDfltSame <- c('nameSame1','nameSame2', 'nameSame3')
#             output <- try(NEONprocIS.base::def.var.mapp.in.out(nameVarIn = nameVarIn, nameVarOut = nameVarOut, nameVarDfltSame = nameVarDfltSame), silent = TRUE)
#             testthat::expect_false((class(report)[1] == "try-error"))
#             testthat::expect_equal(2, length(output))
#             testthat::expect_equal(output$nameVarIn [1], "varIn1")
#             testthat::expect_equal(output$nameVarOut[2], "nameOut2")
#             testthat::expect_equal(output$nameVarOut[3], "nameSame1")
#             testthat::expect_equal(output$nameVarIn[4], "nameSame2")
#             testthat::expect_equal(output$nameVarIn[5], "nameSame3")
#             testthat::expect_equal(output$nameVarIn[5], "nameSame3")
#           })


#get it verified by Cove
# test_that("when nameVarDfltSame has one different value as nameVarIn and nameVarOut is sent in the parameters",
#           {
#             nameVarIn <- c("varIn1", "varIn2")
#             nameVarOut <- c("nameOut1","nameOut2")
#             nameVarDfltSame <- c('nameOut2','nameSame2', 'nameSame3')
#             output <- try(NEONprocIS.base::def.var.mapp.in.out(nameVarIn = nameVarIn, nameVarOut = nameVarOut, nameVarDfltSame = nameVarDfltSame), silent = TRUE)
#             testthat::expect_false((class(report)[1] == "try-error"))
#             testthat::expect_equal(2, length(output))
#             testthat::expect_equal(output$nameVarIn [1], "varIn1")
#             testthat::expect_equal(output$nameVarOut[2], "nameOut2")
#             testthat::expect_equal(output$nameVarOut[3], "nameSame1")
#             testthat::expect_equal(output$nameVarIn[4], "nameSame2")
#             testthat::expect_equal(output$nameVarIn[5], "nameSame3")
#             testthat::expect_equal(output$nameVarIn[5], "nameSame3")
#           })


