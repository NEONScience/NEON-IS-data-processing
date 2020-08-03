#library(testthat)
#source("R/def.dir.copy.symb.R")

test_that("when source directory lenght is greater than 1 and source dir length don't match with dest length",
          {
            inputDir <- c('def.dir.copy.symb/test1', 'def.dir.copy.symb/test2')

            outputDir <- c('def.dir.copy.symb/output/test1', 'def.dir.copy.symb/output/test2', 'def.dir.copy.symb/output/test3')

            report <- try(NEONprocIS.base::def.dir.copy.symb(DirSrc = inputDir, DirDest = outputDir), silent = TRUE)
            testthat::expect_true((class(report)[1] == "try-error"))


          })

test_that("when all source directories don't exist",
          {
           
            inputDir <- c('def.dir.copy.symb/test1', 'def.dir.copy.symb/test3h')
            outputDir <- 'def.dir.copy.symb/output'
            report <- try(NEONprocIS.base::def.dir.copy.symb(inputDir, outputDir), silent = TRUE)
            testthat::expect_false((class(report)[1] == "try-error"))
  
          })



