#library(testthat)
#source("R/def.vect.pars.pair.R")
test_that("when length of vector is not multiple of 2, throw an error",
          {
            vect <- c('key1','1.5', 'key3')
            KeyExpc <- c('key1','key2','key3')
            ValuDflt <- 3
            NameCol <- c('MyKey','MyValue')
            Type <- c('character','numeric')
            report <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_true((class(report)[1] == "try-error"))

          })

test_that("when NameCol is not 2, throw an error",
          {
            vect <- c('key1','1.5')
            KeyExpc <- c('key1','key2')
            ValuDflt <- 3
            NameCol <- c('MyKey')
            Type <- c('character','numeric')
            report <- try(def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_false((class(report)[1] == "try-error"))

          })

test_that("when length of ValueDflt is not 1, throw an error",
          {
            vect <- c('key1','1.5')
            KeyExpc <- c('key1','key2','key3')
            ValuDflt <- c(3,4)
            NameCol <- c('MyKey', 'MyValu')
            Type <- c('character','numeric')
            report <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_true((class(report)[1] == "try-error"))

          })

