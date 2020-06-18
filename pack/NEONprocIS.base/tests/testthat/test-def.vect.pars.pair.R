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
            rptt <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_true((class(rptt)[1] == "try-error"))


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


test_that("when type is not 2, throw an error",
          {
            vect <- c('key1','1.5')
            KeyExpc <- c('key1','key2')
            ValuDflt <- 3
            NameCol <- c('MyKey', 'MyKey2')
            Type <- c('character')
            rptt <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_true((class(rptt)[1] == "try-error"))


          })

test_that("when vect lenght is 1 and keyExpc is 0, throw an error",
          {
            vect <- c('key1')
            ValuDflt <- 3
            NameCol <- c('MyKey', 'MyKey2')
            Type <- c('character', 'numeric')
            rptt <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_true((class(rptt)[1] == "try-error"))


          })

test_that("when vect lenght is 1 and keyExpc is 1, return value",
          {
            vect <- c('value1')
            ValuDflt <- 3
            KeyExpc <- c('key1')
            NameCol <- c('MyKey', 'MyKey2')
            Type <- c('character', 'character')
            rptt <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect, KeyExp=KeyExpc, ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_equal(2, length(rptt))
            testthat::expect_equal(rptt$MyKey[1], "key1")
            testthat::expect_equal(rptt$MyKey2[1], "value1")


          })

test_that("when vect lenght is greater than 1, return value",
          {
            vect <- c('key1','1.5')
            KeyExpc <- c('key1','key2','key3')
            ValuDflt <- 3
            NameCol <- c('MyKey','MyValue')
            Type <- c('character','numeric')
            rptt <- try(NEONprocIS.base::def.vect.pars.pair(vect=vect,KeyExp=KeyExpc,ValuDflt=ValuDflt,NameCol=NameCol,Type=Type), silent = TRUE)
            testthat::expect_false((class(rptt)[1] == "try-error"))
            testthat::expect_equal(2, length(rptt))
            testthat::expect_equal(rptt$MyKey[1], "key1")
            testthat::expect_equal(rptt$MyKey[2], "key2")
            testthat::expect_equal(rptt$MyKey[3], "key3")
            testthat::expect_equal(rptt$MyValue[1], 1.5)
            testthat::expect_equal(rptt$MyValue[2], 3.0)
            testthat::expect_equal(rptt$MyValue[3], 3.0)


          })