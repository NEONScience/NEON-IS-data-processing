library(testthat)
source("R/def.schm.avro.pars.R")

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

