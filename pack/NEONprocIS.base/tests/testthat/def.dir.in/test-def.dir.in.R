library(testthat)
source("R/def.dir.in.R")

test_that("Data directory input paths",
          {
            #DirBgn='/scratch/pfs/proc_group/prt/27134/2019/01/01',nameDirSub=c('data','calibration')
            dirBgn <- "tests/testthat/def.dir.crea/test_input"
            subDir<-  c('data','calibration')
            def.dir.in(DirBgn = dirBgn, nameDirSub =dirSub)
            testthat::expect_true(dir.exists("tests/testthat/def.dir./test_output/dirCreaTest"))
            
          })







