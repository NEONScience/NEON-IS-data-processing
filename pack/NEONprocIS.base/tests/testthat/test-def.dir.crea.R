library(testthat)
source("R/def.dir.crea.R")

test_that("Data directory created without subpath",
          {
            dirBgn <- "tests/testthat/def.dir.crea/test_output"
            dirSub <- "dirCreaTest"
            NEONprocIS.base::def.dir.crea(DirBgn = dirBgn, DirSub =dirSub)
            testthat::expect_true(dir.exists("tests/testthat/def.dir.crea/test_output/dirCreaTest"))
       if (dir.exists("tests/testthat/def.dir.crea/test_output/dirCreaTest")) {  unlink("tests/testthat/def.dir.crea/test_output/dirCreaTest")}
            
            
          })







