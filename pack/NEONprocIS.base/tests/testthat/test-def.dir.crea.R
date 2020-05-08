
test_that("Data directory created without subpath",
          {
            dirBgn <- "tests/testthat/def.dir.crea/test_output"
            dirSub <- "dirCreaTest"
            NEONprocIS.base::def.dir.crea(DirBgn = dirBgn, DirSub =dirSub)
            testthat::expect_true(dir.exists("def.dir.crea/test_output/dirCreaTest"))
            if (file.exists("tests/testthat/def.dir.crea/test_output/dirCreaTest")) { file.remove("tests/testthat/def.dir.crea/test_output/dirCreaTest")}
          })
