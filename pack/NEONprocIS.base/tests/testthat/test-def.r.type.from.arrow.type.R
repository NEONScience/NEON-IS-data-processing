library(testthat)

test_that("integer Arrow types map to integer()", {
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::int8()),   integer())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::int16()),  integer())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::int32()),  integer())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::int64()),  integer())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::uint8()),  integer())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::uint16()), integer())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::uint32()), integer())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::uint64()), integer())
})

test_that("float Arrow types map to numeric()", {
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::float16()), numeric())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::float32()), numeric())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::float64()), numeric())
  # expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::double()),   numeric())
})

test_that("string Arrow types map to character()", {
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::string()), character())
  # expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::large_string()), character())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::utf8()), character())
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::large_utf8()), character())
})

test_that("boolean type maps to logical()", {
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::boolean()), logical())
})

test_that("binary types map to list(raw())", {
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::binary()),       list(raw()))
  expect_identical(NEONprocIS.base::def.r.type.from.arrow.type(arrow::large_binary()), list(raw()))
})

test_that("date types map to as.Date(character())", {
  expect_identical(
    NEONprocIS.base::def.r.type.from.arrow.type(arrow::date32()),
    as.Date(character())
  )
  expect_identical(
    NEONprocIS.base::def.r.type.from.arrow.type(arrow::date64()),
    as.Date(character())
  )
})

test_that("timestamp types map to POSIXct()", {
  # no timezone → default "GMT"
  ts1 <- NEONprocIS.base::def.r.type.from.arrow.type(arrow::timestamp(unit = "us"))
  expect_s3_class(ts1, "POSIXct")
  expect_identical(attr(ts1, "tzone"), "GMT")
  
  # with timezone
  ts2 <- NEONprocIS.base::def.r.type.from.arrow.type(arrow::timestamp(unit = "ms", timezone = "UTC"))
  expect_s3_class(ts2, "POSIXct")
  expect_identical(attr(ts2, "tzone"), "UTC")
})

test_that("list types map recursively", {
  list_type <- arrow::list_of(arrow::int32())
  result <- NEONprocIS.base::def.r.type.from.arrow.type(list_type)
  
  expect_type(result, "list")
  expect_identical(result[[1]], integer())
})

test_that("struct types map recursively", {
  st <- arrow::struct(a = arrow::int32(), b = arrow::utf8())
  
  result <- NEONprocIS.base::def.r.type.from.arrow.type(st)
  
  expect_type(result, "list")
  expect_named(result, c("a", "b"))
  expect_identical(result$a, integer())
  expect_identical(result$b, character())
})

test_that("unsupported type returns NULL", {
  unsupported <- arrow::duration("ms")  # duration not supported in your function
  expect_null(NEONprocIS.base::def.r.type.from.arrow.type(unsupported))
})