library(magrittr)
library(stringr)
library(tidyr)
library(dplyr)

dirInData <- '/scratch/pfs/pluviou_data_source_kafka/pluvio_raw/2024/'
files <- list.files(dirInData, recursive = T, full.names = T, pattern = '.parquet')
files_trnc <- files[grep(files, pattern = '55221')]
pluvio_all <- data.frame()

for (file in files){
pluvio_raw <- NEONprocIS.base::def.read.parq(file)
pluvio <- pluvio_raw %>% 
  dplyr::mutate(serial_output = stringr::str_remove_all(serial_output, pattern = "\\+")) %>%
  tidyr::separate(col = serial_output, 
                  into = c("intensity_rt",
                              "accu_rt_nrt",
                              "accu_nrt",
                              "accu_total_nrt",
                              "bucket_rt",
                              "bucket_nrt",
                              "cell_temperature",
                              "heater_status",
                              "sensorStatus",
                              "electronics_temperature",
                              "supply_voltage",
                              "inletTemp"), sep = ";")
pluvio_all <- rbind(pluvio_all, pluvio)
}

pluvio_all <- pluvio_all %>% 
  dplyr::mutate_at(c("intensity_rt",
              "accu_rt_nrt",
              "accu_nrt",
              "accu_total_nrt",
              "bucket_rt",
              "bucket_nrt",
              "cell_temperature",
              "heater_status",
              "sensorStatus",
              "electronics_temperature",
              "supply_voltage",
              "inletTemp"), as.numeric)


plotly::ggplotly(ggplot(pluvio_all) +
  geom_line(aes(x=readout_time, y = accu_nrt), color = 'black'))

plotly::ggplotly(ggplot(pluvio_all) +
  geom_line(aes(x=readout_time, y = bucket_nrt), color = 'blue'))
