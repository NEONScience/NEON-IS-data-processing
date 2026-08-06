library(NEONprocIS.base)

getwd()

list.files <- list.files(path = "~/pfs/l4discharge_predict", pattern = ".*\\.parquet$", full.names = TRUE, recursive = TRUE)

list.files2 <- list.files(path = "~/pfs/l4discharge_group_and_parse/2025/09", pattern = ".*\\.csv",full.names = TRUE, recursive = TRUE)


CSD_15_min <- data.frame()
for(i in 1:length(list.files)){
    CSD_15_min_i  <-  base::try(NEONprocIS.base::def.read.parq(list.files[i]))
    CSD_15_min <- rbind(CSD_15_min, CSD_15_min_i)
}

library(ggplot2)

CSD_15_min$siteID <- substr(CSD_15_min$curveID, 1, 4)

# Plot discharge over time faceted by siteID
ggplot(CSD_15_min, aes(x = startDateTime, y = dischargeContinuous))+
  geom_point() +
  facet_wrap(~ siteID, scales = "free_y") +
  labs(title = "Discharge Over Time by Site", x = "Date", y = "Discharge (m³/s)") +
  theme_minimal()
