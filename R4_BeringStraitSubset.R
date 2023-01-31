################################################################################
# TITLE: Bering Strait Subset of Vessel Traffic Data
# PURPOSE: This code takes the Pacific Arctic Vessel Traffic Data Set from 
# 2015-2022 and creates a subset for just the Bering Strait. Results are saved 
# in both shapefile format and as csvs with summary statistics. 
# AUTHOR: Kelly Kapsar
# CREATED: 2023-01-31
# LAST UPDATED ON: 2023-01-31
# 
# NOTE:
################################################################################

# Load libraries
library(sf)
library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)

# Specify file paths for inputs and outputs 

# Pacific Arctic AIS data
fileloc <- "D:/AIS_V2_DayNight_60km6hrgap/Vector/"

# Boundaries of study area
berstr <- st_read("C:/Users/Kelly Kapsar/OneDrive - Michigan State University/Research/2c-MetacoupledShipping/Data_Raw/StudyArea_ModifiedBerkman.shp")

# Load data 

filenames <- list.files(fileloc, pattern=".shp", full.names=TRUE)

metadata <- data.frame(yr = c(), mnth=c(), type=c(), nships=c())

for(i in 1:length(filenames)){
  if(i %% 10 == 0){print(i)}
  
  shpfile <- st_read(filenames[i])
  
  df <- data.frame(yr =lubridate::year(shpfile$tm_strt[1]), 
                   mnth=lubridate::month(shpfile$tm_strt[1]), 
                   type=shpfile$AIS_Typ[1], nships=NA)
  
  temp <- st_intersection(shpfile, berstr)
  
  
  test <- temp %>% st_drop_geometry() %>% 
    group_by(AIS_Typ) %>% 
    mutate(nships=length(unique(scrmblm)))
  
  df$nships[1] <- ifelse(length(test$nships) == 0, 0, test$nships)
  
  metadata <- rbind(metadata, df)
}

write.csv(metadata, "D:/AIS_V2_DayNight_60km6hrgap/Figures/Metadata_BeringStraitSubset.csv")


# plot(shpfile$geometry)
# plot(st_intersection(shpfile$geometry, berstr)) # st_intersection cuts the lines at the borders of the study area 

################ SUMMARY PLOTS ################ (Adapted from R4_Metadata_OutputSummaries script)

figloc <- "D:/AIS_V2_DayNight_60km6hrgap/Figures/"
locationsubset <- "BeringStrait"

# Turn year and month columns into a date format
meta$yrmnth <- as.Date(paste(meta$yr,meta$mnth,"01",sep="-"), format="%Y-%m-%d")

metaall <- meta %>% group_by(yrmnth) %>% summarize(ntotal_mmsis = sum(nships))

# Number of unique vessels over time
ggplot(metaall, aes(x=yrmnth, y=ntotal_mmsis)) +
  geom_line(lwd=1) +
  xlab("Year") +
  scale_x_date(date_labels = "%Y", date_breaks="1 year", expand=c(0,0)) +
  # scale_y_continuous(breaks=seq(0,2000, by=500), limits=c(0,NA)) +
  ylab("Unique ships") +
  theme_bw() +
  theme(text = element_text(size=30))
ggsave(paste0(figloc,"NumberShipsPerMonth_", locationsubset, ".png"),width=25,height=10,units='in',dpi=300)


# Number of unique vessels by tupe over time
ggplot(meta, aes(x=yrmnth, y=nships)) +
  geom_line(aes(color=type), lwd=1) +
  geom_smooth(aes(color=type)) +
  facet_wrap(~type, scales="free") +
  xlab("Year") +
  scale_x_date(date_labels = "%Y", date_breaks="1 year", expand=c(0,0)) +
  # scale_y_continuous(breaks=seq(0,2000, by=500), limits=c(0,NA)) +
  ylab("Unique ships") +
  theme_bw() +
  theme(text = element_text(size=30), 
        axis.text.x = element_text(angle=45, hjust=1))
ggsave(paste0(figloc,"NumberShipsPerMonthByType_", locationsubset, ".png"), width=25,height=15,units='in',dpi=300)
