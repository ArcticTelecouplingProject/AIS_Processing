################################################################################
# TITLE: Summary Plots from Raster  Metadata
# PURPOSE: This code uses the metadata files developed in the R3_AIS_Rasterization.R
#   script to create summary plots of vessel traffic over the study period. 
# AUTHOR: Kelly Kapsar
# CREATED: ??
# LAST UPDATED ON: 2022-06-09
# 
# NOTE:
################################################################################

# Load libraries 
library(tidyr)
library(dplyr)
library(ggplot2)
library(RColorBrewer)

# Read in metadata from all years
files2015 <- list.files("D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2015/Metadata/", pattern='Metadata', full.names=T)
files2016 <- list.files("D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2016/Metadata/", pattern='Metadata', full.names=T)
files2017 <- list.files("D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2017/Metadata/", pattern='Metadata', full.names=T)
files2018 <- list.files("D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2018/Metadata/", pattern='Metadata', full.names=T)
files2019 <- list.files("D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2019/Metadata/", pattern='Metadata', full.names=T)
files2020 <- list.files("D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/2020/Metadata/", pattern='Metadata', full.names=T)

allyrs <- list(files2015, files2016, files2017, files2018, files2019, files2020)

nestedlist <- lapply(allyrs, function(x){lapply(x, read.csv)})

t <- do.call(Map, c(f = rbind, nestedlist))
meta <- do.call(rbind, t)

# Turn year and month columns into a date format
meta$yrmnth <- as.Date(paste(meta$yr,meta$mnth,"01",sep="-"), format="%Y-%m-%d")

# Number of unique vessels over time
ggplot(meta, aes(x=yrmnth, y=nmmsi)) +
  geom_line(lwd=1) +
  xlab("Year") +
  scale_x_date(date_labels = "%Y", date_breaks="1 year", expand=c(0,0)) +
  scale_y_continuous(breaks=seq(0,2000, by=500), limits=c(0,NA)) +
  ylab("Unique ships") +
  theme_bw() +
  theme(text = element_text(size=30))
# ggsave("D:/AlaskaConservation_AIS_20210225/Figures/NumberShipsPerMonth.png",width=25,height=10,units='in',dpi=300)


# Shipping days by type over time
longmeta <- meta %>% 
            select(yrmnth, ntank_aisids, nfish_aisids, ncargo_aisids, nother_aisids) %>% 
            gather(key=type,value=naisids, ntank_aisids:nother_aisids)

ggplot(longmeta, aes(x=yrmnth, y=naisids)) +
  geom_line(aes(color=type), lwd=1)+
  scale_color_brewer(palette = "Dark2", name="Ship Type", 
                     breaks=c("nfish_aisids", "nother_aisids", "ncargo_aisids", "ntank_aisids"), 
                     labels=c("Fishing", "Other", "Cargo", "Tanker")) +
  xlab("Year") +
  scale_x_date(date_labels = "%Y", date_breaks="1 year", expand=c(0,0)) +
  scale_y_continuous(expand=c(0,1000)) +
  ylab("Operating days") + 
  theme_bw() +
  theme(text = element_text(size=30))
# ggsave("D:/AlaskaConservation_AIS_20210225/Figures/OperatingDaysPerMonth_ByType.png",width=25,height=10,units='in',dpi=300)


# write.csv(meta, "D:/AlaskaConservation_AIS_20210225/Data_Processed_HPCC_FINAL/Metadata.csv")
