################################################################################
# TITLE: Summary Plots from Raster  Metadata
# PURPOSE: This code uses the metadata files developed in the R3_AIS_Rasterization.R
#   script to create summary plots of vessel traffic over the study period. 
# AUTHOR: Kelly Kapsar
# CREATED: 2021-22
# LAST UPDATED ON: 2023-01-31
# 
# NOTE:
################################################################################

# Load libraries 
library(tidyr)
library(dplyr)
library(ggplot2)
library(RColorBrewer)

# Fig loc 
figloc <- "D:/AIS_V2_DayNight_60km6hrgap/Figures/"
locationsubset <- "WholeStudyArea"

# Read in metadata from all years
filenames <- list.files("D:/AIS_V2_DayNight_60km6hrgap/Metadata/", pattern='Metadata', full.names=T)

allyrs <- lapply(filenames, read.csv)

meta <- allyrs %>% bind_rows(.)

# Turn year and month columns into a date format
meta$yrmnth <- as.Date(paste(meta$yr,meta$mnth,"01",sep="-"), format="%Y-%m-%d")

# Number of unique vessels over time
ggplot(meta, aes(x=yrmnth, y=ntotal_mmsis)) +
  geom_line(lwd=1) +
  xlab("Year") +
  scale_x_date(date_labels = "%Y", date_breaks="1 year", expand=c(0,0)) +
  scale_y_continuous(breaks=seq(0,2000, by=500), limits=c(0,NA)) +
  ylab("Unique ships") +
  theme_bw() +
  theme(text = element_text(size=30))
ggsave(paste0(figloc,"NumberShipsPerMonth_", locationsubset, ".png"),width=25,height=10,units='in',dpi=300)


# Monthly traffic by vessel type 
types <- meta %>% dplyr::select(yrmnth, 
                               ntank_mmsis, 
                               ntug_mmsis, 
                               npass_mmsis, 
                               nsail_mmsis, 
                               npleas_mmsis, 
                               nfish_mmsis, 
                               ncargo_mmsis, 
                               nother_mmsis) %>% 
  gather(data =., key="type", value="nships",-yrmnth) %>% 
  mutate(type = recode(type, ntank_mmsis = "Tanker", 
                       ntug_mmsis = "Tug", 
                       npass_mmsis = "Passenger", 
                       nsail_mmsis = "Sailing", 
                       npleas_mmsis = "Pleasure", 
                       nfish_mmsis = "Fishing", 
                       ncargo_mmsis = "Cargo", 
                       nother_mmsis = "Other"))

# Number of unique vessels by tupe over time
ggplot(types, aes(x=yrmnth, y=nships)) +
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

# VERIFICATION that the total number of mmsis is equivalent to the sum of the number of ships of each type. 
test <- types %>% group_by(yrmnth) %>% summarize(nships =sum(nships))
temp <- meta %>% dplyr::select(yrmnth, ntotal_mmsis) %>% left_join(., test, by=c("yrmnth" = "yrmnth"))
which(temp$ntotal_mmsis != temp$nships)

