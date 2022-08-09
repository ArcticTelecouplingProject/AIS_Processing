################################################################################
# TITLE: Spoof Removing Script 
# PURPOSE:  A script to remove spoofed lines from the already vectorized AIS data 
# AUTHOR: Kelly Kapsar 
# CREATED: ??
# LAST UPDATED ON: --
# 
# NOTE: For more on spoofing see: https://globalfishingwatch.org/data/spoofing-one-identity-shared-by-multiple-vessels/
################################################################################

# Load libraries
library(sf)
library(dplyr)
library(tidyr)

# Import data 
files <- list.files("../Data_Processed_HPCC_SPOOFIN/2020/Vector", pattern=".shp", full.names=T) 

despoof <- function(file){
  sffile <- st_read(file, quiet=TRUE)
  # Remove all NA columns 
  sffile <- sffile %>% select(-MMSI_y,-Nvgtnl_, -SOG, -Longitd, -Latitud)
  # Calculate length of lines
  sffile$length_km <- as.numeric(st_length(sffile)/1000)
  # Remove impossibly long lines (and count how many were removed)
  removed <- sum(sffile$length_km > 10000)
  sffile <- sffile %>% filter(length_km < 10000)
  
  # Save revised file 
  yr <- substr(na.omit(sffile$Time)[1],1,4)
  mnth <- substr(na.omit(sffile$Time)[1],5,6)
  type <- sffile$AIS_Typ[1]
  write_sf(sffile, paste0("../Data_Processed_HPCC_FINAL/",yr,"/Vector/Tracks_",yr,"-",mnth,"-",type,".shp"))
  df <- data.frame(yr=yr, mnth=mnth,type=sffile$AIS_Typ[1],removed=removed)
  return(df)
}

out <- lapply(files, despoof)
remove.df <- do.call(rbind, out)
write.csv(remove.df, "../Data_Processed_HPCC_FINAL/2020/Metadata/Removals.csv")


# Check number of days of data per file 
files <- list.files("../Data_Processed_HPCC_FINAL/2020/Vector", pattern=".shp", full.names=T) 

checkdays <- function(file){
  sffile <- st_read(file, quiet=TRUE)
  yr <- substr(na.omit(sffile$Time)[1],1,4)
  mnth <- substr(na.omit(sffile$Time)[1],5,6)
  type <- sffile$AIS_Typ[1]
  days <- length(unique(substr(na.omit(sffile$Time),7,8)))
  df <- data.frame(yr=as.numeric(yr), mnth=as.numeric(mnth),type=sffile$AIS_Typ[1],days=days)
  return(df)
}

daysout <- lapply(files, checkdays)

days.df <- do.call(rbind, daysout)
expected <- data.frame(mnth=1:12, expected=c(31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31))

days.df <- left_join(days.df, expected, by="mnth")
days.df$daysaccurate <- days.df$days == days.df$expected

write.csv(days.df, "../Data_Processed_HPCC_FINAL/2020/Metadata/DaysOfData.csv")
