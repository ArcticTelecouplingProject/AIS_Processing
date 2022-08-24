################################################################################
# TITLE: MMSI Scramble Script 
# 
# PURPOSE: This script takes individual daily AIS positions (in csv format) 
# from exactEarth and collects the unique MMSIs. It then creates a scrambled version 
# of the MMSI so that individual ships cannot be identified, but that individual 
# ship scrambled MMSIs remain consistent across multiple years. 
# 
# AUTHOR: Kelly Kapsar
# CREATED: 2022-08-23
# LAST UPDATED ON: 2022-08-23
################################################################################

# Load libraries 
library(dplyr)

# Set seed
set.seed(101557)

################################################################################ 
################### EXTRACT UNIQUE MMSIS FOR EACH YEAR #########################
# Run once per year of data 
# If already completed, skip to next section 

# # Specify year of data to process (the script runs one year at a time -- take 20 min to 1 hour per year of data)
# yr <- 2020
# 
# # Pull up list of AIS files
# filedr <- paste0("D:/AlaskaConservation_AIS_20210225/Data_Raw/", yr,"/")
# files <- paste0(filedr, list.files(filedr, pattern='.csv'))
# 
# # A function to extract unique MMSIs and the number of points per MMSI for each year of data 
# MMSIextract <- function(filepath){
#   csv <- read.csv(filepath, header=TRUE, colClasses = c("character", rep("NULL", 138)))
#   mmsis <- csv %>% group_by(MMSI) %>% summarize(npoints = n())
#   return(mmsis)
# }
# 
# 
# start <- proc.time()
# 
# mmsilist <- lapply(files, MMSIextract)
# mmsis <- do.call(rbind , mmsilist)
# uniquemmsis <- mmsis %>% group_by(MMSI) %>% summarize(ndays = n(), npoints=sum(npoints)) %>% mutate(year = yr)
# write.csv(uniquemmsis, paste0("./Data_Processed/MMSIs/UniqueMMSIs_",yr,".csv"), row.names=FALSE)
# 
# (proc.time()-start)/60

# browseURL("https://www.youtube.com/watch?v=K1b8AhIsSYQ")

################################################################################ 
######################## Unique MMSIs in 2015-2020 data ######################## 

# mmsiyrlist <- paste0("./Data_Processed/MMSIs/", list.files("./Data_Processed/MMSIs/", pattern='.csv'))
# 
# mmsiyrlist <- lapply(mmsiyrlist, read.csv)
# 
# mmsiall <- do.call(rbind , mmsiyrlist)
# 
# mmsi <- mmsiall %>% group_by(MMSI) %>% summarize(ndays=sum(ndays), npoints=sum(npoints)) 
# 
# filtmmsi <- mmsi %>% filter(nchar(MMSI) == 9)
# 
# # Replace last six digits of MMSI with random integers 
# filtmmsi$scramblemmsi <- paste0(substr(filtmmsi$MMSI, 1, 3), as.character(sample(100000:999999, length(filtmmsi$MMSI), replace=FALSE)))

# write.csv(filtmmsi, "./Data_Processed/MMSIs/ScrambledMMSI_Keys_2015-2020_FROZEN.csv")

################################################################################ 
######################## Unique MMSIs in future data ########################### 
# RUN ONE YEAR AT A TIME
# CODE WRITTEN BUT NOT YET TESTED

# Read in MMSI key created from 2015-2020 data 
filtmmsi <- read.csv("./Data_Processed/MMSIs/ScrambledMMSI_Keys_2015-2020.csv")

# Read in new MMSI data 
xx <- read.csv("./Data_Processed/MMSIs/UniqueMMSIs_2021.csv") %>% filter(nchar(MMSI) == 9)

# Join new and old data 
newmmsi <- full_join(filtmmsi, xx, by="MMSI")

# Calculate new numbers of points and days for all ships 
newmmsi <- newmmsi %>% 
  mutate(newnpoints = sum(npoints.x, npoints.y, na.rm=T), 
                              newndays = sum(ndays.x, ndays.y, na.rm=T)) %>% 
  select(-npoints.x, -npoints.y, -ndays.x, -ndays.y) %>% 
  rname(npoints = newnpoints, ndays=newndays)

# Don't know if this will work need to test... 
noscram <- which(is.na(newmmsi$scramblemmsi))

for(i in 1:length(noscram)){
  repeat{
    id <- paste0(substr(newmmsi$MMSI[noscram[i]], 1, 3),as.character(sample(100000:999999, 1, replace=FALSE)))
    if (!id %in% newmmsi$scramblemmsi) break 
  }
  newmmsi$scramblemmsi[i] <- id
}

# FIX BEFORE SAVING FILE
write.csv(newmmsi, "./Data_Processed/ScrambleMMSI_Keys_2015-XXXX.csv")









