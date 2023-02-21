################################################################################
# TITLE: AIS Vectorization Script
#
# PURPOSE: This script takes individual daily csv files from exactEarth, cleans
# data to remove erroneous points, and linearly interpolates to create daily
# transit segments for individual vessels in vector format. 
#
# AUTHORS: Ben Sullender & Kelly Kapsar
# CREATED: 2021
# LAST UPDATED ON: 2023-02--1
# 
# NOTE 2: CODE DESIGNED TO RUN ON HPCC.
################################################################################

# Start timer
start <- proc.time()

# Load libraries 
library(maptools)
library(rgdal)
library(dplyr)
library(tidyr)
library(tibble)
library(sf)
library(purrr)
library(foreach)
library(doParallel)

####################################################################
##################### AIS PROCESSING FUNCTION ######################
####################################################################

# INPUTS: A list of lists containing all daily csv file names for one year of AIS data (organized by month).  
## Inner list = file paths/names for daily AIS csvs
## Outer list = a list of months 

# OUTPUTS: 
## Monthly shapefiles for each ship type containing vectorized daily ship transit segments
## Text file with output information (number of unique ships, rows excluded, etc.)

FWS.AIS <- function(csvList, flags, scrambleids, daynight=FALSE){
  # Start overall timer 
  starttime <- proc.time()
  
  ##################### Initial data import ######################
  # Start segment timer 
  start <- proc.time()
  
  # this will come in handy later. chars 28 to 34 = "yyyy-mm"
  print(csvList[[1]][1])
  
  MoName <- substr(csvList[[1]][1],45, 51) # HPCC
  # MoName <- substr(csvList[[1]][1],77, 83) # MY COMPUTER
  yr <- substr(MoName, 1, 4) 
  mnth <- substr(MoName, 6, 7)
  print(paste("Processing",yr, mnth))
  
  # set up dfs for metadata and runtimes
  metadata <- data.frame(yr = yr, mnth=mnth)
  runtimes <- data.frame(yr = yr, mnth=mnth)
  
  # read in csv (specifying only columns that we want based on position in dataframe)
  temp <-  lapply(csvList, read.csv, header=TRUE, na.strings=c("","NA"),
                  colClasses = c(rep("character", 2), "NULL", "character", "NULL", "NULL", 
                                 "character", rep("NULL", 6), "character", "NULL", rep("character",8),
                                 "NULL", "character", "NULL", "character", "NULL", rep("character", 2), rep("NULL", 109)
                  ))

  AIScsv <- do.call(rbind , temp)

  metadata$orig_MMSIs <- length(unique(AIScsv$MMSI))
  metadata$orig_pts <- length(AIScsv$MMSI)
  
  runtimes$importtime <- (proc.time() - start)[[3]]/60
  
  # print(paste("Imported: ",yr, mnth))
  ##################### Data cleaning ######################
  start <- proc.time()
  
  # Convert character columns to numeric as needed
  numcols <- c(1:2, 6:12, 14:17)
  AIScsv[,numcols] <- lapply(AIScsv[,numcols], as.numeric)
  
  # print(paste("Numeric cols: ",yr, mnth))
  # Create df using only position messages (excluding type 27 which has increased location error)
  # For more info on message types see: https://www.marinfo.gc.ca/e-nav/docs/list-of-ais-messages-en.php
  AIScsvDF5 <- AIScsv %>%
    dplyr::select(MMSI,Latitude,Longitude,Time,Message_ID,SOG) %>%
    subset(!(Message_ID %in% c(5,24, 27)))
  
  metadata$messid_mmsis <- length(unique(AIScsvDF5$MMSI))
  metadata$messid_pts <- length(AIScsvDF5$MMSI)
  
  # print(paste("Position messages: ",yr, mnth))
  # Remove invalid lat/long values
  AIScsvDF4 <- AIScsvDF5 %>%
    filter(!is.na(Latitude)) %>%
    filter(!is.na(Longitude)) 
  
  metadata$invallatlon__mmsis <- length(unique(AIScsvDF4$MMSI))
  metadata$invallatlon__pts <- length(AIScsvDF4$MMSI)
  
  # print(paste("Inval lat/lon: ",yr, mnth))
  # Remove invalid MMSIs
  AIScsvDF3 <- AIScsvDF4 %>%
    dplyr::filter(nchar(trunc(abs(MMSI))) == 9)
  
  metadata$invalmmsi__mmsis <- length(unique(AIScsvDF3$MMSI))
  metadata$invalmmsi__pts <- length(AIScsvDF3$MMSI)
  
  # print(paste("Inval MMSI: ",yr, mnth))
  # Remove stationary aids to navigation
  AIScsvDF2 <- AIScsvDF3 %>%
    dplyr::filter(MMSI < 990000000) 
  
  metadata$aton_mmsis <- length(unique(AIScsvDF2$MMSI))
  metadata$aton_pts <- length(AIScsvDF2$MMSI)

  # Remove original MMSI and switch to scrambled version 
  AIScsvDF1 <- AIScsvDF2  %>% left_join(scrambleids, by="MMSI") %>% dplyr::select(-MMSI)
  
  # Create AIS_ID field
  AIScsvDF <- AIScsvDF1 %>% add_column(AIS_ID = paste0(AIScsvDF1$scramblemmsi,"-",substr(AIScsvDF1$Time,1,8)))
  
  # Identify and remove frost flowers 
  # (i.e. multiple messages from the same exact location sporadically transmitted throughout the day)
  ff <- AIScsvDF %>% 
    group_by(AIS_ID, Longitude, Latitude) %>% 
    summarize(n=n()) %>% filter(n > 2) %>% 
    mutate(tempid = paste0(AIS_ID, Longitude, Latitude))
  
  AISspeed4 <- AIScsvDF %>% 
    mutate(tempid = paste0(AIS_ID, Longitude, Latitude)) %>% 
    filter(!(tempid %in% ff$tempid))
    
  runtimes$dftime <- (proc.time() - start)[[3]]/60
  
  print(paste("Finished Cleaning ",yr, mnth))
  
  ##################### Speed filtering ######################
  start <- proc.time()
  
  # Make dataframe into spatial object 
  AISspeed3 <- AISspeed4 %>%
    mutate(Time = as.POSIXct(Time, format="%Y%m%d_%H%M%OS", tz="GMT")) %>% # S-AIS are in UTC with is GMT 
    st_as_sf(coords=c("Longitude","Latitude"),crs=4326, remove = FALSE) %>%
    # project into Alaska Albers (or other CRS that doesn't create huge gap in mid-Bering with -180W and 180E)
    st_transform(crs=3338) %>%
    arrange(Time)
  
  # Calculate the euclidean speed between points
  # Also remove successive duplicate points (i.e., same time stamp and/or same location)
  AISspeed3[, c("x", "y")] <- st_coordinates(AISspeed3)
  AISspeed2 <- AISspeed3 %>% 
    group_by(AIS_ID) %>%
    arrange(AIS_ID, Time) %>% 
    mutate(timediff = as.numeric(difftime(Time,lag(Time),units=c("hours"))),
           distdiff = sqrt((y-lag(y))^2 + (x-lag(x))^2)/1000) %>% 
    filter(timediff > 0) %>% 
    filter(distdiff > 0) %>% 
    mutate(speed = distdiff/timediff)
  
  metadata$redund_aisids <- length(unique(AISspeed2$AIS_ID))
  metadata$redund_mmsi <- length(unique(AISspeed2$scramblemmsi))
  metadata$redund_pts <- length(AISspeed2$scramblemmsi)
  
  # Implement speed filter of 100 km/hr (also remove NA speed)
  AISspeed1 <- AISspeed2 %>% dplyr::filter(speed < 100) %>% dplyr::filter(!is.na(speed))
  
  AISspeed1 <- AISspeed1 %>% rename(long=Longitude, lat=Latitude)
  
  metadata$speed_aisids <- length(unique(AISspeed1$AIS_ID))
  metadata$speed_mmsi <- length(unique(AISspeed1$scramblemmsi))
  metadata$speed_pts <- length(AISspeed1$scramblemmsi)

  # Create new segment ids for gaps greater than 60 km or 6 hrs
  # Recalculate new distance and time difference now that speedy points have been removed 
  AISspeed1 <- AISspeed1 %>% 
    group_by(AIS_ID) %>%
    arrange(AIS_ID, Time) %>% 
    mutate(timediff = as.numeric(difftime(Time,lag(Time),units=c("hours"))),
           distdiff = sqrt((y-lag(y))^2 + (x-lag(x))^2)/1000) %>% 
    ungroup()
  
  # Calculate whether points are occurring during daytime or at night 
  if(daynight==TRUE){
    print(paste("Spatial ",yr, mnth))
    temp <- st_coordinates(st_transform(AISspeed1, 4326))
    
    solarpos <- maptools::solarpos(temp, dateTime=AISspeed1$Time, POSIXct.out=TRUE)
    sunrise <- maptools::sunriset(temp, dateTime=AISspeed1$Time, direction="sunrise", POSIXct.out=TRUE)
    sunset <- maptools::sunriset(temp, dateTime=AISspeed1$Time, direction="sunset", POSIXct.out=TRUE)
    
    print(paste("Sunrise",yr, mnth))
    AISspeed1$solarpos <- solarpos[,2]
    AISspeed1$timeofday <- as.factor(ifelse(AISspeed1$solarpos > 0, "day","night"))
    
    AISspeed1$sunrise <- sunrise$time
    AISspeed1$sunset <- sunset$time
  }
  
  
  AISspeed1$newseg <- ifelse(AISspeed1$timediff > 6, 1, 
                             ifelse(AISspeed1$distdiff > 60, 1, 0))
  
  # Create new ID for each segment 
  AISspeed1$newseg[is.na(AISspeed1$newseg)] <- 1
  
  if(daynight==TRUE){
    AISspeed1$newseg[which(AISspeed1$timeofday != dplyr::lag(AISspeed1$timeofday))] <- 1
    temp <- AISspeed1 %>% 
      st_drop_geometry() %>% 
      dplyr::select(AIS_ID, newseg, timeofday) %>% 
      group_by(AIS_ID) %>% 
      mutate(newseg = cumsum(newseg))
  }
  if(daynight==FALSE){
    temp <- AISspeed1 %>% 
      st_drop_geometry() %>% 
      dplyr::select(AIS_ID, newseg) %>% 
      group_by(AIS_ID) %>% 
      mutate(newseg = cumsum(newseg))
  }
  
  AISspeed1$newsegid <- paste0(AISspeed1$AIS_ID, temp$newseg)
  
  # Remove segments with only one point (can't be made into lines)
  shortids <- AISspeed1 %>% st_drop_geometry() %>% group_by(newsegid) %>% summarize(n=n()) %>% dplyr::filter(n < 2)
  AISspeed <- AISspeed1[!(AISspeed1$newsegid %in% shortids$newsegid),]

  metadata$short_aisids <- length(unique(AISspeed$AIS_ID))
  metadata$short_mmsi <- length(unique(AISspeed$scramblemmsi))
  metadata$short_pts <- length(AISspeed$scramblemmsi)
  
  runtimes$speedtime <- (proc.time() - start)[[3]]/60
  
  print(paste("Finished Speed filter ",yr, mnth))
  ##################### Vectorization ######################
  start <- proc.time()
  
  # create sf lines by sorted / grouped points
  if(daynight==TRUE){
    AISsftemp <- AISspeed %>%
      arrange(Time) %>%
      # create 1 line per AIS ID
      group_by(newsegid) %>%
      # keep MMSI for lookup / just in case; do_union is necessary for some reason, otherwise it throws an error
      summarize(scramblemmsi=first(scramblemmsi), 
                Time_Of_Day=first(timeofday),
                TIme_Start = as.character(first(Time)), 
                Time_End = as.character(last(Time)),
                AIS_ID=first(AIS_ID), 
                SOG_Median=median(SOG, na.rm=T), 
                SOG_Mean=mean(SOG, na.rm=T), 
                do_union=FALSE, 
                npoints=n())
    
    AISsftempnew <- AISsftemp[AISsftemp$npoints > 1,]
    
    metadata$daynightshort_aisids <- length(unique(AISsftempnew$AIS_ID))
    metadata$daynightshort_mmsi <- length(unique(AISsftempnew$scramblemmsi))
    
    AISsf <- AISsftempnew %>% 
      st_cast("LINESTRING") %>% 
      st_make_valid() %>% 
      ungroup()
  }
  if(daynight==FALSE){
    AISsf <- AISspeed %>%
      arrange(Time) %>%
      # create 1 line per AIS ID
      group_by(newsegid) %>%
      # keep MMSI for lookup / just in case; do_union is necessary for some reason, otherwise it throws an error
      summarize(scramblemmsi=first(scramblemmsi), 
                Time_Start = as.character(first(Time)), 
                Time_End = as.character(last(Time)),
                AIS_ID=first(AIS_ID), 
                SOG_Median=median(SOG, na.rm=T), 
                SOG_Mean=mean(SOG, na.rm=T), 
                do_union=FALSE, 
                npoints=n()) %>% 
      st_cast("LINESTRING") %>% 
      st_make_valid() %>% 
      ungroup()
  }
  # Figure out which rows aren't LINESTRINGS and remove from data 
  notlines <- AISsf[which(st_geometry_type(AISsf) != "LINESTRING"),]
  AISsf <- AISsf[which(st_geometry_type(AISsf) == "LINESTRING"),]
  
  metadata$NotLine_aisids <- length(AISsf$AIS_ID)
  metadata$NotLine_mmsis <- length(AISsf$scramblemmsi)
  
  runtimes$linetime <- (proc.time() - start)[[3]]/60
  
  print(paste("Finished Vectorization ",yr, mnth))
  ##################### Join position information with static information ######################
  start <- proc.time()
  
  # Calculate total distance travelled
  AISsf$Length_Km <- as.numeric(st_length(AISsf)/1000)

  # create lookup table from static messages
  AISlookup1 <- AIScsv %>%
    add_column(Dim_Length = AIScsv$Dimension_to_Bow+AIScsv$Dimension_to_stern, 
               Dim_Width = AIScsv$Dimension_to_port+AIScsv$Dimension_to_starboard) %>%
    filter(Message_ID %in% c(5,24)) %>%
    dplyr::select(-Dimension_to_Bow,
                  -Dimension_to_stern,
                  -Dimension_to_port,
                  -Dimension_to_starboard, 
                  -Navigational_status, 
                  -SOG, 
                  -Longitude, 
                  -Latitude, 
                  -Time,
                  -Message_ID) %>%
    filter(nchar(trunc(abs(MMSI))) == 9) 
  
  # Identify "best" static message from a given day 
  # Take all the static messages from a given month and down weight messages with a ship 
  # type of 0 or NA. Take the static messages and keeps the message with the highest weight
  # Code adapted from: https://stackoverflow.com/questions/72650475/take-unique-rows-in-r-but-keep-most-common-value-of-a-column-and-use-hierarchy
  wghts <- data.frame(poss = c(0, NA), nums = c(-10, -10))
  matched <- left_join(AISlookup1, wghts, by = c("Ship_Type"= "poss"))
  matched$nums[is.na(matched$nums)] <- 1
  data.table::setDT(matched)[, freq := .N, by = c("MMSI", "Ship_Type")]
  multiplied <- distinct(matched, MMSI, Ship_Type, .keep_all = TRUE)
  multiplied$mult <- multiplied$nums * multiplied$freq
  check <- multiplied[with(multiplied, order(MMSI, -mult)), ]
  AISlookup <- distinct(check, MMSI, .keep_all = TRUE) %>% dplyr::select(-nums, -freq, -mult)
  
  metadata$InSfNotLookup_mmsis <- length(AISsf$MMSI[!(AISsf$MMSI %in% AISlookup$scramblemmsi)])
  metadata$InLookupNotSf_mmsis <- length(AISlookup$MMSI[!(AISlookup$MMSI %in% AISsf$scramblemmsi)])
  
  # Add flag codes (based on original MMSI) 
  # and join with scramblemmsi
  AISlookup <- AISlookup %>% 
    mutate(CountryCode = as.numeric(substr(as.character(MMSI), 1,3))) %>% 
    dplyr::select(-Country) %>% 
    left_join(flags, by=c("CountryCode" = "MID")) %>% 
    left_join(scrambleids, by="MMSI") %>% 
    dplyr::select(-MMSI, -Short.Code)
  
  # Join lookup table to the lines based on scramble mmsi
  AISjoined1 <- AISsf %>%
    left_join(AISlookup,by="scramblemmsi")
  
  # Split lines by ship type
  # link to ship type/numbers table: 
  # https://help.marinetraffic.com/hc/en-us/articles/205579997-What-is-the-significance-of-the-AIS-Shiptype-number-
    AISjoined <- AISjoined1 %>%
      mutate(AIS_Type = case_when(
      is.na(AISjoined1$Ship_Type) ~ "Other",
      substr(AISjoined1$Ship_Type,1,2)==30 ~ "Fishing",
      substr(AISjoined1$Ship_Type,1,2)==52 ~ "Tug",
      substr(AISjoined1$Ship_Type,1,1)==6 ~ "Passenger",
      substr(AISjoined1$Ship_Type,1,2)==36 ~ "Sailing",
      substr(AISjoined1$Ship_Type,1,2)==37 ~ "Pleasure",
      substr(AISjoined1$Ship_Type,1,1)==7 ~ "Cargo",
      substr(AISjoined1$Ship_Type,1,1)==8 ~ "Tanker",
      # from trial and error, I think that this last "TRUE" serves as a catch-all, but I can't logically figure out why it works. -\__(%)__/-
      TRUE ~ "Other"
    ))  
    
  # Remove identifying information 
  AISjoined <- AISjoined %>% dplyr::select(-Vessel_Name, -IMO)
  
    
  nships <- AISjoined %>% st_drop_geometry() %>% group_by(AIS_Type) %>% summarize(n=length(unique(scramblemmsi)))
  metadata$ntank_mmsis <- nships$n[nships$AIS_Type == "Tanker"]
  metadata$ntug_mmsis <- nships$n[nships$AIS_Type == "Tug"]
  metadata$npass_mmsis <-nships$n[nships$AIS_Type == "Passenger"]
  metadata$nsail_mmsis <-nships$n[nships$AIS_Type == "Sailing"]
  metadata$npleas_mmsis <-nships$n[nships$AIS_Type == "Pleasure"]
  metadata$nfish_mmsis <- nships$n[nships$AIS_Type == "Fishing"]
  metadata$ncargo_mmsis <- nships$n[nships$AIS_Type == "Cargo"]
  metadata$nother_mmsis <- nships$n[nships$AIS_Type == "Other"]
  metadata$ntotal_mmsis <- sum(nships$n)
  metadata$totallength_km <- sum(AISjoined$Length_Km)
  
  runtimes$jointime <- (proc.time() - start)[[3]]/60
  
  print(paste("Finished Static/Position Join ",yr, mnth))
  ##################### Save outputs ######################
  start <- proc.time()
  
  # Loop through each ship type, rasterize, and save shp file as well 
  allTypes <- unique(AISjoined$AIS_Type)
  
  for (k in 1:length(allTypes)){

          AISfilteredType <- AISjoined %>%
                filter(AIS_Type==allTypes[k])

          # Save data in vector format
          if(length(AISfilteredType$newsegid > 0)){
            write_sf(AISfilteredType,paste0("../Data_Processed/Vector/Tracks_DayNight", daynight, "_",MoName,"-",allTypes[k],".shp"))
          }
  }
  
# Save processing info to text file 
runtimes$vectortime <- (proc.time() - start)[[3]]/60

runtime <- proc.time() - starttime 
runtimes$runtime_min <- runtime[[3]]/60 

write.csv(metadata, paste0("../Data_Processed/Metadata/Metadata_DayNight", daynight, "_", MoName,".csv"))

write.csv(runtimes, paste0("../Data_Processed/Metadata/Runtimes_DayNight", daynight, "_",MoName,".csv"))
print(runtimes)
return(runtimes)
}

#########################################################
####################### TEST CODE ####################### 
#########################################################

# Pull up list of AIS files
# filedr <- "D:/AlaskaConservation_AIS_20210225/Data_Raw/2015/"
# 
# files <- paste0(filedr, list.files(filedr, pattern='.csv'))
# 
# # Separate file names into monthly lists
# # csvList <- files[27:28] # January 27-28
# csvList <- files[208:209] # June 27-28
# 
# flags <- read.csv("../Data_Raw/FlagCodes.csv")
# scrambleids <- read.csv("../Data_Processed/MMSIs/ScrambledMMSI_Keys_2015-2020_FROZEN.csv") %>% dplyr::select(MMSI, scramblemmsi)
# daynight <- FALSE
# 
# FWS.AIS(csvList, flags, scrambleids, daynight=TRUE)
# 
# # Frost flower segment
# scrambleids$MMSI[scrambleids$scramblemmsi == "366840976"]
# test <- AIScsv %>% filter(MMSI == "366932970")
# 
# # Fishing (non-frost flower segment)
# scrambleids$MMSI[scrambleids$scramblemmsi == "367553062"]
# AIScsv <- AIScsv %>% filter(MMSI == "367184150")
# 
# # Across alaska???
# scrambleids$MMSI[scrambleids$scramblemmsi == "338288778"]
# AIScsv <- AIScsv %>% filter(MMSI == "338189586")

####################################################################
####################### PARALLELIZATION CODE ####################### 
####################################################################

# Pull up list of AIS files
files <- paste0("../Data_Raw/2015/", list.files("../Data_Raw/2015", pattern='.csv'))

# Separate file names into monthly lists
jan <- files[grepl("-01-", files)]
feb <- files[grepl("-02-", files)]
mar <- files[grepl("-03-", files)]
apr <- files[grepl("-04-", files)]
may <- files[grepl("-05-", files)]
jun <- files[grepl("-06-", files)]
jul <- files[grepl("-07-", files)]
aug <- files[grepl("-08-", files)]
sep <- files[grepl("-09-", files)]
oct <- files[grepl("-10-", files)]
nov <- files[grepl("-11-", files)]
dec <- files[grepl("-12-", files)]

# Create a list of lists of all csv file names grouped by month
csvsByMonth <- list(jan, feb, mar, apr, may, jun, jul, aug, sep, oct, nov, dec)

# Import flag code reference list
flags <- read.csv("../Data_Raw/FlagCodes.csv")

# Import scramblemmsi codes
scrambleids <- read.csv("../Data_Raw/ScrambledMMSI_Keys_2015-2022.csv") %>% dplyr::select(MMSI, scramblemmsi)

## MSU HPCC: https://wiki.hpcc.msu.edu/display/ITH/R+workshop+tutorial#Rworkshoptutorial-Submittingparalleljobstotheclusterusing{doParallel}:singlenode,multiplecores
# Request a single node (this uses the "multicore" functionality)
registerDoParallel(cores=as.numeric(Sys.getenv("SLURM_CPUS_ON_NODE")[1]))

# create a blank list to store the results (I truncated the code before the ship-type coding, and just returned the sf of all that day's tracks so I didn't 
#       have to debug the raster part. If we're writing all results within the function - as written here and as I think we should do - the format of the blank list won't really matter.)
res=list()

# foreach and %dopar% work together to implement the parallelization
# note that you have to tell each core what packages you need (another reason to minimize library use), so it can pull those over
# I'm using tidyverse since it combines dplyr and tidyr into one library (I think)
res=foreach(i=1:12,.packages=c("maptools", "rgdal", "dplyr", "tidyr", "tibble", "stars", "raster", "foreach","purrr", "doParallel", "data.table"),
            .errorhandling='pass',.verbose=T,.multicombine=TRUE) %dopar% 
  FWS.AIS(csvList=csvsByMonth[[i]], flags=flags, scrambleids=scrambleids, daynight=FALSE)
# lapply(csvsByMonth, FWS.AIS)

# Elapsed time and running information
tottime <- proc.time() - start
tottime_min <- tottime[[3]]/60

cat("Time elapsed:", tottime_min, "\n")
cat("Currently registered backend:", getDoParName(), "\n")
cat("Number of workers used:", getDoParWorkers(), "\n")

