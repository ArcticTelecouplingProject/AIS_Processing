clean_and_vectorize <- function(csvList, flags, scrambleids, dest, daynight){
  
  # profvis({
  # Start overall timer 
  starttime <- proc.time()
  
  ##### Load and clean data --------------------------------------------------
  
  # Start segment timer 
  start <- proc.time()
  
  # Extract year/month from files (go from end to avoid different file path issues)
  if(year %in% c(2021:2022)){
    MoName <- substr(csvList[[1]][1], nchar(csvList) - 32, nchar(csvList) - 27)
  }else(MoName <- sub("-", "", substr(csvList[[1]][1], nchar(csvList) - 13, 
                                      nchar(csvList) - 7)))
  
  yr <- substr(MoName, 1, 4) 
  mnth <- substr(MoName, 5, 7)  
  
  print(paste("Processing",yr, mnth))
  
  # set up dfs for metadata and runtimes
  metadata <- data.frame(yr = yr, mnth=mnth)
  runtimes <- data.frame(yr = yr, mnth=mnth)
  
  # Read in csvs 
  if(year %in% c(2021:2022)){
    temp <-  lapply(csvList, read.csv, header=TRUE, na.strings=c("","NA"))
  }else( 
    # read in csv (specifying only columns that we want based on position in dataframe)
    temp <-  lapply(csvList, 
                    read.csv, 
                    header=TRUE, 
                    na.strings=c("","NA"),
                    colClasses = c(rep("character", 2), 
                                   "NULL", 
                                   "character", 
                                   "NULL", 
                                   "NULL", 
                                   "character", 
                                   rep("NULL", 6), 
                                   "character", 
                                   "NULL", 
                                   rep("character",8), 
                                   "NULL", 
                                   "character", 
                                   "NULL", 
                                   "character", 
                                   "NULL", 
                                   rep("character", 2), 
                                   rep("NULL", 109)
                    )
    )
  )
  
  AIScsv <- do.call(rbind , temp)
  rm(temp)
  
  # Rename columns in new data to match old data 
  if(year %in% c(2021:2022)){
    AIScsv <- AIScsv %>% 
      subset(message_type != 27) %>% 
      rename(MMSI = mmsi, 
             IMO = imo,
             Longitude = longitude, 
             Latitude = latitude, 
             Time = dt_pos_utc, 
             Message_ID = message_type, 
             SOG = sog, 
             Ship_Type = vessel_type_code,
             Vessel_Name = vessel_name, 
             Draught = draught, 
             Destination = destination,
             Navigational_status = nav_status_code,
             Dim_Length = length, 
             Dim_Width = width) %>% 
      # S-AIS are in UTC with is GMT 
      mutate(Time = lubridate::ymd_hms(Time, tz="GMT"),
             temp = as.numeric(substr(MMSI, 1, 3)), 
             MMSI = as.integer(MMSI)) %>% 
      left_join(., flags, by=join_by(temp == MID))  %>% 
      dplyr::select(MMSI, 
                    Longitude, 
                    Latitude, 
                    Time, 
                    Message_ID, 
                    SOG, 
                    Ship_Type, 
                    Country, 
                    Vessel_Name, 
                    IMO, 
                    Draught, 
                    Destination, 
                    Navigational_status, 
                    Dim_Length, 
                    Dim_Width)
  }
  if(year %in% c(2015:2020)){
    
    # Subset position messages and clean 
    # Create df using only position messages (excluding type 27 which has increased location error)
    # For more info on message types see: https://www.marinfo.gc.ca/e-nav/docs/list-of-ais-messages-en.php
    AIScsv_pos <- AIScsv %>% 
      subset(!(Message_ID %in% c(5,24, 27))) %>% 
      mutate(MMSI = as.integer(MMSI),
             Message_ID = as.integer(Message_ID), 
             Latitude = as.numeric(Latitude),
             Longitude = as.numeric(Longitude), 
             SOG = as.numeric(SOG), 
             Navigational_status = as.numeric(Navigational_status)) %>% 
      dplyr::select(-Draught, 
                    -Destination,
                    -Country, # Some kind of error here. Deriving from MMSI instead. 
                    -Dimension_to_Bow, 
                    -Dimension_to_stern, 
                    -Dimension_to_port, 
                    -Dimension_to_starboard, 
                    -Ship_Type, 
                    -Vessel_Name,
                    -IMO) %>%   
      mutate(Time = lubridate::ymd_hms(Time, tz="GMT"), # S-AIS are in UTC with is GMT 
             Country = as.numeric(substr(MMSI, 1, 3))) %>%   
      left_join(., flags, by=join_by(Country == MID))  %>% 
      dplyr::select(-Short.Code, 
                    -Country) %>% 
      rename(Country = Country.y) 
    
    # Subset static messages and clean 
    AIScsv_stat <- AIScsv %>%
      filter(Message_ID %in% c(5,24)) %>%
      mutate(Time = lubridate::ymd_hms(Time, tz="GMT"), 
             Dim_Length = as.numeric(Dimension_to_Bow)+as.numeric(Dimension_to_stern), 
             Dim_Width = as.numeric(Dimension_to_port)+as.numeric(Dimension_to_starboard), 
             MMSI = as.integer(MMSI)) %>%
      dplyr::select(MMSI,
                    Time, 
                    Vessel_Name,
                    IMO,
                    Ship_Type,
                    Dim_Length,
                    Dim_Width,
                    Draught,
                    Destination)
    
    AIScsv <- left_join(AIScsv_pos, AIScsv_stat, 
                        by = join_by(MMSI, closest(Time >= Time)), 
                        multiple="first") %>% 
      mutate(Time = Time.x) %>% 
      dplyr::select(-Time.y)
    rm(AIScsv_pos, AIScsv_stat)
  }
  
  metadata$orig_MMSIs <- length(unique(AIScsv$MMSI))
  metadata$orig_pts <- length(AIScsv$MMSI)
  
  runtimes$importtime <- (proc.time() - start)[[3]]/60

  # Remove invalid lat/long values
  AIScsvDF4 <- AIScsv %>%
    filter(!is.na(Latitude)) %>%
    filter(!is.na(Longitude)) 
  rm(AIScsv)
  
  metadata$invallatlon__mmsis <- length(unique(AIScsvDF4$MMSI))
  metadata$invallatlon__pts <- length(AIScsvDF4$MMSI)
  
  # print(paste("Inval lat/lon: ",yr, mnth))
  # Remove invalid MMSIs
  AIScsvDF3 <- AIScsvDF4 %>%
    dplyr::filter(nchar(trunc(abs(MMSI))) == 9)
  rm(AIScsvDF4)
  
  metadata$invalmmsi__mmsis <- length(unique(AIScsvDF3$MMSI))
  metadata$invalmmsi__pts <- length(AIScsvDF3$MMSI)
  
  # print(paste("Inval MMSI: ",yr, mnth))
  # Remove stationary aids to navigation
  AIScsvDF2 <- AIScsvDF3 %>%
    dplyr::filter(MMSI < 990000000) 
  rm(AIScsvDF3)
  
  metadata$aton_mmsis <- length(unique(AIScsvDF2$MMSI))
  metadata$aton_pts <- length(AIScsvDF2$MMSI)
  
  # Remove original MMSI and switch to scrambled version 
  AIScsvDF <- AIScsvDF2  %>% 
    left_join(scrambleids, by="MMSI") %>% 
    dplyr::select(-MMSI) %>% 
    mutate(AIS_ID = paste0(scramblemmsi,format(as.POSIXct(Time), "%Y%m%d"))) 
  rm(AIScsvDF2)
  
  # Identify and remove frost flowers 
  # (i.e. multiple messages from the same exact location sporadically transmitted throughout the day)
  ff <- AIScsvDF %>% 
    group_by(AIS_ID, Longitude, Latitude) %>% 
    summarize(n=n()) %>% filter(n > 2) %>% 
    mutate(tempid = paste0(AIS_ID, Longitude, Latitude))
  
  AISspeed4 <- AIScsvDF %>% 
    mutate(tempid = paste0(AIS_ID, Longitude, Latitude)) %>% 
    filter(!(tempid %in% ff$tempid))
  rm(ff, AIScsvDF)
  
  runtimes$dftime <- (proc.time() - start)[[3]]/60
  
  print(paste("Finished Cleaning ",yr, mnth))
  
  ##### Speed filter ------------------------------------------------------
  start <- proc.time()
  
  # Make dataframe into spatial object 
  AISspeed3 <- AISspeed4 %>%
    st_as_sf(coords=c("Longitude","Latitude"),crs=4326) %>%
    # project into Alaska Albers (or other CRS that doesn't create huge gap in mid-Bering with -180W and 180E)
    st_transform(crs=3338) %>%
    arrange(Time)
  rm(AISspeed4)
  
  # Calculate the euclidean speed between points
  # Also remove successive duplicate points (i.e., same time stamp and/or same location)
  AISspeed3[, c("x", "y")] <- st_coordinates(AISspeed3)
  AISspeed2 <- AISspeed3 %>% 
    st_drop_geometry() %>%
    group_by(AIS_ID) %>%
    arrange(AIS_ID, Time) %>% 
    mutate(timediff = as.numeric(difftime(Time,lag(Time),units=c("hours"))),
           distdiff = sqrt((y-lag(y))^2 + (x-lag(x))^2)/1000) %>% 
    filter(timediff > 0) %>% 
    filter(distdiff > 0) %>% 
    mutate(speed = distdiff/timediff)
  rm(AISspeed3)
  
  metadata$redund_aisids <- length(unique(AISspeed2$AIS_ID))
  metadata$redund_mmsi <- length(unique(AISspeed2$scramblemmsi))
  metadata$redund_pts <- length(AISspeed2$scramblemmsi)
  
  # Implement speed filter of 100 km/hr (also remove NA speed)
  AISspeed1 <- AISspeed2 %>% 
    dplyr::filter(speed < 100) %>% 
    dplyr::filter(!is.na(speed))
  rm(AISspeed2)
  
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
  
  AISspeed1$newseg <- ifelse(AISspeed1$timediff > 6, 1,
                             ifelse(AISspeed1$distdiff > 60, 1, 0))
  
  # Create new ID for each segment 
  AISspeed1$newseg[is.na(AISspeed1$newseg)] <- 1
  
  # Calculate whether points are occurring during daytime or at night 
  if(daynight==TRUE){
    print(paste("Spatial ",yr, mnth))
    temp <- st_coordinates(st_transform(AISspeed1, 4326))
    
    solarpos <- maptools::solarpos(temp, 
                                   dateTime=AISspeed1$Time, 
                                   POSIXct.out=TRUE)
    sunrise <- maptools::sunriset(temp, 
                                  dateTime=AISspeed1$Time, 
                                  direction="sunrise", 
                                  POSIXct.out=TRUE)
    sunset <- maptools::sunriset(temp, 
                                 dateTime=AISspeed1$Time, 
                                 direction="sunset", 
                                 POSIXct.out=TRUE)
    
    AISspeed1$solarpos <- solarpos[,2]
    AISspeed1$timeofday <- as.factor(ifelse(AISspeed1$solarpos > 0, "day","night"))
    
    AISspeed1$sunrise <- sunrise$time
    AISspeed1$sunset <- sunset$time
    AISspeed1$newseg[which(AISspeed1$timeofday != dplyr::lag(AISspeed1$timeofday))] <- 1
    temp <- AISspeed1 %>% 
      st_drop_geometry() %>% 
      dplyr::select(AIS_ID, newseg, timeofday) %>% 
      group_by(AIS_ID) %>% 
      mutate(newseg = cumsum(newseg))
    print(paste("Sunrise",yr, mnth))
  }
  if(daynight==FALSE){
    temp <- AISspeed1 %>% 
      dplyr::select(AIS_ID, newseg) %>% 
      group_by(AIS_ID) %>% 
      mutate(newseg = cumsum(newseg))
  }
  
  AISspeed1$newsegid <- stringi::stri_c(AISspeed1$AIS_ID,  temp$newseg, sep = "")
  
  # Remove segments with only one point (can't be made into lines)
  shortids <- AISspeed1 %>% 
    group_by(newsegid) %>% 
    summarize(n=n()) %>% 
    dplyr::filter(n < 2)
  AISspeed <- AISspeed1[!(AISspeed1$newsegid %in% shortids$newsegid),]
  rm(AISspeed1)
  
  metadata$short_aisids <- length(unique(AISspeed$AIS_ID))
  metadata$short_mmsi <- length(unique(AISspeed$scramblemmsi))
  metadata$short_pts <- length(AISspeed$scramblemmsi)
  
  runtimes$speedtime <- (proc.time() - start)[[3]]/60
  
  print(paste("Finished Speed filter ",yr, mnth))
  
  ##### Vectorization  ------------------------------------------------------
  
  start <- proc.time()
  
  # create sf lines by sorted / grouped points
  if(daynight==TRUE){
    AISsftemp <- AISspeed %>%
      st_as_sf(coords=c("x","y"),crs=3338) %>%
      arrange(Time) %>%
      # create 1 line per AIS ID
      group_by(newsegid) %>%
      # keep MMSI for lookup / just in case; do_union is necessary for some reason, otherwise it throws an error
      summarize(scramblemmsi=first(scramblemmsi), 
                Time_Of_Day=first(timeofday),
                Time_Start = as.character(first(Time)), 
                Time_End = as.character(last(Time)),
                AIS_ID=first(AIS_ID), 
                SOG_Median=median(SOG, na.rm=T), 
                SOG_Mean=mean(SOG, na.rm=T), 
                Ship_Type = first(Ship_Type, na_rm = T), 
                Country = first(Country, na_rm = T),
                Dim_Length = first(Dim_Length, na_rm = T), 
                Dim_Width = first(Dim_Width, na_rm = T), 
                Draught = first(Draught, na_rm = T), 
                Destination = first(Destination, na_rm = T),
                npoints=n(), 
                do_union=FALSE)
    AISsftempnew <- AISsftemp[AISsftemp$npoints > 1,]
    
    metadata$daynightshort_aisids <- length(unique(AISsftempnew$AIS_ID))
    metadata$daynightshort_mmsi <- length(unique(AISsftempnew$scramblemmsi))
    
    AISsf1 <- AISsftempnew %>% 
      st_cast("LINESTRING") %>% 
      st_make_valid() %>% 
      ungroup()
  }
  if(daynight==FALSE){
    AISsf1 <- AISspeed %>%
      st_as_sf(coords=c("x","y"),crs=3338) %>%
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
                Ship_Type = first(Ship_Type, na_rm = T), 
                Country = first(Country, na_rm = T),
                Dim_Length = first(Dim_Length, na_rm = T), 
                Dim_Width = first(Dim_Width, na_rm = T), 
                Draught = first(Draught, na_rm = T), 
                Destination = first(Destination, na_rm = T),
                npoints=n(), 
                do_union=FALSE) %>% 
      st_cast("LINESTRING") %>% 
      st_make_valid() %>% 
      ungroup()
  }
  rm(AISspeed)
  
  # Add in destination codes 
  AISsf <- AISsf1 %>% left_join(., dest, by=c("Destination" = "Destntn"))
  rm(AISsf1)
  
  # Calculate total distance travelled
  AISsf$Length_Km <- as.numeric(st_length(AISsf)/1000)
  
  ##### Ship type ------------------------------------------------------
  start <- proc.time()
  
  # Split lines by ship type
  # link to ship type/numbers table: 
  # https://help.marinetraffic.com/hc/en-us/articles/205579997-What-is-the-significance-of-the-AIS-Shiptype-number-
  AISjoined <- AISsf %>%
    mutate(AIS_Type = case_when(
      is.na(AISsf$Ship_Type) ~ "Other",
      substr(AISsf$Ship_Type,1,2)==30 ~ "Fishing",
      substr(AISsf$Ship_Type,1,2)%in% c(31, 32, 52) ~ "TugTow",
      substr(AISsf$Ship_Type,1,1)==6 ~ "Passenger",
      substr(AISsf$Ship_Type,1,2)==36 ~ "Sailing",
      substr(AISsf$Ship_Type,1,2)==37 ~ "Pleasure",
      substr(AISsf$Ship_Type,1,1)==7 ~ "Cargo",
      substr(AISsf$Ship_Type,1,1)==8 ~ "Tanker",
      # from trial and error, I think that this last "TRUE" serves as a catch-all, but I can't logically figure out why it works. -\__(%)__/-
      TRUE ~ "Other"
    ))  
  rm(AISsf)
  
  nships <- AISjoined %>% 
    st_drop_geometry() %>% 
    group_by(AIS_Type) %>% 
    summarize(n=length(unique(scramblemmsi)))
  
  metadata$ntank_mmsis <- nships$n[nships$AIS_Type == "Tanker"]
  metadata$ntug_mmsis <- nships$n[nships$AIS_Type == "TugTow"]
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
  
  ##### Save outputs ------------------------------------------------------
  
  start <- proc.time()
  
  # Loop through each ship type, rasterize, and save shp file as well 
  allTypes <- unique(AISjoined$AIS_Type)
  
  for (k in 1:length(allTypes)){
    
    AISfilteredType <- AISjoined %>%
      filter(AIS_Type==allTypes[k])
    
    # Save data in vector format
    if(length(AISfilteredType$newsegid) > 0){
      write_sf(AISfilteredType,
               paste0("../Data_Processed/Vector/Tracks_DayNight", 
                      daynight, "_",MoName,"-",allTypes[k],".shp"), 
               overwrite=T)
    }
  }
  
  # Save processing info to text file 
  runtimes$vectortime <- (proc.time() - start)[[3]]/60
  
  runtime <- proc.time() - starttime 
  runtimes$runtime_min <- runtime[[3]]/60 
  
  write.csv(metadata, paste0("../Data_Processed/Metadata/Metadata_DayNight", 
                             daynight, "_", MoName,".csv"))
  
  write.csv(runtimes, paste0("../Data_Processed/Metadata/Runtimes_DayNight", 
                             daynight, "_",MoName,".csv"))
  print(runtimes)
  
# })
  return(runtimes)
}