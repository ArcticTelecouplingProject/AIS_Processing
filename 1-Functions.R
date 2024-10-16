clean_and_transform <- function(csvList, flags, scrambleids, dest, daynight, output, hexgrid=NA){
  
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
  # rm(temp)
  
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
      rename(Time = Time.x) %>% 
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
  # rm(AISspeed2)
  
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
  
  AISspeed1$cut_line <- ifelse(AISspeed1$timediff > 6, TRUE,
                             ifelse(AISspeed1$distdiff > 60, TRUE, FALSE))
  
  # Indicate new segment for first point in each line  
  AISspeed1$cut_line[is.na(AISspeed1$cut_line)] <- TRUE
  
  # Identify vessed that are "stopped" (traveling <2 knots) for > 1 hour
  test <- AISspeed1 %>%
    arrange(Time) %>%
    group_by(scramblemmsi) %>%
    mutate(
      # Step 1: Create a logical vector for each ship if SOG is below 2 knots
      below_threshold = SOG < 2,
      
      # Step 2: Calculate the time difference between consecutive points in hours
      time_diff = as.numeric(difftime(Time, lag(Time, default = first(Time)), units = "hours")),
      
      # Step 3: Set time_diff to 0 where speed is >= 2 knots
      time_diff_below_threshold = ifelse(below_threshold, time_diff, 0),
      
      # Step 4: Calculate cumulative time spent below the speed threshold within each stop period
      cum_time_below_threshold = ifelse(below_threshold, ave(time_diff_below_threshold, cumsum(!below_threshold), FUN = cumsum), 0),
      
      # Step 5: Mark points as "stopped" if the vessel has been below 2 knots for at least 1 hour
      below_1hr = cum_time_below_threshold >= 1,  # Logical vector marking points after 1 hour below threshold
      
      # Step 6: Identify the start of the below-threshold period
      # Create a flag for the start of below-threshold segments
      start_below_threshold = below_threshold & lag(below_threshold, default = FALSE) == FALSE,
      
      # Step 7: Create a grouping identifier for each below-threshold period
      below_threshold_group = cumsum(start_below_threshold),
      
      # Step 8: Update stopped_sog: set to 1 for all points in a group where any point reaches 1 hour below threshold
      stopped_sog = ifelse(below_threshold_group %in% below_threshold_group[below_1hr], 1, 0),
      
      # Step 9: Reset stopped_sog to 0 when the vessel speeds up above 2 knots
      stopped_sog = ifelse(SOG >= 2, 0, stopped_sog),
      
      # Step 10: Create a flag for when stopped_sog changes
      status_change = ifelse(stopped_sog != lag(stopped_sog, default = first(stopped_sog)), TRUE, FALSE)
      
    ) %>%
    ungroup() %>%
    select(-below_threshold, -time_diff, -time_diff_below_threshold, -cum_time_below_threshold, -below_1hr, -start_below_threshold, -below_threshold_group) %>%
    arrange(scramblemmsi, Time)
  
  
  # Calculate whether points are occurring during daytime or at night 
  if(daynight==TRUE){
    print(paste("Spatial ",yr, mnth))
    
    temp <- test %>%   
      st_as_sf(coords=c("x","y"),crs=3338) %>%
      st_transform(4326) %>% 
      st_coordinates()
    
    solarpos <- maptools::solarpos(temp, 
                                   dateTime=test$Time, 
                                   POSIXct.out=TRUE)

    test$solarpos <- solarpos[,2]
    test$timeofday <- as.factor(ifelse(test$solarpos > -6, "day","night"))
    
    test <- test %>% 
      group_by(scramblemmsi) %>% 
      mutate(status_change = ifelse(timeofday != lag(timeofday, default = first(timeofday)), 
                                             TRUE, status_change)) %>% 
      ungroup()
    
    # test$status_change[which(test$timeofday != dplyr::lag(test$timeofday))] <- TRUE
    
    print(paste("Sunrise",yr, mnth))
  }
  
  test <- test %>% 
    group_by(scramblemmsi) %>% 
    mutate(newline = cumsum(cut_line), 
           newseg = cumsum(status_change) + 1) %>% 
           # newseg = ifelse(is.na(newseg), lag(newseg), newseg)) %>% 
    # filter(!is.na(newseg)) %>% 
           # Create a group ID for each set of consecutive points based on scramblemmsi and status_change
    mutate(newsegid = stringi::stri_c(AIS_ID, newline, newseg, sep = "-"))
  
  # Create duplicate points at end of each segment (duplicate to the beginning of the next segment) 
  # So that track lines are continuous even if segments are separated. 
  test <- test %>%
    arrange(scramblemmsi, Time) %>% 
    group_by(scramblemmsi) %>%
    do({
      segment <- .
      expanded_segment <- segment
      
      if(length(unique(segment$newseg)) > 1){
        
        for (i in 2:length(unique(segment$newseg))) {
          
          seg <- segment[segment$newseg == i,]
          
          # Only connect segments if the reason for cutting them wasn't time or distance gaps
          if(seg$cut_line[1] == FALSE){
            
            new_segment_id <- i - 1
  
            # Duplicate the last point and modify its newseg
            first_point <- seg[1,]
            first_point$status_change <- FALSE
            first_point$newseg <- new_segment_id
            
            # Create a new row with the duplicated point and new segment_id
            expanded_segment <- rbind(expanded_segment, first_point)
          }
        }
      }
      expanded_segment
    }) %>%
    ungroup() %>% 
    # update newsegid for new points joining onto the lines 
    mutate(newsegid = stringi::stri_c(AIS_ID, newline, newseg, sep = "-")) %>% 
    arrange(scramblemmsi, Time, newseg)
  
  
  # Remove segments with only one point (can't be made into lines)
  shortids <- test %>%
    group_by(newsegid) %>%
    summarize(n=n()) %>%
    dplyr::filter(n < 2)

  AISspeed <- test[!(test$newsegid %in% shortids$newsegid),]
  rm(AISspeed1)

  metadata$short_aisids <- length(unique(AISspeed$AIS_ID))
  metadata$short_mmsi <- length(unique(AISspeed$scramblemmsi))
  metadata$short_pts <- length(AISspeed$scramblemmsi)
  
  runtimes$speedtime <- (proc.time() - start)[[3]]/60
  
  print(paste("Finished Speed filter ",yr, mnth))
  
  
  ################################################################################
  ##### Vectorize  ------------------------------------------------------
  if(output == "vector"){
    start <- proc.time()
    
    # create sf lines by sorted / grouped points
    if(daynight==TRUE){
      
      AISsftemp <- AISspeed %>%
        st_as_sf(coords=c("x","y"),crs=3338) %>%
        # create 1 line per AIS ID
        group_by(newsegid) %>%
        # group_by(newsegid, stopped_navstat, stopped_sog) %>%
        arrange(Time) %>% 
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
                  stopped_sog = first(stopped_sog, na_rm = T), 
                  npoints=n(), 
                  do_union=FALSE, 
                  geometry = st_combine(geometry)) %>% 
        mutate(geometry = st_cast(geometry, "LINESTRING")) %>% 
        ungroup() %>% 
        st_make_valid()
      
      AISsf1 <- AISsftemp[AISsftemp$npoints > 1,]
      
      metadata$daynightshort_aisids <- length(unique(AISsf1$AIS_ID))
      metadata$daynightshort_mmsi <- length(unique(AISsf1$scramblemmsi))
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
                  stopped_sog = first(stopped_sog, na_rm = T), 
                  stopped_navstat = first(stopped_navstat, na_rm = T),
                  npoints=n(), 
                  geometry = st_combine(geometry)) %>% 
        mutate(geometry = st_cast(geometry, "LINESTRING")) %>% 
        ungroup()
    }
    # rm(AISspeed)
    
    # Add in destination codes 
    AISsf <- AISsf1 %>% left_join(., dest, by=c("Destination" = "Destntn"))
    # rm(AISsf1)
    
    # Calculate total distance travelled
    AISsf$Length_Km <- as.numeric(st_length(AISsf)/1000)
    
    # Remove any non-linestring types if still remaining after st_make_valid
    AISsf <- st_collection_extract(AISsf, "LINESTRING")
    
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
    # rm(AISsf)
    
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
        st_write(AISfilteredType,
                 paste0("../Data_Processed/Vector/Tracks_DayNight", 
                        daynight, "_",MoName,"-",allTypes[k],".shp"),
                 append = F)
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
  
  ################################################################################
  ##### Hex  ------------------------------------------------------
  if(output == "hex"){
    # HEX CODE HERE
    # Calculate number of ships with 0 values in dimensions and convert to NA
    metadata$nolength <- length(which(is.na(AISspeed$Dim_Length)))
    zerolength <- which(AISspeed$Dim_Length == 0)
    metadata$pctzerolength <- round((metadata$nolength + length(zerolength))/length(AISspeed$Dim_Length)*100,2)
    
    metadata$nowidth <- length(which(is.na(AISspeed$Dim_Width)))
    zerowidth <- which(AISspeed$Dim_Width == 0)
    metadata$pctzerowidth <- round((metadata$nowidth + length(zerowidth))/length(AISspeed$Dim_Width)*100,2)
    
    # Remove zero value rows for consideration of respective measurement
    # (i.e. if either bow or stern is zero, then both get NA 
    # and if either port or starboard is zero then both get NA)
    AISspeed$Dim_Length[zerolength] <- NA
    AISspeed$Dim_Width[zerowidth] <- NA
    
    ##### Ship type ------------------------------------------------------
    start <- proc.time()
    
    # Make points spatial, ID ship type, and join to hex grid 
    # link to ship type/numbers table: 
    # https://help.marinetraffic.com/hc/en-us/articles/205579997-What-is-the-significance-of-the-AIS-Shiptype-number-
    AISjoined <- AISspeed %>%
      st_as_sf(coords=c("x","y"),crs=3338) %>% 
      st_join(hexgrid["hexID"]) %>% 
      mutate(AIS_Type = case_when(
        is.na(AISspeed$Ship_Type) ~ "Other",
        substr(AISspeed$Ship_Type,1,2)==30 ~ "Fishing",
        substr(AISspeed$Ship_Type,1,2)%in% c(31, 32, 52) ~ "TugTow",
        substr(AISspeed$Ship_Type,1,1)==6 ~ "Passenger",
        substr(AISspeed$Ship_Type,1,2)==36 ~ "Sailing",
        substr(AISspeed$Ship_Type,1,2)==37 ~ "Pleasure",
        substr(AISspeed$Ship_Type,1,1)==7 ~ "Cargo",
        substr(AISspeed$Ship_Type,1,1)==8 ~ "Tanker",
        TRUE ~ "Other"
      ))  
    # rm(AISspeed)
    
    nships <- AISjoined %>% 
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

    
    runtimes$jointime <- (proc.time() - start)[[3]]/60
    

    
    # Thrown out for missing/incorrect lat/lon/MMSI, duplicate points, speed > 100 km/hr, outisde hex grid
    metadata$pctmissingwidth <- round(sum(is.na(AISjoined$Dim_Width))/length(AISjoined$Dim_Width)*100, 2)
    metadata$ pctmissinglength <- round(sum(is.na(AISjoined$Dim_Length))/length(AISjoined$Dim_Length)*100, 2)
    metadata$pctmissingSOG <-  round(sum(is.na(AISjoined$SOG))/length(AISjoined$Dim_Length)*100, 2)
    
    # # Loop through each ship type and calculate summary statistics
    allTypes <- unique(AISjoined$AIS_Type)
    
    
    # Calculate summary stats for each ship type
    for (k in 1:length(allTypes)){
      # Select ship type
      AISfilteredType <- AISjoined %>%
        filter(AIS_Type==allTypes[k])
      
      if(daynight == TRUE){
        
        # Calculate total number of hours of daytime and nighttime transmissions 
        # for each unique ship in each day in each hex
        # This method accounts for high latitude days where days may extend past midnight. 
        AIShours <- AISfilteredType %>% 
          st_drop_geometry() %>% 
          arrange(Time) %>% # arrange by time 
          # Create new sequence of row numbers for each unique day/ship/hex/timeofday combo
          group_by(AIS_ID, hexID, scramblemmsi,timeofday) %>% 
          mutate(rownum = 1:n()) %>%
          ungroup()
        # ID first signal in each of the above ID'd sequences
        AIShours <- AIShours %>% mutate(daychange=ifelse(rownum == 1, 1, 0))
        
        AIShoursums <- AIShours %>% 
          # Remove these signals because there was at least one change in day/ship/hex/timeofday
          # between the signals and so we don't wnat to use the time diff value in our total 
          # time calculation below. 
          filter(daychange != 1) %>% 
          group_by(AIS_ID, hexID, scramblemmsi, timeofday) %>% 
          # Calculate the total amount of time in each unique day/ship/hex/timeofday state 
          summarize(Hrs = sum(timediff)) 
        
        nightHrs <- AIShoursums %>% 
          filter(timeofday == "night") %>% 
          group_by(hexID) %>% 
          summarize(N_Hrs = round(sum(Hrs, na.rm=T), 2),
                    N_nShp=length(unique(scramblemmsi)),
                    N_OpD=length(unique(AIS_ID)))
        
        dayHrs <- AIShoursums %>% 
          filter(timeofday == "day") %>% 
          group_by(hexID) %>% 
          summarize(D_Hrs = round(sum(Hrs, na.rm=T), 2),
                    D_nShp=length(unique(scramblemmsi)),
                    D_OpD=length(unique(AIS_ID)))
        

        joinOut <- left_join(dayHrs, nightHrs, by=c("hexID"))
        joinOut <- joinOut %>% 
          mutate_at(c(1:ncol(joinOut)), ~replace_na(.,0))
      }
        
        # Hrs will not be the sum of day and night hrs because it includes 
        # the transition intervals (e.g., between day and night)
        joinOutNew <- AISfilteredType %>%
          st_drop_geometry() %>% 
          group_by(hexID) %>%
          summarize(Hrs=round(sum(timediff, na.rm=T), 2),
                    nShp=length(unique(scramblemmsi)),
                    OpD=length(unique(AIS_ID)))
        if(daynight == T){
          joinOutNew <- left_join(joinOutNew, joinOut)
        }
      
      colnames(joinOutNew)[2:ncol(joinOutNew)] <- paste0(colnames(joinOutNew)[2:ncol(joinOutNew)],"_",substring(allTypes[k],1,2))
      
      hexgrid <- left_join(hexgrid, joinOutNew, by="hexID")
    }
    
    if(daynight == TRUE){
      # Calculate summary stats for all ship types in aggregate
      # Calculate average speed within hex grid 
      # Calculate total number of hours of daytime and nighttime transmissions 
      # for each unique ship in each day in each hex
      # This method accounts for high latitude days where days may extend past midnight. 
      TotAIShours <- AISjoined %>% 
        st_drop_geometry() %>% 
        arrange(Time) %>% # arrange by time 
        # Create new sequence of row numbers for each unique day/ship/hex/timeofday combo
        group_by(AIS_ID, hexID, scramblemmsi,timeofday) %>% 
        mutate(rownum = 1:n()) %>%
        ungroup()
      # ID first signal in each of the above ID'd sequences
      TotAIShours <- TotAIShours %>% mutate(daychange=ifelse(rownum == 1, 1, 0))
      
      TotAIShoursums <- TotAIShours %>% 
        # Remove these signals because there was at least one change in day/ship/hex/timeofday
        # between the signals and so we don't wnat to use the time diff value in our total 
        # time calculation below. 
        filter(daychange != 1) %>% 
        group_by(AIS_ID, hexID, scramblemmsi, timeofday) %>% 
        # Calculate the total amount of time in each unique day/ship/hex/timeofday state 
        summarize(Hrs = sum(timediff)) 
      
      TotnightHrs <- TotAIShoursums %>% 
        filter(timeofday == "night") %>% 
        group_by(hexID) %>% 
        summarize(N_Hrs = round(sum(Hrs, na.rm=T), 2),
                  N_nShp=length(unique(scramblemmsi)),
                  N_OpD=length(unique(AIS_ID)))
      
      TotdayHrs <- TotAIShoursums %>% 
        filter(timeofday == "day") %>% 
        group_by(hexID) %>% 
        summarize(D_Hrs = round(sum(Hrs, na.rm=T), 2),
                  D_nShp=length(unique(scramblemmsi)),
                  D_OpD=length(unique(AIS_ID)))
    }
    TotalHrs <- AISjoined %>%
      st_drop_geometry() %>% 
      group_by(hexID) %>%
      summarize(Hrs=round(sum(timediff, na.rm=T), 2),
                nShp=length(unique(scramblemmsi)),
                OpD=length(unique(AIS_ID)))
    if(daynight == TRUE){
      allShipsNew <- left_join(TotalHrs, TotnightHrs, by=c("hexID"))
      allShipsNew <- left_join(allShipsNew, TotdayHrs, by=c("hexID"))
    }else(allShipsNew <- TotalHrs)
    
    colnames(allShipsNew)[2:ncol(allShipsNew)] <- paste0(colnames(allShipsNew)[2:ncol(allShipsNew)],"_Al")
    
    hexgrid <- left_join(hexgrid, allShipsNew, by="hexID")
    
    hexgrid$year <- yr
    hexgrid$month <- mnth
    
    print(paste("Finished hex calcs ",yr, mnth))
    # hexpts <- st_as_sf(AISjoined, coords = c("x", "y"), crs = 3338)
    
    # Save data in vector format
    write_sf(hexgrid, paste0("../Data_Processed/Hex/Hex_",MoName,"_DayNight",daynight,".shp"))
    # write_sf(AISjoined, paste0("../Data_Processed_TEST/Hex/SpeedPts_",MoName,"_",ndays,".shp"))
    # write_sf(hexpts, paste0("../Data_Processed_TEST/Hex/SpeedPts_",MoName,".shp"))
    
    
    # Save processing info to text file 
    runtimes$hextime <- (proc.time() - start)[[3]]/60
    
    runtimes$runtime <- (proc.time() - starttime)[[3]]
    runtimes$runtime_min <- runtimes$runtime/60 
    
    write.csv(metadata, paste0("../Data_Processed/Hex/HexMetadata_",MoName,"_DayNight",daynight,".csv"))
    
    
    write.csv(runtimes, paste0("../Data_Processed/Hex/HexRuntimes_",MoName,"_DayNight",daynight,".csv"))
    # write.csv(runtimes, paste0("../Data_Processed_TEST/Hex/Runtimes_SpeedHex_",MoName,"_",ndays,".csv"))
    # print(runtimes)
    # return(runtimes)
  }
  }


