
#' Remove invalid latitude and longitude values
#'
#' @param df Data frame with Latitude and Longitude columns
#'
#' @return Filtered data frame without NA values in Latitude and Longitude columns
clean_lat_lon <- function(df) {
  df %>% filter(!is.na(Latitude) & !is.na(Longitude))
}

#' Remove invalid MMSIs
#'
#' @param df Data frame with MMSI column
#'
#' @return Filtered data frame with valid MMSIs (9-digit numeric values)
clean_mmsi <- function(df) {
  df %>% filter(nchar(trunc(abs(MMSI))) == 9)
}

#' Remove stationary aids to navigation
#'
#' @param df Data frame with MMSI column
#'
#' @return Filtered data frame without stationary aids to navigation
remove_navigation_aids <- function(df) {
  df %>% filter(MMSI < 990000000)
}

#' Add scrambled AIS IDs and remove original MMSI
#'
#' @param df Data frame with MMSI column
#' @param scrambleids Data frame with scrambled MMSI information
#'
#' @return Data frame with scrambled AIS_ID column and original MMSI removed
add_scrambled_ids <- function(df, scrambleids) {
  df %>% 
    left_join(scrambleids, by = "MMSI") %>% 
    select(-MMSI) %>% 
    mutate(AIS_ID = paste0(scramblemmsi, format(as.POSIXct(Time), "%Y%m%d")))
}

#' Remove frost flowers (multiple messages from the same exact location)
#'
#' @param df Data frame with AIS_ID, Longitude, and Latitude columns
#'
#' @return Filtered data frame without frost flowers
remove_frost_flowers <- function(df) {
  ff <- df %>% 
    group_by(AIS_ID, Longitude, Latitude) %>% 
    summarize(n = n()) %>% 
    filter(n > 2) %>% 
    mutate(tempid = paste0(AIS_ID, Longitude, Latitude))
  
  df %>% 
    mutate(tempid = paste0(AIS_ID, Longitude, Latitude)) %>% 
    filter(!(tempid %in% ff$tempid)) %>% 
    select(-tempid)
}

#' Make data frame into spatial object and transform coordinates
#'
#' @param df Data frame with Longitude and Latitude columns
#' @param crs Coordinate Reference System (CRS) to transform to (default: 3338)
#'
#' @return Spatial data frame with transformed coordinates
transform_to_spatial <- function(df, crs = 3338) {
  df %>%
    st_as_sf(coords = c("Longitude", "Latitude"), crs = 4326) %>%
    st_transform(crs = crs) %>%
    arrange(Time)
}

#' Calculate speed and remove duplicate or invalid points
#'
#' @param df Spatial data frame with AIS_ID and time information
#'
#' @return Data frame with calculated speed and filtered duplicate or invalid points
calculate_speed <- function(df) {
  if(!("x" %in% colnames(df) & "y" %in% colnames(df))){
    df[, c("x", "y")] <- st_coordinates(df)
  }
  dfnew <- df %>% 
    st_drop_geometry() %>%
    group_by(scramblemmsi) %>%
    arrange(scramblemmsi, Time) %>% 
    mutate(timediff = as.numeric(difftime(Time, lag(Time), units = "hours")),
           distdiff = sqrt((y - lag(y))^2 + (x - lag(x))^2) / 1000) %>% 
    filter(timediff > 0) %>%
    filter(distdiff > 0) %>%
    mutate(speed = distdiff / timediff) %>% 
    ungroup()
  
  return(dfnew)
  }

#' Identify stopped vessels
#'
#' @param df Data frame with vessel information, including SOG and Time
#'
#' @return Data frame with information on vessels that are "stopped" (traveling <3 knots for >1 hour)
identify_stopped_vessels <- function(df, speed_threshold, time_threshold) {
  df %>%
    arrange(Time) %>%
    filter(!is.na(SOG)) %>% 
    group_by(scramblemmsi) %>%
    mutate(
      # Step 1: Create a logical vector for each ship if SOG is below 2 knots
      below_threshold = SOG < speed_threshold,
      
      # Step 2: Calculate the time difference between consecutive points in hours
      time_diff = as.numeric(difftime(Time, lag(Time, default = first(Time)), units = "hours")),
      
      # Step 3: Set time_diff to 0 where speed is >= 2 knots
      time_diff_below_threshold = ifelse(below_threshold, time_diff, 0),
      
      # Step 4: Calculate cumulative time spent below the speed threshold within each stop period
      cum_time_below_threshold = ifelse(below_threshold, ave(time_diff_below_threshold, cumsum(!below_threshold), FUN = cumsum), 0),
      
      # Step 5: Mark points as "stopped" 
      below_hr_threshold = cum_time_below_threshold >= time_threshold,  # Logical vector marking points after 1 hour below threshold
      
      # Step 6: Identify the start of the below-threshold period
      # Create a flag for the start of below-threshold segments
      start_below_threshold = below_threshold & lag(below_threshold, default = FALSE) == FALSE,
      
      # Step 7: Create a grouping identifier for each below-threshold period
      below_threshold_group = cumsum(start_below_threshold),
      
      # Step 8: Update stopped_sog: set to 1 for all points in a group where any point reaches 1 hour below threshold
      stopped_sog = ifelse(below_threshold_group %in% below_threshold_group[below_hr_threshold], 1, 0),
      
      # Step 9: Reset stopped_sog to 0 when the vessel speeds up above speed_threshold
      stopped_sog = ifelse(SOG >= speed_threshold, 0, stopped_sog),
      
      # Step 10: Create a flag for when stopped_sog changes
      status_change = ifelse(stopped_sog != lag(stopped_sog, default = first(stopped_sog)), TRUE, FALSE)
    ) %>%
    ungroup() %>%
    select(-below_threshold, -time_diff, -time_diff_below_threshold, -cum_time_below_threshold, -below_hr_threshold, -start_below_threshold, -below_threshold_group) %>%
    arrange(scramblemmsi, Time)
}

#' Handle day-night transitions and calculate new segments
#'
#' @param df Data frame containing spatial data and time information
#'
#' @return Data frame with updated segment information based on day-night transitions
handle_daynight_transitions <- function(df) {
  temp <- df %>%
    st_as_sf(coords = c("x", "y"), crs = 3338) %>%
    st_transform(4326) %>%
    st_coordinates()
  
  solarpos <- maptools::solarpos(temp, dateTime = df$Time, POSIXct.out = TRUE)
  df$solarpos <- solarpos[, 2]
  df$timeofday <- as.factor(ifelse(df$solarpos > -6, "day", "night"))
  
  df %>%
    group_by(scramblemmsi) %>%
    mutate(status_change = ifelse(timeofday != lag(timeofday, default = first(timeofday)), TRUE, status_change)) %>%
    ungroup()
}

#' Create segments for AIS data
#'
#' @param df Data frame containing AIS data
#'
#' @return Data frame with updated segment information for each vessel
create_segments <- function(df) {
  df %>%
    group_by(scramblemmsi) %>% 
    mutate(newline = cumsum(cut_line), 
           newseg = cumsum(status_change) + 1) %>% 
    mutate(newsegid = stringi::stri_c(scramblemmsi, newline, newseg, sep = "-"))
}

#' Expand segments to connect track lines
#'
#' @param df Data frame containing AIS segments
#'
#' @return Data frame with expanded segments for continuous track lines
expand_segments <- function(df) {
  # temp <- t %>%
  df %>%
    arrange(scramblemmsi, Time) %>%
    group_by(scramblemmsi) %>%
    do({
      segment <- .
      expanded_segment <- segment
      
      if (length(unique(segment$newseg)) > 1) {
        for (i in 2:length(unique(segment$newseg))) {
          seg <- segment[segment$newseg == i, ]
          
          # If it's not a new line bc of time/distance thresholds 
          if (seg$cut_line[1] == FALSE) {
            # If the ship is starting from a stop 
            if(seg$stopped_sog[1] ==  0 & last(segment$stopped_sog[segment$newseg == (i-1)]) == 1){  
              # Then attach the last point of being stopped to the first point of the moving segment
              last_point <- last(segment[segment$newseg == (i-1),])
              last_point$status_change <- FALSE
              last_point$newseg <- i
              last_point[1, setdiff(names(last_point), 
                                    c("scramblemmsi", "Time", "AIS_Type", "Ship_Type", "SOG", "status_change", "cut_line", "newseg", "newline", "x", "y", "timeofday"))] <- NA
              expanded_segment <- rbind(expanded_segment, last_point)
              }else{
                # Otherwise, attach the first point of the next segment as the last point of the current segment
                new_segment_id <- (i - 1)
                first_point <- seg[1, ]
                first_point$status_change <- FALSE
                first_point$newseg <- new_segment_id
                expanded_segment <- rbind(expanded_segment, first_point) 
              }
          }
        }
      }
      expanded_segment
    }) %>%
    ungroup() %>%
    mutate(newsegid = stringi::stri_c(scramblemmsi, newline, newseg, sep = "-")) %>%
    arrange(scramblemmsi, Time, newseg)
}

#' Remove short segments with less than 2 points
#'
#' @param df Data frame containing AIS segments
#'
#' @return Data frame with short segments removed
remove_short_segments <- function(df) {
  shortids <- df %>%
    group_by(newsegid) %>%
    summarize(n = n()) %>%
    filter(n < 2)
  
  df[!(df$newsegid %in% shortids$newsegid), ]
}



#' Identify ship types from code 
#' link to ship type/numbers table: 
#' https://api.vtexplorer.com/docs/ref-aistypes.html
#'
#' @param df Data frame containing AIS segments
#'
#' @return Data frame with AIS_type added 
id_ship_type <- function(df){
  types <- data.frame(
    Ship_Type = c(30, 31, 32, 35, 52, 36, 37, 60:69, 70:79, 80:89, NA),
    AIS_Type = c("Fishing", "TugTow", "TugTow", "Military", "TugTow", rep("Recreation", 12), 
                 rep("Cargo", 10), rep("Tanker", 10), "Unknown")
  ) 
  
  # Determine ship type 
  scrmb_types <- df %>%
    filter(!is.na(Ship_Type)) %>%
    group_by(scramblemmsi, Ship_Type) %>%
    summarize(count = n(), .groups = "drop") %>%
    group_by(scramblemmsi) %>%
    filter(count == max(count)) %>%
    summarize(
      Ship_Type = if (n() == 1) Ship_Type else NA_character_,
      .groups = "drop"
    )
  
  dfnew <- df %>% 
    select(-Ship_Type) %>% 
    left_join(scrmb_types, by = "scramblemmsi") %>% 
    mutate(Ship_Type = as.numeric(Ship_Type)) %>% 
    left_join(types, by = "Ship_Type") 
  
  dfnew$AIS_Type[dfnew$Ship_Type <100 & is.na(dfnew$AIS_Type)] <- "Other"
  dfnew$AIS_Type[is.na(dfnew$AIS_Type)] <- "Unknown" 
  dfnew$AIS_Type[dfnew$AIS_Type == 0] <- "Unknown"  
  
  return(dfnew)
}

