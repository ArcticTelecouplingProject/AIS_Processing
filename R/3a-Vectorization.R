#' Vectorize Ship Segments
#'
#' This function converts AIS data into spatial segments for each ship, represented as LINESTRING geometries.
#'
#' @param df A data frame containing ship data, with coordinates and other ship information.
#' @param dest A data frame containing destination information.
#'
#' @return A spatial data frame (sf) containing LINESTRING geometries for each ship segment.
#' 
#' @examples
#' df <- data.frame(x = c(1, 2), y = c(3, 4), Time = c("2023-01-01", "2023-01-02"), newsegid = c(1, 1))
#' dest <- data.frame(Destntn = "Some Port")
#' segments <- vectorize_segments(df, dest)
vectorize_segments <- function(df, dest){
  
  # Common base process
  AISsf1 <- df %>%
    st_as_sf(coords = c("x", "y"), crs = 3338) %>%
    arrange(Time) %>%
    # Create 1 line per AIS ID
    group_by(newsegid) %>%
    # Summarize with common columns
    summarize(scramblemmsi = first(scramblemmsi),
              Time_Start = as.character(first(Time)),
              Time_End = as.character(last(Time)),
              Time_Of_Day = case_when(
                daynight ~ first(timeofday, na_rm = TRUE),
                TRUE ~ NA_character_
              ),
              AIS_ID = first(AIS_ID),
              SOG_Median = median(SOG, na.rm = TRUE),
              SOG_Mean = mean(SOG, na.rm = TRUE),
              Ship_Type = first(Ship_Type, na_rm = TRUE),
              AIS_Type = first(AIS_Type, na_rm = TRUE),
              Country = first(Country, na_rm = TRUE),
              Dim_Length = first(Dim_Length, na_rm = TRUE),
              Dim_Width = first(Dim_Width, na_rm = TRUE),
              Draught = first(Draught, na_rm = TRUE),
              Destination = first(Destination, na_rm = TRUE),
              stopped_sog = first(stopped_sog, na_rm = TRUE),
              npoints = n(),
              geometry = st_combine(geometry)) %>%
    mutate(
      Time_Of_Day =  factor(Time_Of_Day, levels = c("day", "night")),
      geometry = st_cast(geometry, "LINESTRING")) %>%
    ungroup() %>% 
    st_make_valid() %>%
    filter(npoints > 1)
  
  # Apply st_make_valid only if daynight == TRUE and filter npoints > 1
  if (daynight) {
    metadata$daynightshort_aisids <- length(unique(AISsf1$AIS_ID))
    metadata$daynightshort_mmsi <- length(unique(AISsf1$scramblemmsi))
  }
  
  if(!daynight){
    AISsf1 <- AISsf1 %>% dplyr::select(-Time_Of_Day)
  }
  
  AISsf <- AISsf1 %>% 
    
    # Add in destination codes 
    left_join(., dest, by=c("Destination" = "Destntn")) %>% 
    
    # Calculate total distance travelled
    mutate(Length_Km = as.numeric(st_length(.)/1000)) %>% 
    
    # Remove any non-linestring types if still remaining after st_make_valid
    st_collection_extract("LINESTRING")
}

#' Save Ship Segments
#'
#' This function saves the spatial ship segments as shapefiles for each unique ship type.
#'
#' @param AISsf A spatial data frame (sf) containing ship segment data.
#' @param MoName A string representing the month name.
#' @param daynight A logical value indicating if the segments represent day or night.
#'
#' @return None. Saves shapefiles to the specified directory.
#' 
#' @examples
#' save_segments(AISsf, "January", TRUE)
save_segments <- function(AISsf, MoName, daynight){
  
  # Loop through each ship type and save shp file 
  allTypes <- unique(AISsf$AIS_Type)
  
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
}