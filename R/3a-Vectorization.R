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
vectorize_segments <- function(df, dest, daynight){
  
  # Common base process
  AISsf1 <- df %>%
    st_as_sf(coords = c("x", "y"), crs = 3338) %>%
    arrange(Time) %>%
    mutate(
      Dim_Length = as.numeric(Dim_Length),
      Dim_Width = as.numeric(Dim_Width),
      Draught = as.numeric(Draught)
    ) %>%
    mutate(
      Dim_Length = if_else(Dim_Length == 0, NA_real_, Dim_Length),
      Dim_Width = if_else(Dim_Width == 0, NA_real_, Dim_Width),
      Draught = if_else(Draught == 0, NA_real_, Draught)
    ) %>%
    # Create 1 line per AIS ID
    group_by(newsegid) %>%
    # Summarize with common columns
    summarize(
      scramblemmsi = first(scramblemmsi),
      Time_Start = first(Time),
      Time_End = last(Time),
      Time_Of_Day = ifelse(daynight, 
                           case_when(daynight ~ first(timeofday, na_rm = TRUE),
                                     TRUE ~ NA_character_), NA
      ),
      SOG_Median = median(SOG, na.rm = TRUE),
      SOG_Mean = mean(SOG, na.rm = TRUE),
      Ship_Type = if (all(is.na(Ship_Type))) NA else if (length(unique(na.omit(Ship_Type))) == 1) unique(na.omit(Ship_Type)) else NA,
      AIS_Type = if (all(is.na(AIS_Type))) NA else if (length(unique(na.omit(AIS_Type))) == 1) unique(na.omit(AIS_Type)) else NA,
      Country = if (all(is.na(Country))) NA else if (length(unique(na.omit(Country))) == 1) unique(na.omit(Country)) else NA,
      Dim_Length = if (all(is.na(Dim_Length))) NA_real_ else if (length(unique(na.omit(Dim_Length))) == 1) unique(na.omit(Dim_Length)) else NA_real_,
      Dim_Width = if (all(is.na(Dim_Width))) NA_real_ else if (length(unique(na.omit(Dim_Width))) == 1) unique(na.omit(Dim_Width)) else NA_real_,
      Draught = if (all(is.na(Draught))) NA_real_ else if (length(unique(na.omit(Draught))) == 1) unique(na.omit(Draught)) else NA_real_,
      Destination = if (all(is.na(Destination))) NA else if (length(unique(na.omit(Destination))) == 1) unique(na.omit(Destination)) else NA,
      stopped_sog = median(stopped_sog),
      npoints = n(),
      geometry = st_combine(geometry)
    ) %>%
    mutate(
      Time_Of_Day =  factor(Time_Of_Day, levels = c("day", "night")),
      geometry = st_cast(geometry, "LINESTRING")
    ) %>%
    # Extract first and last points of the LINESTRING
    rowwise() %>%
    mutate(
      x_first = st_coordinates(geometry)[1, 1],
      y_first = st_coordinates(geometry)[1, 2],
      x_last = st_coordinates(geometry)[nrow(st_coordinates(geometry)), 1],
      y_last = st_coordinates(geometry)[nrow(st_coordinates(geometry)), 2]
    ) %>%
    ungroup() %>%
    st_make_valid() %>%
    filter(npoints > 1)
  
  if(!daynight){
    AISsf1 <- AISsf1 %>% dplyr::select(-Time_Of_Day)
  }
  
  dest$Destntn <-  trimws(gsub(" {2,}", " ", dest$Destntn))
  
  AISsf <- AISsf1 %>% 
    
    # Remove extraneous spaces from destination columns 
    mutate(Destination = gsub(" {2,}", " ", Destination)) %>% 
    
    # Add in destination codes 
    left_join(., dest, by=c("Destination" = "Destntn"), multiple="first") %>% 
    
    # Calculate total distance travelled
    mutate(Length_Km = as.numeric(st_length(.)/1000),
           Time_Start = format(Time_Start, "%Y-%m-%d %H:%M:%S"),
           Time_End = format(Time_End, "%Y-%m-%d %H:%M:%S")) %>% 
    
    # Remove any non-linestring types if still remaining after st_make_valid
    st_collection_extract("LINESTRING", warn = FALSE)
  
  return(AISsf)
}

#' Save Ship Segments
#'
#' This function saves the spatial ship segments as shapefiles for each unique ship type.
#'
#' @param df A spatial data frame (sf) containing ship segment data.
#' @param MoName A string representing the month name.
#' @param daynight A logical value indicating if the segments represent day or night.
#'
#' @return None. Saves shapefiles to the specified directory.
#' 
#' @examples
#' save_segments(AISsf, "January", TRUE)
save_segments <- function(df, MoName, daynight){
  
  # Loop through each ship type and save shp file 
  allTypes <- unique(df$AIS_Type)
  
  for (k in 1:length(allTypes)){
    
    AISfilteredType <- df %>%
      filter(AIS_Type==allTypes[k])
    
    # Save data in vector format
    if(length(AISfilteredType$newsegid) > 0){
      st_write(AISfilteredType,
               paste0("../Data_Processed_V4/Vector/Tracks_DayNight", 
                      daynight, "_",MoName,"-",allTypes[k],".shp"),
               append = F)
    }
  }
}