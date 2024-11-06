#' Main Processing Function
#'
#' @param csvList List of CSV file paths
#' @param year Year of the data
#' @param flags Data frame with country flags information
#' @param dest Data frame with destination codes information
#' @param scrambleids Csv file containing paired MMSIs with their anonymized counterparts 
#' @param output Output type for the data ("vector", "hex", or "raster")
#' @param hexgrid Hex grid for hex-based calculations (default: NULL)
#' @param daynight Boolean indicating whether to handle day-night transitions (default: FALSE)
#' @param speed_threshold Speed threshold for identifying stopped vessels (default: 3 knots)
#' @param time_threshold Time threshold for identifying stopped vessels (in hours, default: 1 hour)
#' @param timediff_threshold Time difference threshold for segmenting (default: 6 hours)
#' @param distdiff_threshold Distance difference threshold for segmenting (default: 60 units)
#' @return Processed AIS data frame
process_ais_data <- function(csvList, 
                             year, 
                             flags, 
                             dest, 
                             scrambleids, 
                             hexgrid = NULL, 
                             daynight = FALSE, 
                             output = "vector", 
                             speed_threshold = 2, 
                             time_threshold = 1,
                             timediff_threshold = 6, 
                             distdiff_threshold = 60) {
  
  # output = "vector"
  # speed_threshold = 2
  # time_threshold = 1
  # timediff_threshold = 6
  # distdiff_threshold = 60
  # daynight <- FALSE

  # Start segment timer to measure processing time
  start <- proc.time()
  
  # Initiate runtimes and metadata dataframes 
  metadata <- data.frame(init = 1)
  runtimes <- data.frame(init = 1)
  
  # Load and process data based on year range
  if (year %in% c(2021:2022)) {
    AIScsv <- load_and_process_2021_2022(csvList, flags)
  } else if (year %in% c(2015:2020)) {
    AIScsv <- load_and_process_2015_2020(csvList, flags)
  }
  
  MoName <- paste0(year(AIScsv$Time[1]), sprintf("%02d", month(AIScsv$Time[1])))
  
  clean_data <- AIScsv %>%
    # Initial metadata
    { metadata <<- update_metadata(metadata, "orig", ., column = "MMSI"); . } %>%
    
    # Measure import time
    { runtimes$importtime <<- (proc.time() - start)[[3]] / 60; . } %>%
    
    #### CLEANING --------------------------------------------------------------
    
    # Clean lat/long values
    clean_lat_lon() %>%
    
    # Remove invalid MMSIs
    clean_mmsi() %>%
    
    # Remove stationary aids to navigation
    remove_navigation_aids() %>%
    { metadata <<- update_metadata(metadata, "aton", ., column = "MMSI"); . } %>%
    
    # Add scrambled AIS IDs and remove original MMSI
    add_scrambled_ids(scrambleids = scrambleids) %>%
    
    # Remove frost flowers (multiple messages from the same exact location)
    remove_frost_flowers(.) %>% 
    { metadata <<- update_metadata(metadata, "ff", ., column = "scramblemmsi"); . } %>%
    
    # Transform to spatial object and transform coordinates
     transform_to_spatial() %>%
    
    # Calculate speed and remove duplicate or invalid points
    calculate_speed(.) %>% 
    { metadata <<- update_metadata(metadata, "redund", ., column = "scramblemmsi"); . } %>%

    # Implement speed filter and remove invalid speeds
    filter(speed < 100) %>% 
    filter(!is.na(speed)) %>%
    { metadata <<- update_metadata(metadata, "speed", ., column = "scramblemmsi"); . } %>%

    # DIFFERENCE IS OCCURRING HERE
    # Recalculate speed now that erroneous speedy points have been removed
    calculate_speed() %>%
    
    # Identify beginning of new segments
    mutate(cut_line = ifelse(timediff > timediff_threshold | distdiff > distdiff_threshold, TRUE, FALSE)) %>%
    mutate(cut_line = ifelse(is.na(cut_line), TRUE, cut_line)) %>% 
    
    # Identify vessels traveling below speed_threshold for more than time_threshold
    identify_stopped_vessels(speed_threshold = speed_threshold, time_threshold = time_threshold) %>%
    
    # Handle day-night transitions if daynight is TRUE
    { if (daynight) handle_daynight_transitions(.) else . } %>%
    
    # Identify ship types
    id_ship_type() %>%
    
    # Measure import time
    { runtimes$cleantime <<- (proc.time() - start)[[3]] / 60; . } %>%
    
    {print(paste0(MoName, " cleaning complete.")); .} %>% 
    
    #### SEGMENTATION ----------------------------------------------------------
    
    # Create segments for AIS data
    create_segments() %>%

    # Expand segments to connect track lines
    expand_segments() %>%

    # Remove short segments with less than 2 points
    remove_short_segments() 
    
    # Measure import time
    runtimes$segment <- (proc.time() - start)[[3]] / 60
  
    print(paste0(MoName, " segmentation complete."))
  
  #### VECTORIZATION ---------------------------------------------------------
  if (output %in% c("vector", "raster")) {
    
    # Transform to lines
    vec_data <- clean_data %>%
      vectorize_segments(dest = dest, daynight = daynight) %>% 
      dplyr::select(-Dim_Width, -Dim_Length, -Draught) 

    # Export as shapefiles
    if (output == "vector") {
      save_segments(vec_data, MoName = MoName, daynight = daynight)
    }
    
    # Generate metadata for the final number of ships after cleaning
    metadata <- update_metadata(metadata, "cleaned", vec_data, column = "scramblemmsi")
    
    # Final stats for ship type numbers 
    metadata <- shiptype_metadata(metadata, vec_data)
      
    # Calculate pct data missing for key columns
    metadata <- missing_data_metadata(metadata, vec_data)
    
    # Measure import time
    runtimes$vector <- (proc.time() - start)[[3]] / 60
    
    print(paste0(MoName, " vectorization complete."))
    
    print(runtimes)
    write.csv(runtimes, paste0("../Data_Processed_V3/Metadata/Vector_Runtimes_", MoName, "_DayNight", daynight, ".csv"))
    
    print(metadata) 
    write.csv(metadata, paste0("../Data_Processed_V3/Metadata/Vector_Metadata_", MoName, "_DayNight", daynight, ".csv"))
  }

  #### HEX -------------------------------------------------------------------
  if (output == "hex") {
    
    # Main Script
    hex_data <- clean_data %>% calculate_hex_grid_summary(., hexgrid, daynight)

    # Save data in vector format
    write_sf(hex_data, paste0("../Data_Processed_V3/Hex/Hex_", MoName, "_DayNight", daynight, ".shp"))
    
    # Measure import time
    runtimes$hex <- (proc.time() - start)[[3]] / 60
    
    print(paste0(MoName, " hex generation complete."))
    
    print(runtimes)
    write.csv(runtimes, paste0("../Data_Processed_V3/Metadata/Hex_Runtimes_", MoName, "_DayNight", daynight, ".csv"))
    
    print(metadata) 
    write.csv(runtimes, paste0("../Data_Processed_V3/Metadata/Hex_Metadata_", MoName, "_DayNight", daynight, ".csv"))
  }

  # #### RASTER --------------------------------------------------------------
  # if (output == "raster") {
  #   # ADD RASTER CODE HERE.... 
  # }
}
