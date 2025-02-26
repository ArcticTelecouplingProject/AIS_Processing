#' Load and Process CSV Files for 2021-2022
#'
#' @param csvList List of CSV file paths
#' @param flags Data frame with country flags information
#' @return A data frame with loaded and processed data for 2023-2024
load_and_process_2023_2024 <- function(csvList, flags) {
  # Extract year and month from file names using substring
  MoName <- substr(basename(csvList[[1]][1]), 16, 22)
  yr <- substr(MoName, 1, 4)  # Extract year from MoName
  mnth <- substr(MoName, 6, 7)  # Extract month from MoName
  
  # Print processing information
  print(paste("Processing", yr, mnth))
  
  # Read in CSV files, treating empty strings and "NA" as missing values
  temp <- lapply(csvList, fread, header = TRUE, na.strings = c("", "NA"))
  AIScsv <- do.call(rbind, temp)  # Combine all CSV files into one data frame
  
  # Rename columns and process data to match required format
  AIScsv %>%
    rename(MMSI = mmsi, 
           IMO = imo, 
           Longitude = longitude, 
           Latitude = latitude, 
           Time = position_timestamp, 
           # Message_ID = message_type, 
           SOG = speed, # CONFIRM THAT THIS IS SPEED OVER GROUND 
           Ship_Type = ship_and_cargo_type, 
           Vessel_Name = name, 
           Draught = draught, 
           Destination = destination, 
           Navigational_status = status, 
           Dim_Length = length, 
           Dim_Width = width) %>%
    mutate(Time = lubridate::ymd_hms(Time, tz = "GMT"),  # Convert time to datetime with GMT timezone
           temp = as.numeric(substr(MMSI, 1, 3)),  # Extract country code from MMSI
           MMSI = as.integer(MMSI)) %>%  # Convert MMSI to integer
    left_join(., flags, by = join_by(temp == MID)) %>%  # Join with flags data to get country information
    dplyr::select(MMSI, 
                  Longitude, 
                  Latitude, 
                  Time, 
                  SOG, 
                  Ship_Type, 
                  Country, 
                  Vessel_Name, 
                  IMO, 
                  Draught, 
                  Destination, 
                  Navigational_status, 
                  Dim_Length, 
                  Dim_Width)  # Select relevant columns
}

#' Load and Process CSV Files for 2021-2022
#'
#' @param csvList List of CSV file paths
#' @param flags Data frame with country flags information
#' @return A data frame with loaded and processed data for 2021-2022
load_and_process_2021_2022 <- function(csvList, flags) {
  # Extract year and month from file names using substring
  MoName <- substr(csvList[[1]][1], nchar(csvList) - 32, nchar(csvList) - 27)
  yr <- substr(MoName, 1, 4)  # Extract year from MoName
  mnth <- substr(MoName, 5, 7)  # Extract month from MoName
  
  # Print processing information
  print(paste("Processing", yr, mnth))
  
  # Read in CSV files, treating empty strings and "NA" as missing values
  temp <- lapply(csvList, read.csv, header = TRUE, na.strings = c("", "NA"))
  AIScsv <- do.call(rbind, temp)  # Combine all CSV files into one data frame
  
  # Rename columns and process data to match required format
  AIScsv %>%
    subset(message_type != 27) %>%  # Remove rows with message type 27
    rename(MMSI = mmsi, IMO = imo, Longitude = longitude, Latitude = latitude, 
           Time = dt_pos_utc, Message_ID = message_type, SOG = sog, 
           Ship_Type = vessel_type_code, Vessel_Name = vessel_name, 
           Draught = draught, Destination = destination, 
           Navigational_status = nav_status_code, Dim_Length = length, 
           Dim_Width = width) %>%
    mutate(Time = lubridate::ymd_hms(Time, tz = "GMT"),  # Convert time to datetime with GMT timezone
           temp = as.numeric(substr(MMSI, 1, 3)),  # Extract country code from MMSI
           MMSI = as.integer(MMSI)) %>%  # Convert MMSI to integer
    left_join(., flags, by = join_by(temp == MID)) %>%  # Join with flags data to get country information
    dplyr::select(MMSI, 
                  Longitude, 
                  Latitude, 
                  Time,
                  SOG, Ship_Type, 
                  Country, Vessel_
                  Name, 
                  IMO, 
                  Draught, 
                  Destination, 
                  Navigational_status,
                  Dim_Length, 
                  Dim_Width)  # Select relevant columns
}

#' Load and Process CSV Files for 2015-2020
#'
#' @param csvList List of CSV file paths
#' @param flags Data frame with country flags information
#' @return A data frame with loaded and processed data for 2015-2020
load_and_process_2015_2020 <- function(csvList, flags) {
  # Extract year and month from file names using substring and remove hyphen
  MoName <- sub("-", "", substr(csvList[[1]][1], nchar(csvList) - 13, nchar(csvList) - 7))
  yr <- substr(MoName, 1, 4)  # Extract year from MoName
  mnth <- substr(MoName, 5, 7)  # Extract month from MoName
  
  # Print processing information
  print(paste("Processing", yr, mnth))
  
  # Read in CSV files with specified column classes (select relevant columns)
  temp <- lapply(csvList, read.csv, header = TRUE, na.strings = c("", "NA"),
                 colClasses = c(rep("character", 2), "NULL", "character", "NULL", 
                                "NULL", "character", rep("NULL", 6), "character", 
                                "NULL", rep("character", 8), "NULL", "character", 
                                "NULL", "character", "NULL", rep("character", 2), 
                                rep("NULL", 109)))
  AIScsv <- do.call(rbind, temp)  # Combine all CSV files into one data frame
  
  # Process position messages
  AIScsv_pos <- AIScsv %>%
    subset(!(Message_ID %in% c(5, 24, 27))) %>%  # Exclude messages with IDs 5, 24, and 27
    mutate(MMSI = as.integer(MMSI),  # Convert MMSI to integer
           Message_ID = as.integer(Message_ID),  # Convert Message_ID to integer
           Latitude = as.numeric(Latitude),  # Convert Latitude to numeric
           Longitude = as.numeric(Longitude),  # Convert Longitude to numeric
           SOG = as.numeric(SOG),  # Convert Speed Over Ground to numeric
           Navigational_status = as.numeric(Navigational_status)) %>%  # Convert Navigational status to numeric
    dplyr::select(-Draught, -Destination, -Country, -Dimension_to_Bow, 
                  -Dimension_to_stern, -Dimension_to_port, -Dimension_to_starboard, 
                  -Ship_Type, -Vessel_Name, -IMO) %>%  # Drop unnecessary columns
    mutate(Time = lubridate::ymd_hms(Time, tz = "GMT"),  # Convert time to datetime with GMT timezone
           Country = as.numeric(substr(MMSI, 1, 3))) %>%  # Extract country code from MMSI
    left_join(., flags, by = join_by(Country == MID)) %>%  # Join with flags data to get country information
    dplyr::select(-Short.Code, -Country) %>%  # Drop redundant columns
    rename(Country = Country.y)  # Rename the joined Country column
  
  # Process static messages
  AIScsv_stat <- AIScsv %>%
    filter(Message_ID %in% c(5, 24)) %>%  # Keep only messages with IDs 5 and 24
    mutate(Time = lubridate::ymd_hms(Time, tz = "GMT"),  # Convert time to datetime with GMT timezone
           Dim_Length = as.numeric(Dimension_to_Bow) + as.numeric(Dimension_to_stern),  # Calculate total length
           Dim_Width = as.numeric(Dimension_to_port) + as.numeric(Dimension_to_starboard),  # Calculate total width
           MMSI = as.integer(MMSI)) %>%  # Convert MMSI to integer
    dplyr::select(MMSI, Time, Vessel_Name, IMO, Ship_Type, Dim_Length, Dim_Width, Draught, Destination)  # Select relevant columns
  
  # Merge position and static messages based on MMSI and closest time match
  left_join(AIScsv_pos, AIScsv_stat, by = join_by(MMSI, closest(Time >= Time)), multiple = "first") %>%
    rename(Time = Time.x) %>%  # Rename time column to avoid conflicts
    dplyr::select(-Time.y, -Message_ID)  # Drop redundant time column
}