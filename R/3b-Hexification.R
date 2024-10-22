#' Calculate Summary Statistics for Each Ship Type
#' 
#' This function calculates summary statistics for a specific ship type within the AIS data.
#' 
#' @param ais_data Data frame containing AIS information.
#' @param ship_type The type of ship for which statistics are being calculated.
#' @param include_daynight Logical indicating whether to include day/night calculations.
#' @return A data frame with summary statistics for the given ship type.
calculate_summary_stats <- function(ais_data, ship_type, include_daynight) {
  # Filter by ship type
  filtered_data <- ais_data %>%
    filter(AIS_Type == ship_type)
  
  if (include_daynight) {
    day_night_stats <- calculate_day_night_stats(filtered_data)
  } else {
    day_night_stats <- NULL
  }
  
  summary_stats <- filtered_data %>%
    st_drop_geometry() %>%
    group_by(hexID) %>%
    summarize(Hrs = round(sum(timediff, na.rm = TRUE), 2),
              nShp = length(unique(scramblemmsi)),
              OpD = length(unique(AIS_ID)))
  
  if (!is.null(day_night_stats)) {
    summary_stats <- left_join(summary_stats, day_night_stats)
  }
  
  colnames(summary_stats)[2:ncol(summary_stats)] <- paste0(colnames(summary_stats)[2:ncol(summary_stats)], "_", substring(ship_type, 1, 2))
  
  return(summary_stats)
}

#' Calculate Day/Night Summary Statistics
#' 
#' This function calculates summary statistics for daytime and nighttime ship activities.
#' 
#' @param filtered_data Data frame containing filtered AIS information.
#' @return A data frame with day/night summary statistics.
calculate_day_night_stats <- function(filtered_data) {
  hours_data <- filtered_data %>%
    st_drop_geometry() %>%
    arrange(Time) %>%
    group_by(AIS_ID, hexID, scramblemmsi, timeofday) %>%
    mutate(rownum = 1:n()) %>%
    ungroup() %>%
    mutate(daychange = ifelse(rownum == 1, 1, 0))
  
  hour_summaries <- hours_data %>%
    filter(daychange != 1) %>%
    group_by(AIS_ID, hexID, scramblemmsi, timeofday) %>%
    summarize(Hrs = sum(timediff))
  
  night_hours <- hour_summaries %>%
    filter(timeofday == "night") %>%
    group_by(hexID) %>%
    summarize(N_Hrs = round(sum(Hrs, na.rm = TRUE), 2),
              N_nShp = length(unique(scramblemmsi)),
              N_OpD = length(unique(AIS_ID)))
  
  day_hours <- hour_summaries %>%
    filter(timeofday == "day") %>%
    group_by(hexID) %>%
    summarize(D_Hrs = round(sum(Hrs, na.rm = TRUE), 2),
              D_nShp = length(unique(scramblemmsi)),
              D_OpD = length(unique(AIS_ID)))
  
  day_night_summary <- left_join(day_hours, night_hours, by = c("hexID")) %>%
    mutate_at(c(1:ncol(.)), ~replace_na(., 0))
  
  return(day_night_summary)
}

#' Calculate Aggregate Summary Statistics
#' 
#' This function calculates summary statistics for all ship types in aggregate.
#' 
#' @param ais_data Data frame containing AIS information.
#' @param include_daynight Logical indicating whether to include day/night calculations.
#' @return A data frame with summary statistics for all ship types.
calculate_aggregate_stats <- function(ais_data, include_daynight) {
  if (include_daynight) {
    day_night_summary <- calculate_day_night_stats(ais_data)
  } else {
    day_night_summary <- NULL
  }
  
  total_hours <- ais_data %>%
    st_drop_geometry() %>%
    group_by(hexID) %>%
    summarize(Hrs = round(sum(timediff, na.rm = TRUE), 2),
              nShp = length(unique(scramblemmsi)),
              OpD = length(unique(AIS_ID)))
  
  if (!is.null(day_night_summary)) {
    aggregate_summary <- left_join(total_hours, day_night_summary, by = c("hexID"))
  } else {
    aggregate_summary <- total_hours
  }
  
  colnames(aggregate_summary)[2:ncol(aggregate_summary)] <- paste0(colnames(aggregate_summary)[2:ncol(aggregate_summary)], "_Al")
  
  return(aggregate_summary)
}

#' Calculate Hex Grid Summary
#' 
#' This function calculates summary statistics for each ship type and aggregates them into the provided hex grid.
#' 
#' @param ais_data Data frame containing AIS information.
#' @param hex_grid Data frame representing the hex grid.
#' @param include_daynight Logical indicating whether to include day/night calculations.
#' @param year Year of the data.
#' @param month Month of the data.
#' @return A data frame with the hex grid including summary statistics.
calculate_hex_grid_summary <- function(ais_data, hex_grid, include_daynight, year, month) {
  ship_types <- unique(ais_data$AIS_Type)
  
  for (k in 1:length(ship_types)) {
    summary_stats <- calculate_summary_stats(ais_data, ship_types[k], include_daynight)
    hex_grid <- left_join(hex_grid, summary_stats, by = "hexID")
  }
  
  aggregate_stats <- calculate_aggregate_stats(ais_data, include_daynight)
  hex_grid <- left_join(hex_grid, aggregate_stats, by = "hexID")
  
  hex_grid$year <- year(ais_data$Time_Start[1])
  hex_grid$month <- month(ais_data$Time_Start[1])
  
  print(paste("Finished hex calcs", year, month))
  
  return(hex_grid)
}