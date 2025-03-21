#' Update metadata information
#'
#' @param metadata List to store metadata information
#' @param key_prefix Prefix for metadata keys
#' @param df Data frame to analyze
#' @param column Column name to calculate unique counts
#'
#' @return Updated metadata list
update_metadata <- function(metadata, key_prefix, df, column = "scramblemmsi") {
  unique_count <- length(unique(df[[column]]))
  # total_count <- length(df[[column]])
  metadata[[paste0(key_prefix, "_mmsis")]] <- unique_count
  # metadata[[paste0(key_prefix, "_pts")]] <- total_count
  return(metadata)
}


#' Update Ship Type Metadata
#'
#' This function updates metadata for each unique ship type and for the total ship data.
#'
#' @param metadata A list or data frame containing metadata that will be updated.
#' @param df A data frame containing ship data, with a column `AIS_Type`.
#'
#' @return Updated metadata with counts for each ship type and total ships.
#' 
#' @examples
#' metadata <- list()
#' df <- data.frame(AIS_Type = c("Cargo", "Tanker", "Cargo"))
#' updated_metadata <- shiptype_metadata(metadata, df)
shiptype_metadata <- function(metadata, df){
  for(i in 1:length(unique(df$AIS_Type))){
    typ <- unique(df$AIS_Type)[i]
    temp <- df[df$AIS_Type == typ,]
    key <- paste0("n", tolower(substr(typ, 1, 3)))
    metadata <- update_metadata(metadata, key, temp)
  }
  metadata <- update_metadata(metadata, "ntotal", df)
  return(metadata)
}

#' Update Missing Data Metadata
#'
#' This function updates metadata with the percentage of missing data for ship dimensions and speed.
#'
#' @param metadata A list or data frame containing metadata that will be updated.
#' @param df A data frame containing ship data, with columns `Dim_Length`, `Dim_Width`, and `SOG`.
#'
#' @return Updated metadata with percentages of missing data.
#' 
#' @examples
#' metadata <- list()
#' df <- data.frame(Dim_Length = c(50, NA, 0), Dim_Width = c(20, 0, 30), SOG = c(5, NA, 7))
#' updated_metadata <- missing_data_metadata(metadata, df)
missing_data_metadata <- function(metadata, df){
  
  nolength <- which(df$Dim_Length == 0 | is.na(df$Dim_Length))
  nowidth <- which(df$Dim_Width == 0 | is.na(df$Dim_Width))
  
  metadata$pctmissingwidth <- round(length(nowidth)/length(df$Dim_Width)*100, 2)
  metadata$pctmissinglength <- round(length(nolength)/length(df$Dim_Length)*100, 2)
  metadata$pctmissingSOG <-  round(sum(is.na(df$SOG_Mean))/length(df$Dim_Length)*100, 2)
  
  return(metadata)
}


