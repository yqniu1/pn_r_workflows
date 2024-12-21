# Load Libraries ----
library(purrr)
library(googlesheets4)
library(tidyverse)
library(data.table)
library(stringi)
library(qdapDictionaries)
library(hunspell)

# 1. Load Dataset: we primarily work from google suite ----
df <- read_sheet("any_google_sheet_with_a_response_column")
# the names dictionary is a vector of names from our customer database ----
names <- read_rds("~/Documents/text_mining/Source/data/namesDict.rds") 

# 2. Define NA Patterns: this detects most ways a user enters an invalid response ----
na_patterns <- c(
  "^\\s*(?i)na\\s*$", "^\\s*(?i)n/a\\s*$", "^\\s*(?i)n\\.a\\.\\s*$",
  "^\\s*(?i)not\\s+(applicable|available)\\s*$", "^\\s*(?i)none\\s*\\.*$",
  "^\\s*(?i)nothing\\s*\\.*$", "^\\s*(-{1,3})\\s*$", "^\\s*$", "^\\s*\\.+\\s*$"
)
na_pattern_combined <- paste(na_patterns, collapse = "|")

# 3. Define Gibberish Detection Function ----
detect_gibberish <- function(text) {
  if (is.na(text)) return(FALSE)
  text <- str_trim(text)
  if (text == "") return(FALSE)

  consecutive_patterns <- list(
    vowels = "^[aeiouy]{4,}$",
    consonants = "^[bcdfghjklmnpqrstvwxyz]{5,}$",
    symbols = "^[.,!@#$%^&*()]{5,}$"
  )
  is_gibberish <- any(map_lgl(consecutive_patterns, ~ str_detect(text, .x)))

  keyboard_pattern <- "^(asdf|qwer|zxcv)$"
  is_gibberish || str_detect(text, keyboard_pattern)
}

# 4. Preprocessing Function ----
preprocess_responses <- function(df, text_column = "response") {
  df %>%
    mutate(
      # if the response is less than 2 characters, consider it invalid
      is_short_response = str_length(str_trim(!!sym(text_column))) <= 2,
      
      # call the helper function
      is_gibberish = map_lgl(!!sym(text_column), detect_gibberish),
      
      # string detect with regex 
      is_NA = str_detect(str_trim(!!sym(text_column)), 
                         na_pattern_combined) | is.na(!!sym(text_column)),
      res_processed = if_else(
        is_short_response | is_gibberish | is_NA,
        NA_character_,
        str_trim(!!sym(text_column))
      )
    )
}

# 5. Experience Years Masking ----
mask_experience_years <- function(df, text_column = "res_processed", number_patterns) {
  years_regex <- generate_years_regex(number_patterns)

  df %>%
    rowwise() %>%
    mutate(
      detected_terms = list(str_extract_all(!!sym(text_column), regex(years_regex, ignore_case = TRUE))[[1]]),
      masked_text = if_else(
        !is.na(detected_terms) & length(detected_terms) > 0,
        str_replace_all(!!sym(text_column), regex(years_regex, ignore_case = TRUE), "[EXPERIENCE YEARS]"),
        !!sym(text_column)
      )
    ) %>%
    ungroup()
}

# 6. Name Processing Function ----
process_names_in_text <- function(df, names_vector, words_dict, safe_list) {
  safe_set <- setNames(rep(TRUE, length(safe_list)), tolower(safe_list))
  
  df %>%
    rowwise() %>%
    mutate(
      found_names = str_extract_all(masked_text, "\\b[A-Z][a-z]+\\b")[[1]],
      masked_text = str_replace_all(masked_text, regex(paste(found_names, collapse = "|")), "[NAME]")
    ) %>%
    ungroup()
}

# 7. Flag Non-English Words ----
flag_non_english_words <- function(df, safe_words, chunk_size = 10000) {
  df %>%
    mutate(
      not_a_standard_word = map_chr(masked_text, function(text) {
        words <- unlist(str_split(text, "\\s+"))
        invalid_words <- words[!hunspell_check(words) & !(tolower(words) %in% safe_words)]
        if (length(invalid_words) > 0) paste(invalid_words, collapse = ", ") else NA_character_
      })
    )
}

# 8. Generate Term Frequency ----
create_term_frequency <- function(df, col) {
  terms <- unlist(strsplit(df[[col]], ","))
  terms <- terms[!is.na(terms) & terms != ""]
  as_tibble(table(trimws(terms))) %>%
    arrange(desc(n))
}

# 9. Run Pipeline ----
# Preprocess Responses
df_preprocessed <- preprocess_responses(df)

# Generate Number Patterns
number_patterns <- generate_number_patterns()

# Mask Experience Years
df_masked <- mask_experience_years(df_preprocessed, number_patterns = number_patterns)

# Process Names
df_names_processed <- process_names_in_text(df_masked, names, words, safe_list)

# Flag Non-English Words
safe_words <- c(safe_list, common_contractions)
df_final <- flag_non_english_words(df_names_processed, safe_words)

# Create Term Frequency
term_frequency <- create_term_frequency(df_final, "not_a_standard_word")

# Export Final Dataset
write_sheet(df_final, ss = "https://docs.google.com/spreadsheets/d/1ihRxDaJRzEzpOVPEfKCkoBzmSr-VHT0LtnwX7KOc2IA", sheet = "cleaned")
