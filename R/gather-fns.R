# Add population country-level population data sourcd from WBG API--------------
get_pop <- function() {
  url <- "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
  queryString <- list(format = "json", date = "2000:2024", per_page = "30000")
  response <- VERB("GET", url, body = "", query = queryString, content_type("application/octet-stream"), set_cookies(`api_http.cookie` = "2f4d39862a2fa1b0b0b0c4ad37e6251a"), encode = "raw")
  pop <- jsonlite::fromJSON(content(response, "text"))[[2]] %>%
    mutate(iso3 = countryiso3code, pop = value, year = as.numeric(date), .keep = "none")
  return(pop)
}

# Create starter dataframe with all country-year-months------------------------
write_starter_csv <- function() {
  country_list <- read_csv("https://raw.githubusercontent.com/compoundrisk/monitor/databricks/src/country-groups.csv",
                           col_types = cols(.default = "c"))
  starter <- country_list %>%
    select(iso3 = Code) %>%
    filter(iso3 != "CHI") %>%
    left_join(get_pop(), by = c("iso3")) %>%
    # No WBG population data for Taiwan
    filter(iso3 != "TWN") %>%
    bind_rows(data.frame(iso3 = "TWN", pop = NA, year = 2000:2024)) %>%
    # Add in months and make sure every country has rows for all years 2000-2024
    mutate(
      across(c(iso3), ~ factor(.x)),
      year = factor(year, levels = 2000:2024),
      month = paste(1:12, collapse = ",")) %>%
    separate_longer_delim(month, delim = ",") %>%
    mutate(month = as.numeric(month)) %>%
    complete(iso3, year, month) %>%
    # Population data only extends to 2022; use 2022 data for 2023 and 2024
    group_by(iso3) %>%
    fill(pop) %>%
    ungroup() %>%
    mutate(.before = year,
           year = as.numeric(as.character(year)),
           yearmon = as.yearmon(glue("{year}-{month}"))) %>%
    arrange(year, month, iso3) %>%
    # Remove months after February 2024
    filter(!(year == 2024 & month > 3))
  write_csv(starter, file.path(cm_dir, "starter.csv"))
  return(starter)
}

# Income-levels and lending categories-----------------------------------------
write_income_csv <- function() {
  income_sheet <- "Country Analytical History"
  # Income classifications are set on July 1, to start each fiscal year. This is
  # when I will begin the month classification. However, the data is based on the
  # previous calendar year's data. E.g., for FY24, classifications changed on 
  # 1 July 2023 based on data from 1 January 2022 to 31 December 2022.
  # Below, I add 1.5 years to the data year to mark the month of the new
  # classification, at the start of the fiscal year.
  income_level_headers <- c(
    "iso3", "country",
    unlist(read_xlsx("source-data/OGHIST.xlsx", sheet = income_sheet,
                     range = "C5:AL6", col_names = T)))
  income_levels <- read_xlsx("source-data/OGHIST.xlsx", sheet = income_sheet,
                             range = "A12:AL238", col_names = income_level_headers,
                             col_types = "text", na = "..") %>%
    filter(!is.na(iso3)) %>%
    pivot_longer(
      cols = -c(iso3, country), names_to = "data_year", values_to = "income_level",
      names_transform = as.numeric) %>%
    filter(data_year > 1999 & !is.na(income_level)) %>%
    mutate(
      iso3 = factor(iso3),
      income_level = factor(income_level, levels = c("L", "LM", "LM*", "UM", "H"), labels = c("lower_income", "lower_middle_income", "lower_middle_income", "upper_middle_income", "upper_income")),
      FY = data_year + 2,
      FY_yearmon = as.yearmon(FY - 0.5),
      year = lubridate::year(FY_yearmon),
      month = lubridate::month(FY_yearmon)) %>%
    # Separate Serbia and Montenegro into two countries; doing so gives the same
    # income level to both countries for FY92-FY07 (not sure I want to do this,
    # I should actually be including YUG in the dataset as YUG for relevant years)
    mutate(iso3 = str_replace(iso3, "^YUG$", "SRB,MNE")) %>%
    separate_longer_delim(iso3, delim = ",") %>%
    # Add in all months, with data starting at the start of the fiscal year
    right_join(select(starter, -pop), by = c("iso3", "year", "month")) %>%
    arrange(iso3, year, month) %>%
    group_by(iso3) %>%
    mutate(data_year = case_when(is.na(income_level) ~ NA, T ~ data_year)) %>%
    fill(data_year, FY) %>% ungroup() %>%
    mutate(.by = c(iso3, FY), no_data = all(is.na(income_level))) %>%
    group_by(iso3) %>%
    fill(income_level, .direction = "down") %>% ungroup() %>%
    filter(!is.na(income_level)) %>%
    # mutate(income_level = case_when(no_data ~ NA, T ~ income_level)) %>%
    mutate(income_level_months_stale = 12 * as.numeric(
      as.yearmon(paste(year, month, sep = "-")) - data_year - 1)) %>%
    # Replace income_level with ordinal version
    mutate(
      WBG_income_level = as.numeric(income_level),
      # Create separate columns for each income level
      WBG_lower_income = case_when(WBG_income_level == 1 ~ 1, T ~ 0),
      WBG_lower_middle_income = case_when(WBG_income_level == 2 ~ 1, T ~ 0),
      WBG_upper_middle_income = case_when(WBG_income_level == 3 ~ 1, T ~ 0),
      WBG_upper_income = case_when(WBG_income_level == 4 ~ 1, T ~ 0)) %>%
    select(iso3, year, month,
           WBG_income_level, WBG_income_level_months_stale = income_level_months_stale, ends_with("income"))
  write_csv(income_levels, file.path(cm_dir, "wbg-income-levels.csv"))
}

# Add lending categories ------------------------------------------------------
write_lending_csv <- function() {
  lending_categories <- read_xlsx("source-data/OGHIST.xlsx",
                                  sheet = "Operational Category Change",
                                  range = "A10:D519", col_types = "text") %>%
    filter(!is.na(`Fiscal year`)) %>%
    mutate(
      FY = case_when(
        str_sub(`Fiscal year`, 3, 3) > 3 ~ str_replace(`Fiscal year`, "FY", "19"),
        str_sub(`Fiscal year`, 3, 3) < 3 ~ str_replace(`Fiscal year`, "FY", "20")),
      FY = as.numeric(FY),
      # yearmon = as.yearmon(FY - 0.5),
      iso3 = name2iso(Country),
      iso3 = case_when(
        Country == "Czechoslovakia" ~ "CZE, SVK",
        T ~ iso3)) %>%
    separate_longer_delim(iso3, delim = ", ")
  initial_categories <- lending_categories %>%
    slice_min(by = Country, order_by = FY) %>%
    select(iso3, category = From) %>%
    mutate(FY = 1979) 
  category_changes <- lending_categories %>%
    select(iso3, FY, category = To)
  lending_categories <- bind_rows(initial_categories, category_changes) %>%
    # The documents classifications change in 2009: "Beginning in FY09, the number
    # of IBRD levels are reduced in accordance with the Memorandum to the Executive
    # Directors dated January 17, 2008 (R2008-0007). Level II becomes the effective
    # IDA eligibility threshold with the historic IDA eligibility threshold footnoted;
    # Level III is described as "IBRD terms"; Level IV becomes "IBRD Graduation". As
    # a result, categories are not strictly comparable to those used in previous years.			
    mutate(
      WBG_lending_category = as.numeric(case_when(
        FY < 2009 ~ str_replace_all(category, c(
          "^I$" = "1", "^II$" = "2", "^III|IV$" = "3", "^V$" = "4")),
        FY >= 2009 ~ str_replace_all(category, c(
          "^I$" = "1", "^II$" = "2", "^III$" = "3", "^IV$" = "4")))))
  all_months <- tibble(
    year = factor(1978, levels = 1978:2024),
    month = factor(1, levels = 1:12),
    iso3 = factor("CHL", levels = unique(lending_categories$iso3))) %>%
    complete(year, month, iso3) %>%
    mutate(across(c(year, month), ~ as.numeric(as.character(.x))))
  lending_categories_all_months <- lending_categories %>%
    filter(!is.na(WBG_lending_category)) %>%
    mutate(
      yearmon = as.yearmon(FY - 0.5),
      year = lubridate::year(yearmon),
      month = lubridate::month(yearmon)) %>%
    full_join(all_months, by = c("iso3", "year", "month")) %>%
    arrange(iso3, year, month) %>%
    group_by(iso3) %>%
    fill(WBG_lending_category) %>%
    ungroup() %>%
    mutate(
      # Create separate columns for each lending category
      WBG_lend_cat_civil_works = case_when(WBG_lending_category == 1 ~ 1, T ~ 0),
      WBG_lend_cat_ida = case_when(WBG_lending_category == 2 ~ 1, T ~ 0),
      WBG_lend_cat_ibrd = case_when(WBG_lending_category == 3 ~ 1, T ~ 0),
      WBG_lend_cat_ibrd_grad = case_when(WBG_lending_category == 4 ~ 1, T ~ 0)) %>%
    select(iso3, year, month, starts_with("WBG_")) %>%
    mutate(.by = iso3, WBG_category_change = WBG_lending_category - lag(WBG_lending_category)) %>%
    filter(year > 1999 & !is.na(WBG_lending_category))
  lending_categories_all_iso <- lending_categories_all_months %>%
    right_join(select(starter, -pop), by = c("iso3", "year", "month")) %>%
    sjmisc::replace_na(starts_with("WBG_lend_cat"), WBG_category_change, value = 0)
  write_csv(lending_categories_all_iso, file.path(cm_dir, "wbg-lending-categories.csv"))
}

# Add ACLED dataset-------------------------------------------------------------
write_acled_csv <- function() {
  if (run_acled) {
    # Build API query (will run numerous times until all events are acquired)
    # Select relevant fields from ACLED database 
    fields <- "event_id_cnty|iso|event_date|event_type|fatalities|disorder_type|sub_event_type|actor1|actor2|assoc_actor_1|assoc_actor_2"
    # Load ACLED API credentials from file from a CSV with columns "username" and "key"
    credentials <- read_csv(".access/acled.csv", col_types = "c")
    # Alternatively, manually enter here
    # credentials <- data.frame(username = "----@worldbank.org", key = "-----")
    
    # Gather data from ACLED API
    acled_list <- NULL
    end_date <- Sys.Date()
    continue <- T
    while (end_date > as.Date(as.yearmon(Sys.Date() - 45) - 20) & continue) {
      start_date <- end_date - 30
      dates <- paste(start_date,end_date, sep = "|")
      print(dates)
      
      acled_url <- paste0("https://api.acleddata.com/acled/read/?key=", credentials$key, "&email=", credentials$username,
                          "&event_date=", dates, "&event_date_where=BETWEEN",
                          "&fields=", fields,
                          "&limit=0")
      
      acled <- curl_and_delete(acled_url, FUN = fromJSON)
      acled_list <- c(acled_list, list(as_tibble(acled$data)))
      continue = nrow(acled$data) > 0 | !is.na(nrow(acled$data))
      end_date <- as.Date(min(acled$data$event_date)) - 1
    }
    
    # Create and tidy ACLED dataset
    acled <- acled_list %>% bind_rows() %>% as_tibble()
    acled <- acled %>% 
      mutate(iso = as.numeric(iso),
             cnty_id = substr(event_id_cnty, 1, 3),
             iso3 = countrycode(iso, origin = "iso3n", destination = "iso3c"),
             iso3 = case_when(cnty_id == "XKX" ~ "XKX", T ~ iso3),
             # iso3b = countrycode(cnty_id, origin = "cowc", destination = "iso3c"),
             # iso3 = case_when(is.na(iso3) ~ iso3b, T ~ iso3),
             fatalities = as.numeric(fatalities),
             event_date = as.Date(event_date),
             year = lubridate::year(event_date),
             month = lubridate::month(event_date)) %>%
      select(event_id_cnty, iso, iso3, event_date, year, month, everything()) %>%
      select(-cnty_id) %>%
      arrange(event_date) %>%
      mutate(across(
        .cols = c(event_type, disorder_type, sub_event_type),
        ~ factor(.x))) %>%
      mutate(gang = 
               str_detect(actor1, "Unidentified Gang") | str_detect(actor1, "Unidentified Armed Group") |
               str_detect(actor2, "Unidentified Gang") | str_detect(actor2, "Unidentified Armed Group") |
               str_detect(assoc_actor_1, "Unidentified Gang") | str_detect(assoc_actor_1, "Unidentified Armed Group") |
               str_detect(assoc_actor_2, "Unidentified Gang") | str_detect(assoc_actor_2, "Unidentified Armed Group"))
    
    write_csv(acled, "source-data/acled-processed.csv")
  } else {
    acled <- read_csv("source-data/acled-processed.csv", col_types = "cdcDddfdffccccl")
  }
  
  # Number of conflict related deaths
  conflict_related_deaths <- acled %>% 
    filter(
      event_type %in% c("Battles", "Violence against civilians", "Explosions/Remote violence") &
        fatalities > 0 &
        !gang) %>% 
    summarize(.by = c(iso3, year, month), ACLED_conflict_related_deaths = sum(fatalities))
  # Number of ACLED events, including peaceful protests but excluding gang-related
  event_count <- acled %>% 
    filter(!gang) %>%
    # filter(!sub_event_type == "Peaceful protest") %>%
    count(iso3, year, month, event_type) %>%
    pivot_wider(names_from = event_type, values_from = n) %>%
    rename_with(.cols = -c(iso3, year, month), ~ paste0("ACLED_", slugify(.x, non_alphanum_replace = "_"))) %>%
    rowwise() %>%
    mutate(ACLED_events = sum(c_across(contains("ACLED")), na.rm = T)) %>%
    ungroup()
  # Number of events by sub-type, including peaceful protests but excluding gang-related
  sub_event_count <- acled %>% 
    filter(!gang) %>%
    # filter(!sub_event_type  == "Peaceful protest") %>%
    mutate(sub_event_type = paste(event_type, sub_event_type, sep = "_")) %>%
    count(iso3, year, month, sub_event_type) %>%
    pivot_wider(names_from = sub_event_type, values_from = n) %>%
    rename_with(.cols = -c(iso3, year, month), ~ paste0("ACLED_", slugify(.x, non_alphanum_replace = "_")))
  # Number of gang-related events, exluding peaceful protests
  gang_events <- acled %>%
    filter(gang) %>%
    filter(!sub_event_type  == "Peaceful protest") %>%
    count(iso3, year, month) %>%
    rename(ACLED_Gang_Events = n)
  
  # Calculate monthly totals for each country
  acled_monthly <- 
    reduce(list(conflict_related_deaths, event_count, sub_event_count, gang_events),
           full_join,
           by = c("iso3", "year", "month")) %>%
    mutate(iso3year = paste(iso3, year), .before = 1) %>%
    # Assign 0 to all NA fields for all country-years that appear in ACLED
    right_join(
      filter(starter, paste(iso3, year) %in% .$iso3year),
      by = c("iso3", "year", "month")) %>%
    select(-iso3year) %>%
    filter(!(year == 2003 & month < 12)) %>%
    sjmisc::replace_na(contains("ACLED"), value = 0) %>%
    # Add gang events and remove peaceful protests from ACLED event count
    mutate(ACLED_events = ACLED_events + ACLED_Gang_Events - ACLED_protests_peaceful_protest) %>%
    # Calculate change in events and deaths
    mutate(
      .by = c(iso3),
      BRD_lag = sapply(1:12, \(l) lag(ACLED_conflict_related_deaths, l)),
      Events_lag = sapply(1:12, \(l) lag(ACLED_events, l))) %>%
    rowwise() %>%
    mutate(
      ACLED_conflict_related_deaths_change = ACLED_conflict_related_deaths/mean(c_across(contains("BRD_lag")), na.rm = T) - 1,
      ACLED_events_change = ACLED_events/mean(c_across(contains("Events_lag")), na.rm = T) - 1) %>%
    select(-matches("_lag$")) %>%
    ungroup() %>%
    # Calculate BRD per 100k
    mutate(ACLED_BRD_per_100k = ACLED_conflict_related_deaths/pop * 100000) %>%
    select(iso3, year, month, pop, everything())
  write_csv(acled_monthly, file.path(cm_dir, "acled.csv"))
}

# Add UCDP dataset-------------------------------------------------------------
write_ucdp_csv <- function() {
  ucdp_geo <- readRDS("source-data/GEDEvent_v23_1.rds")
  ucdp_candidate_2023 <- read_csv("https://ucdp.uu.se/downloads/candidateged/GEDEvent_v23_01_23_12.csv",
                                  col_types = "dcddcd_dc_dc_dc_dcdcccccdccccddcdcdcddTTddddddddd", col_names = names(ucdp_geo))
  ucdp_candidate_2024 <- read_csv(c(
    "https://ucdp.uu.se/downloads/candidateged/GEDEvent_v24_0_1.csv",
    "https://ucdp.uu.se/downloads/candidateged/GEDEvent_v24_0_2.csv",
    "https://ucdp.uu.se/downloads/candidateged/GEDEvent_v24_0_3.csv"),
    col_types = "dcddcd_dc_dc_dc_dcdcccccdccccddcdcdcddTTddddddddd", col_names = names(ucdp_geo))
  ucdp_all <- bind_rows(ucdp_geo, ucdp_candidate_2023, ucdp_candidate_2024)
  # Disaggregate event timespans to daily averages
  ucdp_brd_nested <- ucdp_all %>%
    filter(year > 1999) %>%
    select(id, country, country_id, date_start, date_end, deaths = best) %>%
    rowwise() %>%
    mutate(.keep = "unused",
           across(c(date_start, date_end), ~ as.Date(.x)),
           date = list(date_start:date_end),
           duration_days = length(date),
           daily_deaths = deaths/duration_days) %>%
    ungroup()
  # Reaggregate daily averages to monthly averages
  ucdp_brd <- ucdp_brd_nested %>%
    unnest(date) %>%
    mutate(
      date = as.Date(date),
      month = lubridate::month(date),
      year = lubridate::year(date)) %>%
    summarize(.by = c(country, country_id, year, month), deaths = sum(daily_deaths), events = n()) %>%
    mutate(iso3 = countrycode(country_id, origin = "gwn", destination = "iso3c",
                              custom_match = c("345" = "SRB", "347" = "XKX", "678" = "YEM", "816"= "VNM"))) %>%
    select(-country, -country_id)
  # Set values to 0 for all NAs (UCDP has global coverage)
  ucdp_monthly <- left_join(starter, ucdp_brd, by = c("iso3", "year", "month")) %>%
    tidyr::replace_na(list(deaths = 0, events = 0)) %>%
    rename(UCDP_BRD = deaths, UCDP_events = events) %>%
    mutate(UCDP_BRD_per_100k = UCDP_BRD/pop * 100000) %>%
    mutate(
      .by = c(iso3),
      BRD_lag = sapply(1:12, \(l) lag(UCDP_BRD, l))) %>%
    rowwise() %>%
    mutate(
      UCDP_BRD_change = UCDP_BRD/mean(c_across(contains("BRD_lag")), na.rm = T) - 1) %>%
    select(-matches("_lag$")) %>%
    ungroup()
  write_csv(ucdp_monthly, file.path(cm_dir, "ucdp.csv"))
}

# Add GIC dataset on coup-related events---------------------------------------
write_gic_csv <- function() {
  gic <- read_tsv("http://www.uky.edu/~clthyn2/coup_data/powell_thyne_coups_final.txt",
                  col_types = "cdddddddc") %>%
    filter(year > 1999) %>%
    mutate(
      iso3 = name2iso(country),
      GIC_coup_successful = case_when(coup == 2 ~ T),
      GIC_coup_failed = case_when(coup == 1 ~ T)) %>%
    summarize(
      .by = c(iso3, year, month),
      across(.cols = contains("GIC"), ~ sum(.x, na.rm = T)))
  gic <- left_join(starter, gic, by = c("iso3", "year", "month")) %>%
    sjmisc::replace_na(contains("GIC"), value = 0)
  write_csv(gic, file.path(cm_dir, "gic.csv"))
}

# Add IFES on elections (API doesn't include interference)---------------------
lag_multi <- function(x, ns, default = NA, matrix = T, FUN = NULL) {
  mat <- matrix(unlist(map(ns, \(n) lag(x, n, default = default))), ncol = length(ns), nrow = length(x))
  if (matrix) return(mat)
  apply(mat, 1, c, simplify = F)
}

lead_multi <- function(x, ns, default = NA, matrix = T, FUN = NULL) {
  mat <- matrix(unlist(map(ns, \(n) lead(x, n, default = default))), ncol = length(ns), nrow = length(x))
  if (matrix) return(mat)
  apply(mat, 1, c, simplify = F)
}

write_ifes_csv <- function() {
  url <- "https://electionguide.org/api/v2/elections_demo/"
  token <- readLines(".access/ifes-authorization.txt", warn = F)
  ifes_response <- request(url) %>%
    req_headers(Authorization = paste("Token", token)) %>%
    req_perform()
  ifes_data <- ifes_response %>% resp_body_string() %>%
    fromJSON() %>%
    as_tibble()
  ifes <- ifes_data %>%
    select(
      election_id, election_name, #date_updated,
      district, election_type,
      election_range_start_date, election_range_end_date,
      election_declared_start_date, election_declared_end_date,
      is_snap_election) %>%
    unnest(c(election_name, district)) %>%
    select(-district_ocd_id) %>%
    mutate(
      # Codes are wrong for Montenegro (MT), Basque Country (BS), and Northern Cyprus (NT)
      # iso3 = countrycode(district_country, origin = "iso2c", destination = "iso3c"),
      iso3 = forcats::fct_relabel(district_name, \(x) name2iso(x)),
      across(contains("date"), as.Date),
      snap = !is.na(is_snap_election),
      election_start_date = election_declared_start_date,
      election_start_date = case_when(is.na(election_start_date) ~ election_range_start_date,
                                      T ~ election_start_date),
      election_end_date = election_declared_end_date,
      election_end_date = case_when(is.na(election_end_date) ~ election_range_end_date,
                                    T ~ election_end_date)) %>%
    select(iso3, district_type, election_start_date, election_end_date, snap, district_type) %>%
    filter(!is.na(election_start_date) | !is.na(election_end_date)) %>%
    rowwise() %>%
    mutate(date = list(election_start_date:election_end_date)) %>%
    ungroup() %>%
    unnest(date) %>%
    mutate(
      yearmon = as.yearmon(as.Date(date)),
      year = lubridate::year(yearmon),
      month = lubridate::month(yearmon),
      election = T,
      exec_election = case_when(district_type == "national_exec" ~ T, T ~ F)) %>%
    distinct(iso3, yearmon, year, month, snap, district_type, election, exec_election)
  
  ifes_monthly <- ifes %>%
    select(-district_type) %>%
    right_join(select(starter, -pop), by = join_by(iso3, yearmon, year, month)) %>%
    filter(iso3 %in% unique(ifes$iso3) & between(yearmon, min(ifes$yearmon), max(ifes$yearmon))) %>%
    arrange(yearmon) %>%
    replace_na(list(snap = F, election = F, exec_election = F)) %>%
    mutate(.by = iso3,
           anticipated = matrixStats::rowAnys(lead_multi(election, 0:6, default = F)),
           exec_anticipated = matrixStats::rowAnys(lead_multi(exec_election, 0:6, default = F)),
           snap_anticipated = matrixStats::rowAnys(lead_multi(snap, 0:6, default = F)),
           exec_snap_anticipated = snap_anticipated & exec_anticipated) %>%
    rename_with(.cols = -c(iso3, yearmon, year, month), ~ paste0("IFES_", .x)) %>%
    summarize(.by = c(iso3, year, month), across(starts_with("IFES_"), ~ max(.x, na.rm = T)))
  write_csv(ifes_monthly, file.path(cm_dir, "ifes.csv"))
}

# inner_join(reign, ifes_monthly) %>%
#   dplyr::count(REIGN_election_now, IFES_election)
#   # { cor(x = .$REIGN_election_anticipated, y = .$IFES_anticipated) }
#   # { cor(x = .$REIGN_election_anticipated, y = .$IFES_exec_anticipated) }
#   # { cor(x = .$REIGN_irregular_election_anticipated, y = .$IFES_snap_anticipated) }

#   pivot_wider(names_from = district_type, values_from = )

# ifes %>% filter(election_declared_start_date != election_range_start_date)

# Add REIGN dataset on election inteference------------------------------------
write_reign_csv <- function() {
  reign <- read_csv(
    paste0("https://raw.githubusercontent.com/OEFDataScience/REIGN.github.io/gh-pages/data_sets/REIGN_2021_8.csv"),
    col_types = cols()) %>%
    select(
      ccode, country, year, month,
      REIGN_delayed_election = delayed,
      REIGN_irregular_election_anticipated = irreg_lead_ant,
      REIGN_election_anticipated = anticipation,
      REIGN_election_now = election_now) %>%
    filter(year >= 2004) %>%
    mutate(iso3 = forcats::fct_relabel(factor(country), \(x) name2iso(x))) %>%
    mutate(iso3 = case_when(
      country == "UKG" ~ "GBR",
      country == "Cen African Rep" ~ "CAF",
      T ~ iso3)) %>%
    select(iso3, year, month, contains("REIGN")) %>%
    mutate(
      iso3 = factor(iso3),
      yearmon = as.yearmon(paste0(year, "-", month))) %>%
    summarize(.by = c(iso3, year, month), across(contains("REIGN"), ~ max(.x, na.rm = T)))
  reign_monthly <- left_join(starter, reign, by = c("iso3", "year", "month", "yearmon")) %>%
    filter(between(yearmon, min(reign$yearmon), max(reign$yearmon))) %>%
    sjmisc::replace_na(contains("REIGN"), value = 0)
  write_csv(reign_monthly, file.path(cm_dir, "reign.csv"))
}

# Add FEWS NET data on Food Insecurity-----------------------------------------
write_fews_csv <- function() {
  # url <- 'https://datacatalogapi.worldbank.org/ddhxext/ResourceView?resource_unique_id=DR0091743'
  # queryString <- list('resource_unique_id' = "DR0091743")
  # response <- VERB("GET", url, query = queryString)
  # metadata <- fromJSON(content(response, "text"))
  # version_date <- as.Date(str_extract(basename(metadata$distribution$url), "20\\d{2}-\\d{1,2}-\\d{1,2}"))
  # filename <- file.path("source-data", paste0("fews-", version_date, ".csv"))
  filename <- "source-data/fews-2024-01-16.csv"
  # curl::curl_download(url = metadata$distribution$url, destfile = filename)
  fews <- read_csv(filename, col_types = cols(.default = "c")) %>%
    mutate(across(.cols = c(admin_code, year, month, contains("fews"), pop), ~ as.numeric(.x))) %>%
    mutate(.by = c(country, year_month), country_pop = sum(pop)) %>%
    mutate(country_pop_proportion = pop/country_pop, pop) %>%
    select(country, admin_code, year, month, contains("fews"), pop, country_pop_proportion)
  # Current insecurity
  fews_proportions <- fews %>%
    select(-contains('fews'), fews_ipc) %>%
    # filter(!is.na(fews_ipc)) %>%
    pivot_wider(names_from = fews_ipc, values_from = country_pop_proportion, names_prefix = "ipc") %>%
    summarize(.by = c(country, year, month), across(.cols = contains("ipc"), ~ sum(.x, na.rm = T))) %>%
    rowwise() %>% 
    mutate(proportion_sum = sum(c_across(contains("ipc")))) %>%
    ungroup()
  # Verify proprotions add up
  stopifnot(abs(1 - fews_proportions$proportion_sum) < 0.01)
  fews_proportions <- fews_proportions %>%
    mutate(iso3 = name2iso(country)) %>%
    rename_with(.cols = contains('ipc'), ~ paste0("FEWS_", .x)) %>%
    select(iso3, year, month, contains("FEWS"))
  
  # 4-month projected insecurity
  fews_proportions_proj_near <- fews %>%
    select(-contains('fews'), fews_proj_near) %>%
    # filter(!is.na(fews_proj_med)) %>%
    pivot_wider(names_from = fews_proj_near, values_from = country_pop_proportion, names_prefix = "ipc") %>%
    rename_with(.cols = contains("ipc"), ~ paste0(.x, "_4month")) %>%
    summarize(.by = c(country, year, month), across(.cols = contains("ipc"), ~ sum(.x, na.rm = T))) %>%
    rowwise() %>% 
    mutate(proportion_sum = sum(c_across(contains("ipc")))) %>%
    ungroup()
  # Verify proprotions add up
  stopifnot(abs(1 - fews_proportions_proj_near$proportion_sum) < 0.01)
  fews_proportions_proj_near <- fews_proportions_proj_near %>%
    mutate(iso3 = name2iso(country)) %>%
    rename_with(.cols = contains('ipc'), ~ paste0("FEWS_", .x)) %>%
    select(iso3, year, month, contains("FEWS"))
  
  # 8-month projected insecurity
  fews_proportions_proj_med <- fews %>%
    select(-contains('fews'), fews_proj_med) %>%
    # filter(!is.na(fews_proj_med)) %>%
    pivot_wider(names_from = fews_proj_med, values_from = country_pop_proportion, names_prefix = "ipc") %>%
    rename_with(.cols = contains("ipc"), ~ paste0(.x, "_8month")) %>%
    summarize(.by = c(country, year, month), across(.cols = contains("ipc"), ~ sum(.x, na.rm = T))) %>%
    rowwise() %>% 
    mutate(proportion_sum = sum(c_across(contains("ipc")))) %>%
    ungroup()
  # Verify proprotions add up
  stopifnot(abs(1 - fews_proportions_proj_med$proportion_sum) < 0.01)
  fews_proportions_proj_med <- fews_proportions_proj_med %>%
    mutate(iso3 = name2iso(country)) %>%
    rename_with(.cols = contains('ipc'), ~ paste0("FEWS_", .x)) %>%
    select(iso3, year, month, contains("FEWS"))
  fews_monthly <- full_join(fews_proportions, fews_proportions_proj_near, by = c("iso3", "year", "month")) %>%
    full_join(fews_proportions_proj_med, by = c("iso3", "year", "month"))
  fews_monthly <- left_join(starter, fews_monthly, by = c("iso3", "year", "month")) %>%
    group_by(iso3) %>% fill(contains("FEWS")) %>% ungroup() %>%
    filter(if_any(contains("FEWS"), ~ !is.na(.x)))
  write_csv(fews_monthly, file.path(cm_dir, "fews.csv"))
}

# Add Food Price Inflation dataset---------------------------------------------
# fpi <- read_csv("source-data/WLD_RTFP_country_2024-01-25.csv") %>%
#   rename(iso3 = ISO3, Food_Price_Inflation = Inflation) %>%
#   mutate(year = year(date),
#   month = month(date)) %>%
#   select(iso3, year, month, Food_Price_Inflation) %>%
#   filter(!is.na(Food_Price_Inflation))
# fpi <- left_join(starter, fpi, by = c("iso3", "year", "month"))

# Add EIU dataset--------------------------------------------------------------
write_eiu_csv <- function() {
  files <- c("source-data/eiu-operational-risk-macroeconomic-2002-2024.csv",
             "source-data/eiu-political-stability-2002-2024.csv",
             "source-data/eiu-security-risk-2002-2024.csv")
  varnames <- c("EIU_macroeconomic_risk", "EIU_political_stability_risk", "EIU_security_risk")
  read_eiu <- function(file, varname) {
    df <- read_csv(file, na = c("", "–", "NA")) %>%
      mutate(iso3 = name2iso(Geography)) %>%
      select(iso3, starts_with("2")) %>%
      pivot_longer(cols = starts_with("20"), names_to = "yearmon", values_to = varname)
  }
  eiu <- map2(files, varnames, read_eiu) %>%
    reduce(\(a, b) full_join(a, b, by = c("iso3", "yearmon"))) %>%
    mutate(.after = iso3,
           year = as.numeric(str_sub(yearmon, 1, 4)),
           month = as.numeric(str_sub(yearmon, -2, -1))) %>%
    select(-yearmon)
  eiu <- left_join(starter, eiu, by = c("iso3", "year", "month")) %>%
    filter(year > 2001) %>%
    fill(contains("EIU"))
  write_csv(eiu, file.path(cm_dir, "eiu.csv"))
}

# Add FSI dataset---------------------------------------------------------------
write_fsi_csv <- function() {
  # fsi <- read_most_recent('hosted-data/fsi', FUN = read_xlsx, as_of = Sys.Date(), return_date = F)
  fsi_files <- list.files("source-data/fsi-historic", full.names = T)
  fsi_files <- setNames(fsi_files, str_extract(fsi_files, "\\d{4}"))
  
  fsi <- fsi_files %>%
    lapply(\(file) {
      df <- read_xlsx(file, col_types = "text") %>%
        select(Country, FSI = Total) %>%
        mutate(
          FSI = as.numeric(FSI),
          year = as.numeric(str_extract(file, "\\d{4}")))
      return(df)
    }) %>%
    bind_rows() %>%
    mutate(iso3 = forcats::fct_relabel(factor(Country), \(x) name2iso(x)), .keep = "unused") %>%
    # Removing West Bank & Gaza because it is lumped in with Israel
    mutate(iso3 = str_replace(iso3, "ISR, PSE", "ISR"))
  
  fsi_monthly <- left_join(fsi, starter, by = c("iso3", "year"))
  write_csv(fsi_monthly, file.path(cm_dir, "fsi.csv"))
}

# Add INFORM Socioeconomic Vulnerability --------------------------------------
write_inform_risk_csv <- function() {
  # Currently INFORM comes out in September for the following year, so INFORM 2024
  # should be predictive of all months in 2024; I don't know, though, when past datasets
  # were made available, or how edits have been made
  inform <- read_csv("/Users/bennotkin/Documents/world-bank/crm/crm-db/output/inputs-archive/inform_risk.csv") %>%
    select(-c(Flood, Rank, `Lack of Reliability (*)`, `Number of Missing Indicators`, `% of Missing Indicators`, `Countries in HVC`, `Recentness data (average years)`)) %>%
    mutate(year = INFORM_Year, month = list(1:12)) %>%
    rename_with(~ paste0("INFORM_", slugify(.x, tolower = F)), .cols = -c(Country, year, month)) %>%
    select(iso3 = Country, year, month, everything()) %>%
    unnest(month)
  write_csv(inform, file.path(cm_dir, "inform-risk.csv"))
}

inform_severity_collect <- function() {
  # Method using INFORM Severity's own site; previous method used acaps.org
  inform_directory <- "source-data/inform-severity"
  
  if (!dir.exists(inform_directory)) dir.create(inform_directory)  
  
  existing_files <- list.files(inform_directory) %>%
    str_replace("^\\d{8}--", "")
  existing_files_misnamed <- list.files(inform_directory) %>%
    subset(!str_detect(., "^\\d{8}"))
  
  urls <- read_html("https://drmkc.jrc.ec.europa.eu/inform-index/INFORM-Severity/Results-and-data") %>%
    html_elements("a") %>%
    html_attr("href") %>%
    { .[str_detect(., "xlsx") & !is.na(.)] } %>%
    { data.frame(url = paste0("https://drmkc.jrc.ec.europa.eu", .))} %>%
    mutate(
      file_name = str_extract(url, "[^/]*?.xlsx"),
      url = str_replace_all(url, " ", "%20")) %>%
    filter(file_name %ni% existing_files | file_name %in% existing_files_misnamed) %>%
    .[nrow(.):1,] %>% # Reverses order
    filter(!is.na(url))
  
  if (nrow(urls) > 0) {
    urls %>% apply(1, function(url) {
      destfile <- file.path(inform_directory, url["file_name"])
      curl_download(url["url"], destfile = destfile)
      if (!str_detect(url["file_name"], "^20\\d{6}")) {
        # Rename file with YYYYMMDD prefix if it doesn't alreay have one
        # Using "--" to signal the prefix is not a part of the original file name
        write_date <- format(as.Date(pull(read_xlsx(destfile, range = "A3", col_names = "date")), format = "%d/%m/%Y"), "%Y%m%d--")
        file.rename(destfile, file.path(inform_directory, paste0(write_date, url["file_name"])))
      }
    })
  }
}
inform_severity_collect()

write_inform_severity_csv <- function() {
  inform_directory <- "source-data/inform-severity"
  inform_severity <- list.files(inform_directory) %>%
    map(\(f) {
      # yearmonth <- readxl::read_xlsx(file.path(inform_directory, f), sheet = "INFORM Severity - country", range = "A1") %>%
      #   names() %>% str_extract("[A-Za-z]* \\d{4}$") %>% as.yearmon()
      date <- as.Date(str_extract(f, "^\\d{8}"), format = "%Y%m%d")
      column_names <- unlist(readxl::read_xlsx(file.path(inform_directory, f), sheet = "INFORM Severity - country",
                                               range = "A2:Z2", col_names = letters)) %>% na.omit()
      severity <- readxl::read_xlsx(file.path(inform_directory, f), sheet = "INFORM Severity - country",
                                    skip = 4, col_names = column_names, na = "x") %>%
        # select(-c(COUNTRY, `Last updated`)) %>%
        mutate(.keep = "unused", update = as.Date(`Last updated`)) %>%
        select(iso3 = ISO3, everything()) %>%
        rename_with(.cols = -iso3, ~ paste0("INFORMSEVERITY_", slugify(.x))) %>%
        setNames(str_replace(names(.), "INFORMSEVERITY_inform_severity_", "INFORMSEVERITY_")) %>%
        mutate(.after = iso3, date = date, yearmon = as.yearmon(date)) %>%
        filter(if_any(contains("INFORM"), ~ !is.na(.x)))
      return(severity)
    }) %>% bind_rows() %>%
    arrange(iso3, yearmon) %>%
    mutate(yearmon = factor(yearmon, levels = as.yearmon(seq(as.Date("2020-10-01"), Sys.Date(), by = "month")))) %>%
    complete(
      iso3, yearmon, explicit = F,
      fill = list(INFORMSEVERITY_index = 0, INFORMSEVERITY_crisis = "None")) %>%
    mutate(.after = iso3, 
           yearmon = as.yearmon(yearmon),
           year = lubridate::year(yearmon),
           month = lubridate::month(yearmon)) %>%
    arrange(iso3, yearmon) %>%
    slice_max(by = c(iso3, yearmon), order_by = date, with_ties = F) %>%
    select(-date)
  write_csv(inform_severity, file.path(cm_dir, "inform-severity.csv"))
}

write_acaps_risklist_csv <- function() {
  risk_list <- read_csv("/Users/bennotkin/Documents/world-bank/crm/crm-db/output/inputs-archive/acaps_risklist.csv") %>%
    mutate(
      yearmon = as.yearmon(last_risk_update),
      risk_level = ordered(risk_level, levels = c(NA, "Low", "Medium", "High"))) %>%
    summarize(.by = c(iso3, yearmon), risk_level = as.numeric(max(risk_level, na.rm = T))) %>%
    filter(!is.na(risk_level)) %>%
    rename_with(.cols = -c(iso3, yearmon), ~ paste0("ACAPS_", .x)) %>%
    mutate(.after = yearmon, year = lubridate::year(yearmon), month = lubridate::month(yearmon))
  write_csv(risk_list, file.path(cm_dir, "acaps-risklist.csv"))
}

# Add CPIA --------------------------------------------------------------------
write_cpia_csv <- function() {
  # For API, see https://api.worldbank.org/v2/sources/31/indicators
  # Most recent data, with XLSX and API 
  cpia <- read_xlsx("source-data/CPIA.xlsx", sheet = "Data",
                    na = c("NA", "", "..")) %>%
    filter(`Series Name` == "IDA resource allocation index (1=low to 6=high)") %>%
    select(iso3 = `Country Code`, matches("\\d.*")) %>%
    pivot_longer(cols = -iso3, names_to = "year", values_to = "CPIA_IRA") %>%
    mutate(year = as.numeric(str_extract(year, "^\\d{4}"))) %>%
    mutate(month = paste(1:12, collapse = ",")) %>%
    separate_longer_delim(month, delim = ",") %>%
    filter(!is.na(CPIA_IRA))
  write_csv(cpia, file.path(cm_dir, "cpia.csv"))
}

# Add EM-DAT on natural hazards------------------------------------------------
write_emdat_csv <- function() {
  emdat_full <- read_xlsx("source-data/EM-DAT.xlsx",
                          sheet = 1, range = "A1:AT12835", na = "", col_types = "text")
  emdat <- emdat_full %>%
    filter(`End Year` > 2000) %>%
    mutate(
      .keep = "none",
      disno = `DisNo.`, iso3 = ISO,
      disaster_group = factor(`Disaster Group`),
      disaster_subgroup = factor(`Disaster Subgroup`),
      disaster_type = factor(`Disaster Type`),
      disaster_subtype = factor(`Disaster Subtype`),
      start_year = `Start Year`, end_year = `End Year`,
      start_month = `Start Month`, end_month = `End Month`,
      # !! FILLING IN MISSING DATES. Is this appropriate?
      start_month = case_when(is.na(start_month) ~ "01", T ~ start_month),
      end_month = case_when(is.na(end_month) ~ "12", T ~ end_month),
      start_day = `Start Day`, end_day = `End Day`, 
      start_day = case_when(is.na(start_day) ~ "1", T ~ start_day),
      end_day = case_when(is.na(end_day) ~ "28", T ~ end_day),
      affected = `Total Affected`, deaths = `Total Deaths`, damage = `Total Damage, Adjusted ('000 US$)`,
      declaration = Declaration == "Yes",
      across(.cols = c(starts_with("start", ignore.case = F), starts_with("end", ignore.case = F), affected, deaths, damage), ~ as.numeric(.x)),
      start_date = as.Date(paste(start_year, start_month, start_day, sep = "-")),
      end_date = as.Date(paste(end_year, end_month, end_day, sep = "-")))
  if (nrow(filter(emdat, end_date < start_date)) > 0) stop("End dates earlier than start dates")
  emdat <- emdat %>%
    group_by(disno, iso3) %>%
    mutate(
      duration_days = as.numeric(end_date - start_date),
      dates = list(start_date:end_date)) %>%
    unnest(dates) %>%
    mutate(date = as.Date(dates), .keep = "unused")
  emdat_effect <- emdat %>%
    select(-disaster_group) %>%
    mutate(across(c(affected, deaths, damage), ~ .x/head(duration_days, n = 1))) %>%
    ungroup() %>%
    mutate(
      year = lubridate::year(date),
      month = lubridate::month(date)) %>%
    summarize(
      .by = c(iso3, year, month),
      across(c(affected, deaths, damage), ~ sum(.x, na.rm = T)),
      disaster_days = n(),
      disasters = length(unique(disno))) %>%
    rename_with(~ paste0("EMDAT_", .x), .cols = -c(iso3, year, month))
  # emdat_events <- emdat %>%
  #   mutate(
  #     year = lubridate::year(date),
  #     month = lubridate::month(date)) %>%
  #   select(disno, iso3, disaster_type, year, month) %>%
  #   distinct() %>%
  #   mutate(event = 1) %>%
  #   pivot_wider(names_from = disaster_type, values_from = event, names_prefix = "EMDAT_") %>%
  #   setNames(nm = slugify(names(.), tolower = F)) %>%
  #   ungroup() %>%
  #   group_by(iso3, year, month) %>%
  #   summarize(across(contains("EMDAT"), ~ sum(.x, na.rm = T)))
  emdat_declarations <- emdat %>%
    ungroup() %>%
    mutate(
      year = lubridate::year(date),
      month = lubridate::month(date)) %>%
    select(disno, iso3, year, month, declaration) %>% 
    distinct() %>%
    summarize(.by = c("iso3", "year", "month"), EMDAT_declarations = sum(declaration))
  emdat <- full_join(emdat_effect, emdat_declarations, by = c("iso3", "year", "month"))
  # emdat_monthly <- left_join(starter, emdat, by = c("iso3", "year", "month"))
  # emdat_monthly %>% summary()
  write_csv(emdat, file.path(cm_dir, "emdat.csv"))
}

# Add V-DEM--------------------------------------------------------------------
write_vdem_csv <- function() {
  v_dem <- vdemdata::vdem %>%
    as_tibble() %>%
    filter(year >= 2000) %>%
    select(
      iso3 = country_text_id, year,
      VDEM_electoral = v2x_polyarchy, VDEM_liberal = v2x_libdem, VDEM_participatory = v2x_partipdem,
      VDEM_deliberative = v2x_delibdem, VDEM_egalitarian = v2x_egaldem) %>%
    # Make sure only one country-year row each
    # count(iso3, year) %>% filter(n != 1)
    mutate(month = paste(1:12, collapse = ",")) %>%
    separate_longer_delim(month, delim = ",")
  write_csv(v_dem, file.path(cm_dir, "vdem.csv"))
}

# Add GDP data ----------------------------------------------------------------
write_gdp_csv <- function() {
  gdp <- read_xls("source-data/GDP per Capita/GDP per capita, PPP (current international $).xls", skip = 3) %>%
    select(-starts_with('1')) %>%
    mutate(across(1:4, ~ as.factor(.x))) %>%
    select(iso3 = `Country Code`, starts_with('2')) %>%
    pivot_longer(cols = -iso3, names_to = "year", values_to = "WBG_GDP_PPP") %>%
    mutate(month = paste(1:12, collapse = ",")) %>%
    separate_longer_delim(month, delim = ",")
  write_csv(gdp, file.path(cm_dir, "wbg-gdp.csv"))
}

# Add CPI Inflation data ------------------------------------------------------
write_cpi_csv <- function() {
  cpi <- read_xlsx("source-data/Inflation-data.xlsx",
                   sheet = "hcpi_m", range = "R1C1:R189C644") %>%
    # get_col_types_short(cpi, collapse = F) %>% subset(. != "d")
    mutate(.keep = "none", .before = 1,
           iso3 = `Country Code`,
           across(starts_with("2"), ~ as.numeric(.x))) %>%
    pivot_longer(cols = starts_with("2"), names_to = "yearmon", values_to = "WBG_CPI") %>%
    mutate(
      year = as.numeric(str_sub(yearmon, 1, 4)),
      month = as.numeric(str_sub(yearmon, -2, -1))) %>%
    select(-yearmon)
  write_csv(cpi, file.path(cm_dir, "cpi.csv"))
}

# Add WBG Worldwide Governance Indicator---------------------------------------
write_wgi_csv <- function() {
  wgi <- 2:7 %>%
    lapply(function(s) {
      name <- read_xlsx("source-data/wgidataset.xlsx",
                        sheet = s, range = "A1:A1", col_names = F) %>% pull()
      sheet <- read_xlsx("source-data/wgidataset.xlsx",
                         sheet = s, skip = 13, col_types = "text", na = c("", "#N/A"))
      estimate <- sheet[-c(1,2), c(2, which(sheet[1,] == "Estimate"))] %>%
        rename(iso3 = `...2`) %>%
        pivot_longer(cols = -1, names_to = "year", values_to = "value") %>%
        mutate(
          indicator = paste("WGI", slugify(name), sep = "_"),
          year = as.numeric(str_extract(year, "^\\d{4}")),
          month = paste(1:12, collapse = ",")) %>%
        separate_longer_delim(month, delim = ",")
      return(estimate)
      # rank <- sheet[-c(1,2), c(sheet[1,] == "Rank")]
    }) %>%
    bind_rows() %>%
    mutate(value = as.numeric(value)) %>%
    pivot_wider(names_from = "indicator", values_from = "value")
  write_csv(wgi, file.path(cm_dir, "wgi.csv"))
}

# Add UNDP Gender--------------------------------------------------------------
write_gender_inequality_csv <- function() {
  gii <- read_csv("source-data/UNDP_historic dataset (composite, see GII).csv") %>% 
    select(iso3, matches("gii_\\d{4}")) %>%
    pivot_longer(cols = -iso3, names_to = "year", values_to = "UNDP_GII") %>%
    filter(!is.na(UNDP_GII)) %>%
    mutate(
      year = as.numeric(str_extract(year, "\\d{4}")),
      month = paste(1:12, collapse = ","),
      UNDP_GII = as.numeric(UNDP_GII)) %>%
    separate_longer_delim(month, delim = ",")
  write_csv(gii, file.path(cm_dir, "undp-gii.csv"))
}

# Add IDMC Forced displacement-------------------------------------------------
write_idmc_csv <- function() {
  idmc <- read_xlsx("source-data/Displacement (IDMC & UNHCR)/IDMC_Internal_Displacement_Conflict-Violence_Disasters 2008-2022.xlsx") %>%
    select(
      iso3 = ISO3, year = Year,
      IDMC_IDPs_conflict = `Conflict Stock Displacement (Raw)`,
      IDMC_IDPs_disaster = `Disaster Stock Displacement (Raw)`,
      IDMC_ID_movements_conflict = `Conflict Internal Displacements (Raw)`,
      IDMC_ID_movements_disaster = `Disaster Internal Displacements (Raw)`) %>%
    rowwise() %>%
    mutate(
      IDMC_IDPs_combined = sum(IDMC_IDPs_conflict, IDMC_IDPs_disaster, na.rm = T),
      IDMC_ID_movements_combined = sum(IDMC_ID_movements_conflict, IDMC_ID_movements_disaster, na.rm = T),
      month = paste(1:12, collapse = ",")) %>%
    ungroup() %>%
    sjmisc::replace_na(contains("IDMC"), value = 0) %>%
    separate_longer_delim(month, delim = ",") %>%
    mutate(month = as.numeric(month), .after = year)
  
  # The above is the validated data, but it ends in 2022; this scrapes the latest,
  # unvalidated data (past 180 days). Note that this shows internal displacements
  # (movements) rather than IDPs
  
  # Make sure there aren't multiple pages I should be drawing from!
  idmc_latest_json <- request("https://helix-tools-api.idmcdb.org/external-api/idus/last-180-days/?client_id=IDMCWSHSOLO009") %>%
    req_headers(Accept = "application/json") %>%
    req_perform() %>%
    resp_body_json()
  
  idmc_latest_date_spans <- idmc_latest_json %>%
    map(\(event) as_tibble(discard(event, is.null))) %>%
    bind_rows() %>%
    # select(-c(latitude, longitude, centroid, standard_info_text)) %>%
    select(iso3, displacement_type, qualifier, figure, contains("date")) %>%
    # filter(displacement_start_date != displacement_end_date) %>%
    rowwise() %>%
    mutate(
      across(contains("date"), ~ as.Date(.x)),
      date = list(displacement_start_date:displacement_end_date),
      days = length(date),
      daily_movements = figure/days) %>%
    ungroup()
  idmc_latest <- idmc_latest_date_spans %>%
    unnest(cols = date) %>%
    mutate(
      date = as.Date(date),
      yearmon = as.yearmon(date)) %>%
    # year = lubridate::year(date),
    # month = lubridate::month(date)) %>%
    # Filtering out all months that ended before the data's 180-day window
    filter(yearmon >= as.yearmon(min(idmc_latest_date_spans$displacement_end_date))) %>%
    summarize(
      .by = c(displacement_type, iso3, yearmon),
      movements = sum(daily_movements)) %>%
    pivot_wider(names_from = displacement_type, values_from = movements) %>%
    rename(
      IDMC_ID_movements_conflict = Conflict,
      IDMC_ID_movements_disaster = Disaster) %>%
    complete(iso3, yearmon) %>%
    sjmisc::replace_na(contains("IDMC"), value = 0) %>%
    mutate(
      year = lubridate::year(yearmon),
      month = lubridate::month(yearmon),
      IDMC_ID_movements_combined = IDMC_ID_movements_conflict + IDMC_ID_movements_disaster) %>%
    select(-yearmon)
  
  idmc_both <- bind_rows(
    mutate(idmc, IDMC_verified = T),
    mutate(idmc_latest, IDMC_verified = F))
  write_csv(idmc_both, file.path(cm_dir, "idmc.csv"))
}

# See scratchheap.R for plots comparing verified and candidate data

# Add IMF Social Unrest--------------------------------------------------------
write_imf_rsui_csv <- function() {
  most_recent_dir <- read_most_recent(directory_path = "source-data/imf-reported-social-unrest-index",
                                      FUN = paste, as_of = Sys.Date())
  rsui_a <- read_csv(file.path(most_recent_dir, "rsui_headline_long.csv"), col_types = cols("D", .default = "d")) %>%
    filter(year > 1985) %>%
    mutate(month = lubridate::month(Date), .keep = "unused") %>%
    pivot_longer(cols = -c(year, month), names_to = "iso3", values_to = "IMF_rsui_a")
  rsui_details <- read_csv(file.path(most_recent_dir, "rsui_event_details.csv"), col_types = "ffDfillllclccl") %>%
    mutate(month = lubridate::month(Date)) %>%
    select(
      iso3 = cty, year, month,
      IMF_rsui_event = rsui.event,
      IMF_rsui_criteria_2a = event.2.a,
      IMF_rsui_criteria_2b = event.2.b,
      IMF_rsui_criteria_2c = event.2.c,
      IMF_rsui_criteria_3 = event.3) %>%
    distinct()
  imf_rsui <- full_join(rsui_a, rsui_details, by = c("iso3", "year", "month")) %>%
    sjmisc::replace_na(matches("criteria|event"), value = FALSE) %>%
    mutate(iso3 = case_when(
      iso3 == "KOS" ~ "XKX",
      # Causing duplicates, need to think through
      #   iso3 == "CHK" ~ "CHN",
      #   iso3 == "HKC" ~ "HKG",
      T ~ iso3))
  write_csv(imf_rsui, file.path(cm_dir, "imf-rsui.csv"))
}

# ADD SPEI --------------------------------------------------------------------
write_spei_csv <- function() {
  spei <- read_csv("source-data/df_spei-world_1990-2022.csv") %>%
    setNames(str_replace_all(names(.), "-+|\\.", "_")) %>%
    mutate(
      year = lubridate::year(date),
      month = lubridate::month(date), .keep = "unused")
  # Make sure all years and all months appear in dataset; they do
  stopifnot("Not all years 2000-2022 appear in dataset" = length(which_not(2000:2022, spei$year)) == 0)
  stopifnot("Not all months appear in dataset" = length(which_not(1:12, spei$month)) == 0)
  spei <- complete(spei, iso3, year, month)
  write_csv(spei, file.path(cm_dir, "spei.csv"))
}

# WBG natural resource rents
write_natural_resource_rents_csv <- function() {
  resource_rents_request <- request("http://api.worldbank.org/v2/country/all/indicator/NY.GDP.TOTL.RT.ZS") %>%
    req_url_query(format = "json") %>%
    req_headers(Accept = "application/json") %>%
    req_url_query(per_page = 100)
  
  resource_rents_response <- resource_rents_request %>%
    req_throttle(10) %>%
    req_perform_iterative(iterate_with_offset("page"), max_reqs = 300)
  
  WBG_resource_rents <- resps_data(resource_rents_response, \(i) {
    resp_body_json(i)[[2]] %>%
      map(\(j) as_tibble(discard(j, is.null)))
  }) %>%
    bind_rows() %>% 
    select(iso3 = countryiso3code, year = date, WDI_nat_resource_rents = value) %>%
    filter(!is.na(WDI_nat_resource_rents)) %>%
    distinct() %>%
    mutate(month = paste(1:12, collapse = ",")) %>%
    separate_longer_delim(month, delim = ",") %>%
    mutate(month = as.numeric(month)) %>%
    filter(iso3 %in% country_list$Code)
  write_csv(WBG_resource_rents, file.path(cm_dir, "wbg-resource-rents.csv"))
}

# ETH Zurich's Ethnic Power Relations (ETH)
# Only available until 2021
write_epr_csv <- function() {
  epr_raw <- read_csv("https://icr.ethz.ch/data/epr/core/EPR-2021.csv", col_types = c("ifiicddddfl"))
  epr <- epr_raw %>%
    filter(to > 1999) %>%
    summarize(.by = c(gwid, statename, from, to, status), size = sum(size, na.rm = T)) %>%
    pivot_wider(names_from = status, values_from = size) %>%
    sjmisc::replace_na(everything(), value = 0) %>%
    mutate(`STATE COLLAPSE` = `STATE COLLAPSE` != 0) %>%
    rowwise() %>%
    mutate(.keep = "unused",
           iso3 = countrycode(gwid, origin = "gwn", destination = "iso3c", custom_match = c("345" = "SRB", "347" = "XKX", "678" = "YEM", "816" = "VNM")),
           year = list(from:to)) %>%
    select(-statename) %>%
    ungroup() %>%
    unnest(year) %>%
    # !!! Should probably rethink this `distinct()`.
    # Including because I am coding Yugoslavia 2000-2006 as Serbia, and Serbia 
    # also has entry 2006-2008
    distinct(iso3, year, .keep_all = T) %>%
    mutate(month = list(1:12)) %>% 
    unnest(month) %>%
    rename_with(.cols = -c(iso3, year, month), ~ paste0("EPR_", slugify(.x)))
  write_csv(epr, file.path(cm_dir, "eth-epr.csv"))
}

# CrisisWatch, from web not PDFs
write_crisiswatch_csv <- function() {
  file <- "source-data/cw-pages-list.RDS"
  if (!file.exists(file) | run_cw) {
    from_year <- 2000
    from_month <- 1
  } else {
    cw_pages <- readRDS(file)
    latest_yearmon <- max(as.yearmon(unique(unlist(map(cw_pages, \(p) p$entry_month)))))
    from_year <- year(latest_yearmon)
    from_month <- month(latest_yearmon)
  }

  to_year <- year(Sys.Date())
  to_month <- month(Sys.Date())

    read_cw_page <- function(page) {
      country_nodes <- page %>%
        html_elements("div.c-crisiswatch-entry")
      outlist <- country_nodes %>% map(\(x) {
        country_string <- x %>% html_elements("h3") %>% html_text()
        country_name <- str_replace_all(country_string, c("^[\\s\\n]*" = "", "[\\s\\n]*$" = ""))
        if (length(country_string) != 1) stop(paste("Wrong number of country names:", country_name, collapse = "; "))
        entry_month <- x %>% html_elements("time") %>% html_text()
        last_5_months <- x %>% html_elements(".o-state-entry")
        months <- last_5_months %>% html_attr("title")
        # states <- last_5_months %>% html_element("a>span:first-of-type") %>% html_attr("class")
        states <- 1:5 %>% 
          map(\(i) html_attr(html_element(last_5_months, glue("a>span:nth-of-type({i})")), "class")) %>%
          unlist() %>% matrix(ncol = 5) %>% apply(1, \(mo) {paste(na.omit(mo), collapse = ", ")})
        if (length(states) != length(months)) stop(glue("Length of states and months do not match for {country_name}"))
        month_states <- tibble(country = country_name, entry_month = entry_month, month = months, status = states)
        text <- x %>% html_elements(".o-crisis-states__detail") %>%
          html_elements("p") %>% html_text()
        return(list(name = country_name, entry_month = entry_month, past5 = month_states, text = text))
      })
      return(outlist)
    }
    
  base_url <- glue("https://www.crisisgroup.org/crisiswatch/database?crisis_state=&created=custom&from_month={from_month}&from_year={from_year}&to_month={to_month}&to_year={to_year}&page=")
  page0 <- read_html(paste0(base_url, 0))
    cw_page0 <- read_cw_page(page0)
    total_pages <- (page0 %>% html_elements(".o-pagination") %>% html_elements("span") %>% html_text() %>%
                      str_extract("\\d+ of (\\d+)", group = T) %>% na.omit() %>% as.numeric()) - 1
    
    progress_update <- function(current, total, start_time) { 
      duration <- Sys.time() - start_time 
      percentage <- current/total
      expected_duration <- duration / percentage
      remaining <- expected_duration - duration
      end_time <-  start_time + expected_duration
      print(glue("Reading page {current} of {total}; {round(duration, 2)} {units(duration)} elapsed; {round(remaining, 2)} {units(remaining)} remaining ({end_time})"))
    }
    
    start_time <- Sys.time()
  cw_pages_new <- vector(mode = "list", length = total_pages + 1)
  cw_pages_new[[1]] <- cw_page0
    for (i in 1:total_pages) {
      if (i %% 10 == 0) progress_update(i, total_pages, start_time)
    url <- paste0(base_url, i)
      page <- read_html(url)
    cw_pages_new[[i + 1]] <- read_cw_page(page)
    }
  cw_pages_new <- unlist(cw_pages_new, recursive = F)
  names(cw_pages_new) <- unlist(map(cw_pages_new, \(x) paste(x$name, x$entry_month, sep = " - ")))
  cw_pages <- if (exists("cw_pages")) c(cw_pages_new, cw_pages) else cw_pages_new
  cw_pages <- cw_pages[which(!duplicated(names(cw_pages)))]
  saveRDS(cw_pages, "source-data/cw-pages-list.RDS")
  
  cw_df <- map(cw_pages, ~ .x$past5) %>%
    bind_rows() %>%
    arrange(-row_number()) %>%
    distinct(pick(-entry_month), .keep_all = T)
  if (any(count(cw_df, country, month)$n > 1)) stop("More than one entry per country-month")
  cw_df <- cw_df %>%
    mutate(
      conflict_alert = str_detect(status, "risk-alert"),
      resolution_opportunity = str_detect(status, "resolution"),
      status = str_extract(status, "state-(unchanged|deteriorated|improved)") %>%
        factor(levels = c("state-improved", "state-unchanged", "state-deteriorated")),
      risk = as.numeric(status) - 2,
      country = factor(country, levels = sort(unique(country))),
      iso3 = forcats::fct_relabel(country, \(x) name2iso(x)),
      across(contains("month"), ~ as.yearmon(.x)),
      year = lubridate::year(month),
      month = lubridate::month(month)
    ) %>%
    mutate(iso3 = case_when(
      country == "Northern Ireland (UK)" ~ "GBR",
      country == "Corsica" ~ "FRA",
      country == "Nile Waters" ~ "ETH, EGY, SDN",
      # Need to add countries for 'Central Africa', 'Eastern Mediterranean', 'Gulf and Arabian Peninsula'
      T ~ iso3)) %>%
    separate_longer_delim(iso3, ", ")
  
  crisis_watch <- cw_df %>%
    select(iso3, year, month, CW_risk = risk, CW_alert = conflict_alert, CW_resolution_opportunity = resolution_opportunity) %>%
    summarize(.by = c(iso3, year, month), across(starts_with("CW"), ~ max(.x, na.rm = T)))
  
  cw_text <- map(cw_pages, \(x) x$text)
  cw_text %>% yaml::write_yaml("crisiswatch-text.yml")
  
  cw_text_df <- cw_pages %>%
    keep_at(\(x) str_detect(x, "2024|2023")) %>%
    map(\(x) {
      tibble(
        Location = x$name,
        month = as.yearmon(x$entry_month),
        text = paste(x$text, collapse = "\n"))
    }) %>%
    bind_rows() %>%
    slice_max(month, by = Location) %>%
    mutate(iso3 = name2iso(Location), .before = 1) %>%
    separate_longer_delim(iso3, delim = ", ") %>%
    mutate(text = paste(month, text, sep = ": ")) %>%
    summarize(.by = c(Location, iso3, month), text = paste(text, collapse = "\n\n"))
  write_csv(cw_text_df, "crisiswatch-text.csv")
  write_csv(crisis_watch, file.path(cm_dir, "icg-crisiswatch.csv"))
}

# country_regex <- codelist$country.name.en.regex %>% {paste0("(", ., ")")} %>% paste(collapse = "|")

# Believe this is for finding the countries being described in multi-country listings, such as "Nile Waters"
# This doesn't work with multi-word country names
identify_country_groups <- function(country_group) {
  cw_text %>% keep_at(\(x) str_detect(x, country_group)) %>%
    unlist() %>% 
    str_split(boundary("word")) %>% unlist() %>%
    str_subset("^[A-Z]") %>%
    # str_split(" (?=[a-z])") %>%
    str_c(collapse = " ") %>%
    tolower() %>%
    head() %>%
    str_match_all(country_regex) %>%
    # str_match(codelist$country.name.en.regex) %>% 
    .[[1]] %>% . [!is.na(.[,1]),]
  unlist() %>%
    head()
  tibble(text = .) %>%
    count(text) %>%
    mutate(iso = name2iso(text)) %>%
    arrange(desc(n)) #%>%
  # { setNames(.$iso, .$n) }
  # head() %>%
  # dim()
  # (\(mat) {colnames(mat) <- codelist$country.name.en; return(mat)})()
  # setNames(codelist$country.name.en.regex)
}
# identify_country_groups("Nile Waters")

# Evacuations
write_evacuations_csv <- function() {
  # For creating evac-post2015.csv (already done)
  # evac_pdf <- pdf_text("/Users/bennotkin/Downloads/Select Evacuations Through 2024.pdf")
  # rows <- evac_pdf %>%
  #   lapply(\(page) {
  #     page %>% 
  #       str_split_1("\\n") %>%
  #       str_split_fixed("\\s\\s+", n = 10) %>%
  #       as_tibble() %>%
  #       rename(location = 1, action = 2, start_date = 3, length = 4, individuals = 5) %>%
  #       filter(location != "")
  #   }) %>% bind_rows() %>%
  #     filter(action == "Full Emergency Evacuation") %>%
  #     mutate(
  #       country = str_extract(location, "(?<=,\\s).*$"),
  #       iso3 = name2iso(country),
  #       start_date = mdy(start_date),
  #       year = lubridate::year(start_date),
  #       month = lubridate::month(start_date),
  #       evacuation = 1,
  #       .keep = "none"
  #     ) %>% select(-1)
  # write_csv(rows, "source-data/evac-post2015.csv")
  evacuations <- bind_rows(
    read_csv("source-data/evac-post2015.csv", col_types = "ccddd"),
    read_csv("source-data/evac-pre2015.csv", col_types = "cddd") %>%
      mutate(iso3 = name2iso(country))) %>%
    select(-country) %>%
    right_join(select(starter, -pop), by = join_by(iso3, year, month)) %>%
    replace_na(list(evacuation = 0)) %>%
    rename(WBG_evacuation = evacuation)
  write_csv(evacuations, file.path(cm_dir, "wbg-evacuations.csv"))
}

# POLECAT
write_polecat_csv <- function() {
  # How do we want to aggregate Event Intensity when there are multiple events in a given month?
  # Do we want to treat all involved countries the same (actor, recipient, location)? I currently do.
  
  # Column names
  # Event ID        Primary Actor Sector     Recipient Title        GeoNames ID        
  # Event Date      Actor Sectors            Recipient Name Raw     Raw Placename      
  # Event Type      Actor Title              Wikipedia Recipient ID Feature Type       
  # Event Mode      Actor Name Raw           Placename              Source             
  # Event Intensity Wikipedia Actor ID       City                   Publication Date   
  # Quad Code       Recipient Name           District               Story People       
  # Contexts        Recipient Country        Province               Story Organizations
  # Actor Name      Recipient COW            Country                Story Locations    
  # Actor Country   Primary Recipient Sector Latitude               Language           
  # Actor COW       Recipient Sectors        Longitude              Version  
  
  if (run_polecat) {
    separate_into_involved_countries <- function(df) {
      df %>%
        rowwise() %>%
        mutate(involved_cow = paste(actor_cow, recipient_cow, sep = "; ")) %>%
        separate_longer_delim(involved_cow, "; ") %>%
        ungroup() %>%
        filter(involved_cow != "None") %>%
        mutate(
          involved_cow = factor(involved_cow),
          involved_country_iso = forcats::fct_relabel(involved_cow, \(x) {
            countrycode(as.numeric(x), origin = "cown", destination = "iso3c",
                        custom_match = c("260" = "GER", "817" = "VNM"))
          })) %>%
        rowwise() %>%
        mutate(involved_country_iso = paste(country, involved_country_iso, sep = "; ")) %>%
        ungroup() %>%
        separate_longer_delim(involved_country_iso, delim = "; ") %>%
        distinct() %>%
        filter(involved_country_iso != "NA")
    }
    aggregate_by_country <- function(df) {
      df %>%
        mutate(yearmon = as.yearmon(event_date)) %>%
        summarize(.by = c(yearmon, involved_country_iso),
                  most_negative = min(event_intensity, na.rm = T),
                  median_intensity = median(event_intensity, na.rm = T),
                  intensity_sum = sum(event_intensity, na.rm = T),
                  positive_events_count = sum(event_intensity > 0, na.rm = T),
                  negative_events_count = sum(event_intensity < 0, na.rm = T),
                  material_conflict_count = sum(quad_code == "MATERIAL CONFLICT", na.rm = T),
                  verbal_conflict_count = sum(quad_code == "VERBAL CONFLICT", na.rm = T),
                  verbal_conflict_intensity_sum = sum((quad_code == "VERBAL CONFLICT") * event_intensity))
    }
    
    files <- list.files("source-data/polecat.nosync") %>% str_subset("txt$") %>% str_subset("sDV", negate = T) %>% sort()
    
    pc_headers <- file.path("source-data/polecat.nosync", files) %>%
      map(\(x) names(read_tsv(x, n_max = 1))) %>%
      setNames(files)
    pc_headers[1] <- list(setNames(pc_headers[1], "union"))
    header_check <- pc_headers %>%
      accumulate(\(a, b) {
        diff <- which_not(a$union, b, both = T)
        return(list(union = union(a$union, b), diff = diff[lengths(diff) > 0]))
      })
    header_check %>% map(\(x) x$diff)
    
    pc <- file.path("source-data/polecat.nosync", files) %>%
      map(\(x) {
        print(x)
        df <- rename(read_tsv(x, col_types = cols(.default = "c")), any_of(c(`Event Intensity` = 'Intensity'))) %>%
          select(-any_of(c("Headline", "Event Text", "Feed", "Story ID")))
        return(df)
      }) %>% bind_rows() %>% setNames(slugify(names(.))) %>%
      select(event_date, quad_code, event_type, event_mode, event_intensity, actor_country, actor_cow, recipient_country, recipient_cow, country) %>%
      mutate(
        event_date = as.Date(event_date),
        event_intensity = as.numeric(event_intensity)
      )
    
    polecat_2023_24 <-  pc %>%
      separate_into_involved_countries() %>%
      aggregate_by_country()
    
    files_archive <- list.files("source-data/polecat.nosync") %>% str_subset("txt$") %>% str_subset("sDV") %>% sort()
    pc_archive <- vector(mode = "list", length = length(files_archive))
    for (i in seq_along(files_archive)) {
      x <-  file.path("source-data/polecat.nosync", files_archive)[i]
      print(x)
      pc_archive[[i]] <- rename(read_tsv(x, col_types = cols(.default = "c")), any_of(c(`Event Intensity` = 'Intensity'))) %>%
        select(-any_of(c("Headline", "Event Text", "Feed", "Story ID"))) %>%
        setNames(slugify(names(.))) %>%
        select(event_date, quad_code, event_type, event_mode, event_intensity, actor_country, actor_cow, recipient_country, recipient_cow, country) %>%
        mutate(
          event_date = as.Date(event_date),
          event_intensity = as.numeric(event_intensity)
        ) %>%
        separate_into_involved_countries() %>%
        aggregate_by_country()
    }
    
    pc_archive_df <- pc_archive %>% bind_rows()
    
    polecat <- bind_rows(pc_archive_df, filter(polecat_2023_24, yearmon >= "Jan 2023")) %>%
      arrange(yearmon) %>%
      mutate(involved_country_iso = factor(involved_country_iso)) %>%
      filter(involved_country_iso != "None") %>%
      mutate(
        # .keep = "unused", 
        .before = 1,
        iso3 = involved_country_iso,
        year = lubridate::year(yearmon),
        month = lubridate::month(yearmon)) %>%
      select(-involved_country_iso)
    
    write_csv(polecat, "source-data/polecat.csv") 
  } else {
    polecat <- read_csv("source-data/polecat.csv") %>% select(-any_of("involved_country_iso"))
  }
  
  pc_firstmonth <- polecat %>% select(yearmon, year, month) %>% slice_min(yearmon, with_ties = F)
  pc_lastmonth <- polecat %>% select(yearmon, year, month) %>% slice_max(yearmon, with_ties = F)
  
  polecat <- polecat %>% right_join(filter(select(starter, -pop, -yearmon), year >= 2018), by = join_by(iso3, year, month)) %>%
    arrange(year, month, iso3) %>%
    filter(
      !(year == pc_firstmonth$year & month < pc_firstmonth$month),
      !(year == pc_lastmonth$year & month > pc_lastmonth$month)) %>%
    select(-yearmon) %>%
    rename_with(.cols = -c(iso3, year, month), ~ paste0("POLECAT_", .x)) %>%
    sjmisc::replace_na(starts_with("POLECAT"), value = 0)
  write_csv(polecat, file.path(cm_dir, "polecat.csv"))
}


# POLECAT & ICEWS from Mathijs
write_polecat_icews_csv <- function() {
  polecat2 <- read_csv("source-data/icews_and_polecat.csv") %>%
    rename(iso3 = country) %>%
    select(-date)
  write_csv(polecat2, file.path(cm_dir, "polecat-icews.csv"))
  
  # ICEWS by itself
  icews <- read_csv("/Users/bennotkin/Downloads/FCV_training_dataset_with_conflictforecast_data_and_icews.csv") %>%
    select(iso3, year, month, contains("ICEWS")) %>%
    filter(if_any(-c(iso3, year, month), ~ !is.na(.x)))
  # write_csv(icews, "source-data/icews.csv")
  # icews <- read_csv("source-data/icews.csv", col_types = "cdddddddddddddddddddddd")
  write_csv(icews, file.path(cm_dir, "icews.csv"))
}

# Conflictforecast.org
write_conflictforecast_csv <- function() {
  cf_dir <- "source-data/conflict-forecast.nosync"
  dir.create(cf_dir)
  # read_most_recent("source-data/conflict-forecast", paste, as_of = Sys.Date(), return_date = T)
  arch_url <- "http://api.backendless.com/C177D0DC-B3D5-818C-FF1E-1CC11BC69600/C5F2917E-C2F6-4F7D-9063-69555274134E/services/fileService/"
  public_urls <- request(arch_url) %>%
    # req_url_path_append("get-file-listing") %>%
    req_url_path_append("get-all-directories") %>%
    # req_method("GET") %>%
    req_url_query(date = "latest") %>%
    req_headers(accept = "application/json") %>%
    req_perform()
  
  # latest_month <- max(as.yearmon(list.files("source-data/conflict-forecast.nosync"), format = "%m-%Y"))
  
  public_urls %>% resp_body_json() %>%
    # This prevents redownloading, which we may actually want
    discard(\(x) x$name %in% list.files(cf_dir)) %>%
    map(\(x) {
      dir <- file.path(cf_dir, x$name)
      dir.create(dir)
      resp <- req_perform(request(x$publicUrl))
      resp_body_json(resp) %>%
        map(\(y) {
          curl_download(y$publicUrl, file.path(dir, y$name))
        })
      # writeLines(x$updatedOn, file.path(dir, "last-updated"))
    })
  
  # # WARNING Need to replace old names with new names, but it depends on which file I'm reading
  # new_names <- c(
  #         "period" = "year-month",
  #         "fatalities_ucdp" = "best",
  #         "population" = "populationwb",
  #         "ons_anyviolence_03_target" = "ons_anyviolence3", 
  #         "ons_anyviolence_03_text" = "text_model",
  #         "ons_anyviolence_03_all" = "best_model",
  #         "ons_anyviolence_12_target" = "ons_anyviolence12",
  #         "ons_anyviolence_12_text" = "text_model",
  #         "ons_anyviolence_12_all" = "best_model",
  #         "ons_armedconf_03_target" = "ons_armedconf3",
  
  #         )
  
  # Reads all past files
  # armed_conflict_12 <- list.files(cf_dir, recursive = T) %>% str_subset("armedconf_12") %>%
  #   file.path(cf_dir, .) %>%
  #   .[1:3] %>%
  #   map(\(file) {
  #     read_csv(file, col_types = c("f", .default = "c")) #%>%
  #       # rename(any_of(new_names))
  #     }) %>%
  #   reduce(\(a, b) {
  #     print(glue("Missing columns: {setdiff(names(a), names(b))}", collapse = ", "))
  #     # print(glue("New columns: {setdiff(names(b), names(a))}", collapse = ", "))
  #     bind_rows(a, b)
  #   })
  
  file <- list.files(file.path(cf_dir, "latest"), recursive = T, full.names = T) %>% str_subset("armedconf_12")
  conflict_forecast <- read_csv(file, col_types = c("f", .default = "c")) %>%
    mutate(.keep = "unused", .before = 1,
           iso3 = isocode,
           yearmon = as.yearmon(as.character(period), format = "%Y%m"),
           year = lubridate::year(yearmon),
           month = lubridate::month(yearmon)
    ) %>%
    select(iso3, year, month, CONFLICTFORECAST_armed_conflict_12m = ons_armedconf_12_all, any_of(paste0("stock_topic_", 0:14)), stock_tokens) %>%
    rename_with(.cols = contains("stock"), ~ paste0("CONFLICTFORECAST_", .x))
  write_csv(conflict_forecast, file.path(cm_dir, "conflictforecast-org.csv"))
}

# UCDP ViEWS
write_views_csv <- function() {
  views_resps <- request("https://api.viewsforecasting.org") %>%
    req_url_path_append("fatalities002_2024_01_t01") %>%
    # Level of anaylsis: cm for country-month, pgm for PRIO-GRID-month
    req_url_path_append("cm") %>% 
    # Type of violence: sb = state-based; ns = non-state; os = oneosided
    req_url_path_append("sb") %>%
    req_url_query(date_start = "2004-01-01") %>%
    
    # views_req %>%
    
    # req_headers(Accept = "application/json") %>%
    # req_perform_iterative(next_req = iterate_with_link_url("page", \(resp) resp_body_json(resp)$next_page))
    req_perform_iterative(next_req = iterate_with_offset(
      param_name = "page", resp_pages = \(resp) resp_body_json(resp)$page_count))
  views <- views_resps %>%
    resps_data(\(resp) {
      resp_body_json(resp)$data %>%
        map(\(d) as_tibble(d))
    }) %>%
    bind_rows() %>%
    select(-c(country_id, month_id, gwcode, name)) %>%
    rename(iso3 = isoab) %>%
    rename_with(.cols = -c(iso3, year, month), ~ paste0("VIEWS_", .x))
  write_csv(views, file.path(cm_dir, "ucdp-views.csv"))
}

write_acled_cast_csv <- function() {
  # No more World Bank downloads until next year
  credentials <- read.csv(".access/acled.csv")
  resps <- request("https://api.acleddata.com/cast/read") %>%
    req_url_query(key = credentials$key, email = credentials$username) %>%
    req_perform_iterative(next_req = iterate_with_offset("page"))
  resps %>%
    resps_successes() %>%
    resps_data(\(resp) {resp_body_json(resp)$data})
}

# BTI Transformation Index
write_bti_csv <- function() {
  file <- "source-data/BTI_2006-2024_Scores.xlsx"
  sheets <- excel_sheets(file) %>% str_subset("old", negate = T)
  bti_all <- map(sheets, \(s) {
    readxl::read_xlsx(file, sheet = s, range = "A1:DS138", col_types = "text", na = c("n/a", "-", "?")) %>%
      mutate(year = str_extract(s, "\\d{4}"))
  }) %>%
    bind_rows()
  bti <- bti_all %>% 
    select(
      country = 1, year,
      status_index = `S | Status Index...4`,
      status_category = `Category...107`,
      status_category_text = `...108`,
      democracy_status = `SI | Democracy Status...5`,
      economy_status = `SII | Economy Status...29`,
      governance_index = `G | Governance Index...52`,
      governance_category = `Category...116`,
      governance_category_text = `...117`,
      governance_difficulty = `Q13 | Level of Difficulty...53`,
      governance_performance = `GII | Governance Performance...121`
    ) %>%
    mutate(across(-c(country, contains("text")), ~ as.numeric(.x))) %>%
    rename_with(.cols = -c(country, year), ~ paste0("BTI_", .x)) %>%
    mutate(.keep = "unused", .before = 1,
           iso3 = forcats::fct_relabel(factor(country), \(x) name2iso(x)),
           year = factor(year, levels = 2006:2024),
           month = list(1:12)) %>%
    unnest(month) %>%
    arrange(iso3, year, month) %>%
    complete(iso3, year, month) %>%
    group_by(iso3) %>%
    fill(starts_with("BTI"))
  write_csv(bti, file.path(cm_dir, "bti.csv"))
}

write_crm_fragility_csv <- function() {
  read_many_runs <- function(runs_directory = file.path(output_directory, "runs"), since = NULL, before = NULL, tail_n = NULL, index_only = T, lazy = F) {
    files <- sort(list.files(runs_directory))
    if (!is.null(since)) {
      dates <- as.Date(str_extract(files, "\\d{4}-\\d{2}-\\d{2}"))
      files <- files[which(dates >= as.Date(since))]
    }
    if (!is.null(before)) {
      dates <- as.Date(str_extract(files, "\\d{4}-\\d{2}-\\d{2}"))
      files <- files[which(dates <= as.Date(before))]
    }
    if (!is.null(tail_n)) files <- tail(files, n = tail_n)
    col_types <- if (index_only) '--d------dc---D' else 'dddfffffcdccflD'
    # all_runs <- bind_rows(lapply(file.path(runs_directory, files), read_csv, col_types = col_types))
    all_runs <- read_csv(file.path(runs_directory, files), col_types = col_types, lazy = lazy)
    return(all_runs)
  }

  crm <- read_many_runs("/Users/bennotkin/Documents/world-bank/crm/crm-db/output/manual/runs", index_only = F, lazy = T) %>%
    filter(`Data Level` == "Dimension Value" & Outlook %in% c("Overall", "Underlying"), Dimension == "Conflict and Fragility") %>%
    mutate(
      iso3 = Country, yearmon = as.yearmon(Date),
      indicator = glue("CRM {Outlook} Fragility and Conflict"), value_numeric = Value) %>%
    slice_max(Date, by = c(iso3, yearmon, indicator), with_ties = F) %>%
    select(iso3, yearmon, indicator, value_numeric) %>%
    pivot_wider(names_from = indicator, values_from = value_numeric) %>%
    mutate(.keep = "unused", year = lubridate::year(yearmon), month = lubridate::month(yearmon))
  write_csv(crm, file.path(cm_dir, "crm-fragility-conflict.csv"))
}

write_fcs_csv <- function() {
  fcs <- read_csv("/Users/bennotkin/Documents/world-bank/crm/crm-db/output/inputs-archive/fcs.csv") %>%
    mutate(FCS_normalised = case_when(!is.na(FCV_status) ~ 10, T ~ 0)) %>%
    rowwise() %>%
    mutate(yearmon = list(seq.Date(access_date, by = "month", length.out = 12))) %>%
    ungroup() %>%
    unnest(yearmon) %>%
    mutate(yearmon = as.yearmon(yearmon)) %>%
    slice_max(access_date, by = c(Country, yearmon)) %>%
    mutate(.keep = "none", iso3 = Country, yearmon = yearmon, indicator = "FCS List", value_numeric = FCS_normalised, value_character = FCV_status) %>%
    sjmisc::replace_na(value_character, value = "Non-FCS") %>%
      pivot_wider(names_from = indicator, values_from = value_numeric) %>%
      mutate(.keep = "unused", year = lubridate::year(yearmon), month = lubridate::month(yearmon))
  write_csv(fcs, file.path(cm_dir, "fcs-list.csv"))
}