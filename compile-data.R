  # Countrylist
  country_list <- read_csv("/Users/bennotkin/Documents/world-bank/crm/crm-db/src/country-groups.csv") %>%
    select(iso3 = Code)
  
  # Population data from WBG
  get_pop <- function() {
    url <- "http://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
    queryString <- list(format = "json", date = "2000:2024", per_page = "30000")
    response <- VERB("GET", url, body = "", query = queryString, content_type("application/octet-stream"), set_cookies(`api_http.cookie` = "2f4d39862a2fa1b0b0b0c4ad37e6251a"), encode = "raw")
    pop <- jsonlite::fromJSON(content(response, "text"))[[2]] %>%
      mutate(iso3 = countryiso3code, pop = value, year = as.numeric(date), .keep = "none")
    return(pop)
  }

  pop <- get_pop()

  starter <- left_join(country_list, pop, by = c("iso3")) %>%
  filter(iso3 != "TWN") %>% # currently no pop data for it
    mutate(
      across(c(iso3), ~ factor(.x)),
      year = factor(year, levels = 2000:2024),
      month = paste(1:12, collapse = ",")) %>%
    separate_longer_delim(month, delim = ",") %>%
    mutate(month = as.numeric(month)) %>%
    complete(iso3, year, month) %>%
    group_by(iso3) %>%
    fill(pop) %>%
    ungroup() %>%
    mutate(year = as.numeric(as.character(year))) %>%
    arrange(year, month, iso3)


  # ACLED
  
  # Select the last 20 years of data
  # start_date <- as.Date(as.yearmon(Sys.Date() - 45) - 20)
  # start_date <- as.Date(as.yearmon(Sys.Date() - 45) - .1)
  end_date <- Sys.Date()
  # Select fields
  fields <- "event_id_cnty|iso|event_date|event_type|fatalities|disorder_type|sub_event_type|actor1|actor2|assoc_actor_1|assoc_actor_2"

  credentials <- read_csv(paste_path(mounted_path, ".access/acled.csv"), col_types = "c")
  
  acled_list <- NULL
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
  select(-cnty_id)

acled <- acled %>%
  select(event_id_cnty, iso, iso3, event_date, year, month, everything()) %>%
  arrange(event_date)
write_csv(acled, "/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/acled-2004-present.csv")

acled <- acled %>%
  mutate(across(
    .cols = c(event_type, disorder_type, sub_event_type),
    ~ factor(.x))) %>%
  mutate(gang = 
    str_detect(actor1, "Unidentified Gang") | str_detect(actor1, "Unidentified Armed Group") |
    str_detect(actor2, "Unidentified Gang") | str_detect(actor2, "Unidentified Armed Group") |
    str_detect(assoc_actor_1, "Unidentified Gang") | str_detect(assoc_actor_1, "Unidentified Armed Group") |
    str_detect(assoc_actor_2, "Unidentified Gang") | str_detect(assoc_actor_2, "Unidentified Armed Group"))
conflict_related_deaths <- acled %>% 
  filter(
    event_type %in% c("Battles", "Violence against civilians", "Explosions/Remote violence") &
    fatalities > 0 &
    !gang) %>% 
  summarize(.by = c(iso3, year, month), ACLED_conflict_related_deaths = sum(fatalities))
event_count <- acled %>% 
  filter(!gang) %>%
  # filter(!(event_type == "Violence against civilians" & sub_event_type  == "Attack")) %>%
  count(iso3, year, month, event_type) %>%
  pivot_wider(names_from = event_type, values_from = n) %>%
  rename_with(.cols = -c(iso3, year, month), ~ paste0("ACLED_", slugify(.x)))
gang_events <- acled %>%
  filter(gang) %>%
  count(iso3, year, month) %>%
  rename(ACLED_Gang_Events = n)
# Still need to filter out & separately include gang activity
acled_monthly <- full_join(conflict_related_deaths, event_count, by = c("iso3", "year", "month")) %>%
  full_join(gang_events, by = c("iso3", "year", "month")) %>%
  rowwise() %>%
  mutate(ACLED_events = sum(ACLED_protests, ACLED_strategic_developments, ACLED_battles, ACLED_explosionsremote_violence, ACLED_riots, ACLED_violence_against_civilians, ACLED_Gang_Events, na.rm = T)) %>%
  ungroup() %>%
  complete(iso3, month) %>%
  sjmisc::replace_na(contains("ACLED"), value = 0) %>%
  arrange(iso3, year, month) %>%
  mutate(
    .by = c(iso3),
    BRD_lag = sapply(1:12, \(l) lag(ACLED_conflict_related_deaths, l)),
    Events_lag = sapply(1:12, \(l) lag(ACLED_events, l))) %>%
  rowwise() %>%
  mutate(
    ACLED_conflict_related_deaths_change = ACLED_conflict_related_deaths/mean(c_across(contains("BRD_lag")) - 1),
    ACLED_events_change = ACLED_events/mean(c_across(contains("Events_lag"))) - 1) %>%
  select(-matches("_lag$"))

acled_monthly_all <- left_join(starter, ungroup(acled_monthly), by = c("iso3", "year", "month")) %>%
  filter(year >= 2003) %>%
  filter(!(year == 2024 & month > 2)) %>%
  mutate(ACLED_BRD_per_100k = ACLED_conflict_related_deaths/pop * 100000)

summary(acled_monthly_all)
acled_monthly_all[incomplete_cases(acled_monthly_all),] %>%
  select(iso3, year) %>%
  distinct() %>%
  filter(year > 2016) %>%
  distinct(iso3)

# UCDP
ucdp <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/UCDP/GEDEvent_v23_1.xlsx")


# GIC
gic <- read_tsv("http://www.uky.edu/~clthyn2/coup_data/powell_thyne_coups_final.txt",
                col_types = "cdddddddc") %>%
       mutate(
        iso3 = name2iso(country),
        GIC_coup_successful = case_when(coup == 2 ~ T),
        GIC_coup_failed = case_when(coup == 1 ~ T)) %>%
  summarize(
    .by = c(iso3, year, month),
    across(.cols = contains("GIC"), ~ sum(.x, na.rm = T)))
gic <- filter(gic, year > 2000)
gic <- left_join(starter, gic, by = c("iso3", "year", "month")) %>%
  sjmisc::replace_na(contains("GIC"), value = 0)
# IFES
# ifes_data <- system(paste0("curl -X GET https://electionguide.org/api/v1/elections_demo/ -H 'Authorization: Token ", readLines(".access/ifes-authorization.txt", warn = F),"'"),
#    intern = T) %>%
#    fromJSON()

# REIGN
reign <- read_csv(paste0("https://raw.githubusercontent.com/OEFDataScience/REIGN.github.io/gh-pages/data_sets/REIGN_", 2021, "_", 8, ".csv"),
                                                col_types = cols()) %>%
  select(ccode, country, year, month, REIGN_delayed_election = delayed, REIGN_irregular_election_anticipated = irreg_lead_ant, REIGN_election_anticipated = anticipation) %>%
  filter(year >= 2000) %>%
  # mutate(iso3 = countrycode(ccode, origin = "cown", destination = "iso3c"))
  mutate(iso3 = name2iso(country))
reign <- reign %>% 
  mutate(iso3 = case_when(
    country == "UKG" ~ "GBR",
    country == "Cen African Rep" ~ "CAF",
    T ~ iso3)) %>%
  select(iso3, year, month, contains("REIGN"))
  reign <- reign %>%
    mutate(iso3 = factor(iso3)) %>%
    summarize(.by = c(iso3, year, month), across(contains("REIGN"), max, na.rm = T))
reign <- left_join(starter, reign, by = c("iso3", "year", "month")) %>%
  sjmisc::replace_na(contains("REIGN"), value = 0)

reign %>% count(iso3, year, month) %>% filter(n > 1) 

filter(reign, iso3 == "AFG", year == 2014 & month == 9)
filter(reign_all, country == "Afghanistan", year == 2014 & month == 9)

# FEWS
# url <- 'https://datacatalogapi.worldbank.org/ddhxext/ResourceView?resource_unique_id=DR0091743'
# queryString <- list('resource_unique_id' = "DR0091743")
# response <- VERB("GET", url, query = queryString)
# metadata <- fromJSON(content(response, "text"))
# version_date <- as.Date(str_extract(basename(metadata$distribution$url), "20\\d{2}-\\d{1,2}-\\d{1,2}"))
filename <- file.path(inputs_archive_path, "fews", paste0("fews-", version_date, ".csv"))
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
  fill(contains("FEWS"))


# Food Price Inflation
# fpi <- read_csv("/Users/bennotkin/Documents/world-bank/crm/crm-db/hosted-data/food-price-inflation/WLD_RTFP_country_2024-01-08.csv") %>%
#   rename(iso3 = ISO3, Food_Price_Inflation = Inflation) %>%
#   mutate(year = year(date),
#   month = month(date)) %>%
#   select(iso3, year, month, Food_Price_Inflation) %>%
#   filter(!is.na(Food_Price_Inflation))
# fpi <- left_join(starter, fpi, by = c("iso3", "year", "month"))

# EIU
eiu <- read_csv("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/eiu-operational-risk-macroeconomic-2002-2024.csv",
          na = c("", "–", "NA")) %>%
  mutate(iso3 = name2iso(Geography)) %>%
  select(iso3, starts_with("2")) %>%
  pivot_longer(cols = starts_with("20"), names_to = "yearmon", values_to = "EIU_macroeconomic_risk") %>%
  mutate(
    year = str_sub(yearmon, 1, 4),
    month = str_sub(yearmon, -2, -1)) %>%
  select(-yearmon)
  eiu <- left_join(starter, eiu, by = c("iso3", "year", "month")) %>%
    fill(contains("EIU"))

# FSI
# fsi <- read_most_recent('hosted-data/fsi', FUN = read_xlsx, as_of = Sys.Date(), return_date = F)
fsi_files <- list.files("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/fsi-historic", full.names = T)
fsi_files <- setNames(fsi_files, str_extract(fsi_files, "\\d{4}"))

fsi <- fsi_files %>%
  lapply(read_xlsx, col_types = "text") %>%
  bind_rows()
fsi <- fsi %>%
  mutate(iso3 = name2iso(Country)) %>%
  select(iso3, FSI = Total)

setNames(fsi_list)
fsi_years
fsi_list
fsi <- bind_rows(fsi_list)
fsi

# INFORM Socioeconomic Vulnerability
inform <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/INFORM_TREND_2014_2023.xlsx")
inform <- filter(inform, str_detect(IndicatorName, "Socio")) %>% 
  select(iso3 = Iso3, year = INFORMYear, INFORM_Socio_Vuln = IndicatorScore) %>%
  # mutate(month = factor(1, levels = 1:12)) %>%
  mutate(month = paste(1:12, collapse = ",")) %>%
  separate_longer_delim(month, delim = ",")

# CPIA
cpia <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/CPIA.xlsx", sheet = "Data",
  na = c("NA", "", "..")) %>%
  filter(`Series Name` == "IDA resource allocation index (1=low to 6=high)") %>%
  select(iso3 = `Country Code`, matches("\\d.*")) %>%
  pivot_longer(cols = -iso3, names_to = "year", values_to = "CPIA_IRA") %>%
  mutate(year = as.numeric(str_extract(year, "^\\d{4}"))) %>%
  mutate(month = paste(1:12, collapse = ",")) %>%
  separate_longer_delim(month, delim = ",") %>%
  filter(!is.na(CPIA_IRA))

# EM-DAT
emdat_full <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/EM-DAT.xlsx",
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
        disasters = length(unique(disno)))
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

# V-DEM
# vdem <- read_csv("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/V-Dem-CY-Full+Others-v13.csv")
# devtools::install_github("vdeminstitute/vdemdata")
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

# GDP
gdp <- read_xls("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/GDP per Capita/GDP per capita, PPP (current international $).xls", skip = 3) %>%
  select(-starts_with('1')) %>%
  mutate(across(1:4, ~ as.factor(.x))) %>%
  select(iso3 = `Country Code`, starts_with('2')) %>%
  pivot_longer(cols = -iso3, names_to = "year", values_to = "WBG_GDP_PPP") %>%
  mutate(month = paste(1:12, collapse = ",")) %>%
  separate_longer_delim(month, delim = ",")

# CPI Inflation
cpi <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/Inflation-data.xlsx",
          sheet = "hcpi_m", range = "R1C1:R189C644") %>%
  # get_col_types_short(cpi, collapse = F) %>% subset(. != "d")
  mutate(.keep = "none",
    iso3 = `Country Code`,
    across(starts_with("2"), ~ as.numeric(.x))) %>%
  pivot_longer(cols = starts_with("2"), names_to = "yearmon", values_to = "WBG_CPI") %>%
  mutate(
    year = as.numeric(str_sub(yearmon, 1, 4)),
    month = as.numeric(str_sub(yearmon, -2, -1))) %>%
  select(-yearmon)

# WBG Worldwide Governance Indicator
wgi <- 2:7 %>%
  lapply(function(s) {
    name <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/wgidataset.xlsx",
      sheet = s, range = "A1:A1", col_names = F) %>% pull()
    sheet <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/wgidataset.xlsx",
      sheet = s, skip = 13, col_types = "text")
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
  pivot_wider(names_from = "indicator", values_from = "value")

# UNDP Gender
gii <- read_csv("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/UNDP_historic dataset (composite, see GII).csv") %>% 
  select(iso3, matches("gii_\\d{4}")) %>%
  pivot_longer(cols = -iso3, names_to = "year", values_to = "UNDP_GII") %>%
  filter(!is.na(UNDP_GII)) %>%
  mutate(
    year = as.numeric(str_extract(year, "\\d{4}")),
    month = paste(1:12, collapse = ",")) %>%
  separate_longer_delim(month, delim = ",")

# IDMC Forced displacement
idmc <- read_xlsx("/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/Displacement (IDMC & UNHCR)/IDMC_Internal_Displacement_Conflict-Violence_Disasters 2008-2022.xlsx") %>%
  select(iso3 = ISO3, year = Year, IDMC_IDPs = `Conflict Stock Displacement (Raw)`) %>%
  mutate(
    month = paste(1:12, collapse = ",")) %>%
  separate_longer_delim(month, delim = ",")

# Combine together 
variables <- Reduce(
  function(a, b) {
    b <- filter(b, year >= 2000)
    b <- select(b, -any_of("pop"))
    if (typeof(b$year) != "double") b$year = as.numeric(b$year)
    if (typeof(b$month) != "double") b$month = as.numeric(b$month)
    length_check <- count(b, iso3, year, month) %>% filter(n > 1)
    if(nrow(length_check) > 0) {
      print(head(b))
      print(length_check)
      stop("More than one value per country-month")
    }
    left_join(a, b, by = c("iso3", "year", "month"))
  },
 list(
    starter,
    acled_monthly_all,
    gic,
    fews_monthly,
    reign,
    cpi,
    # fpi,
    eiu,
    # inform,
    cpia,
    gdp,
    wgi,
    v_dem,
    gii,
    emdat
    )) %>%
    arrange(iso3, year, month)
variables$iso3 <- factor(variables$iso3) 
variables <- variables %>% filter(!is.na(iso3), !is.na(year), !is.na(month))

# FCV trigger (Total risk) = (BRD > 20 AND BRD > 0.2 per 100,000) OR
# (Political Event* < 25) OR (Coup = 1 OR Election Interference = 1)

# FCV trigger (Change risk) = 
# [(BRD > 10 AND BRD > 0.1 per 100,000) OR 
# (BRD > 0.1 AND 25% increase in BRD relative to 12-month average)] AND 
# [(25% increase in number of Political Events* relative to 12 month mean) OR 
# (Coup = 1 OR Election Interference = 1)]

# Notes: Polical events classified as the sum of all events linked to battles,
# explosions, riots, stategic development, protest, gang-related violance, or violence
# against civilians in a given month

variables <- variables %>%
  mutate(
    trigger_total_risk =
      (ACLED_conflict_related_deaths > 20 & ACLED_BRD_per_100k > 0.2) |
      (GIC_coup_failed | GIC_coup_successful) |
      (REIGN_delayed_election == 1 | REIGN_irregular_election_anticipated == 1),
    trigger_change_risk =
      ( ACLED_conflict_related_deaths > 10 &
        ACLED_BRD_per_100k > 0.1 &
        ACLED_conflict_related_deaths_change > .25) |
      ( ACLED_events > 5 & ACLED_events_change > .25 ) |
      ( GIC_coup_failed | GIC_coup_successful) |
      ( REIGN_delayed_election == 1 | REIGN_irregular_election_anticipated == 1))

write_csv(variables, "/Users/bennotkin/Documents/world-bank/crm/fcv-prediction/fcv-prediction-variables-2.csv")

variables

variables %>% summary()

variables %>%
  select(iso3, year, month, ACLED_conflict_related_deaths, ACLED_BRD_per_100k, ACLED_events, GIC_coup_failed, GIC_coup_successful) %>%
  summary()

reign %>% filter(REIGN_delayed_election == 1 | REIGN_irregular_election_anticipated == 1) %>%
select(iso3, year) %>% distinct() %>% mutate()
