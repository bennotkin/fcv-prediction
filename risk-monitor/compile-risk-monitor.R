#-------------------------------------------------------------------------------
#   COMPILING FCV RISK MONITOR DATASET
#   AUTHORS: BN
#   DATE: April 2024
#-------------------------------------------------------------------------------
#   Notes: For updated version of the R script use the link below in Github repo
#   https://github.com/bennotkin/fcv-prediction/blob/main/risk-monitor/compile-risk-monitor-indicators.R
#   Please be aware that source files can be found in the shared Office folder
#   File locations in the R code should be replaced with relevant local source
#
#   File compiles training data from CSVs written with R/gather.R 
#
#   WARNING: GENERATES FABRICATED DATA FOR INDICATORS WE DON'T YET HAVE ACCESS TO

source("R/setup.R")
indicator_meta <- read_csv("risk-monitor/indicator-list.csv", col_types = "iccffc")

cm_dir <- "country-month-data"

base <- read_csv(file.path(cm_dir, "wbg-income-levels.csv")) %>%
  filter(iso3 %in% unique(filter(., WBG_income_level < 4 & year == 2024)$iso3)) %>%
  select(iso3, year, month)

# Compile relevant datasets
left_join_with_checks <- function(a, b) {
  b <- filter(b, year >= 2000)
  b <- select(b, -any_of(c("pop", "yearmon")))
  # if (typeof(b$iso3) == "character") b$iso3 = factor(b$iso3)
  if (typeof(b$year) != "double") b$year = as.numeric(b$year)
  if (typeof(b$month) != "double") b$month = as.numeric(b$month)
  length_check <- dplyr::count(b, iso3, year, month) %>% filter(n > 1)
  if(nrow(length_check) > 0) {
    print(head(b))
    print(length_check)
    stop("More than one value per country-month")
  }
  left_join(a, b, by = c("iso3", "year", "month"))
}
most_wide <- Reduce(
  left_join_with_checks,
  list(
    base,
    read_csv(file.path(cm_dir, "fcs-list.csv")),
    read_csv(file.path(cm_dir, "crm-fragility-conflict.csv")),
    read_csv(file.path(cm_dir, "wbg-forecast.csv")),
    read_csv(file.path(cm_dir, "fsi.csv")),
    # read_csv(file.path(cm_dir, "bti.csv")),
    # read_csv(file.path(cm_dir, "vdem.csv")),
    read_csv(file.path(cm_dir, "acled.csv")),
    read_csv(file.path(cm_dir, "eiu.csv")),
    read_csv(file.path(cm_dir, "gic.csv")),
    read_csv(file.path(cm_dir, "icg-crisiswatch.csv")),
    read_csv(file.path(cm_dir, "idmc.csv")),
    read_csv(file.path(cm_dir, "ifes.csv")),
    # read_csv(file.path(cm_dir, "imf-rsui.csv")),
    read_csv(file.path(cm_dir, "inform-severity.csv")),
    read_csv(file.path(cm_dir, "ucdp.csv")),
    # read_csv(file.path(cm_dir, "wgi.csv")),
    read_csv(file.path(cm_dir, "acaps-risklist.csv")),
    read_csv(file.path(cm_dir, "polecat-icews.csv")),
    read_csv(file.path(cm_dir, "ucdp-views.csv")),
    read_csv(file.path(cm_dir, "conflictforecast-org.csv")))
    ) %>%
  select(iso3, year, month, any_of(indicator_meta$slug), na.omit(any_of(indicator_meta$slug_text))) %>%
  arrange(iso3, year, month) %>%
  mutate(iso3 = factor(iso3))

most <- most_wide %>%
  select(iso3, year, month, any_of(indicator_meta$slug)) %>%
  mutate(.keep = "unused",
    yearmon = as.yearmon(paste(year, month, sep = "-"))) %>%
  pivot_longer(cols = -c(iso3, yearmon), names_to = "indicator", values_to = "value_numeric")
  
most_character <- most_wide %>%
  select(iso3, year, month, na.omit(any_of(indicator_meta$slug_text))) %>%
  mutate(.keep = "unused",
    yearmon = as.yearmon(paste(year, month, sep = "-"))) %>%
  pivot_longer(cols = -c(iso3, yearmon), names_to = "indicator", values_to = "value_character") %>%
  left_join(
    select(indicator_meta, indicator_id, slug_text),
    by = join_by(indicator == slug_text)) %>%
  select(-indicator)

sample_real <- most %>%
  filter(yearmon >= as.yearmon("Jan 2014") & iso3 %in% unique(base$iso3)) %>%
  complete(iso3, indicator, yearmon, fill = list(value_numeric = NA, value_character = "")) %>%
  # Done in this indirect way so that `indicator` has the full indicator name
  # and slug is the new name for what was previously called indicator
  { right_join(indicator_meta, ., by = join_by(slug == indicator)) } %>%
  left_join(most_character, by = join_by(indicator_id, yearmon, iso3))

sample_fake <- indicator_meta %>%
  filter(indicator_id %ni% sample_real$indicator_id) %>%
  mutate(
    iso3 = list(unique(sample_real$iso3)),
    yearmon = list(as.yearmon(seq(as.Date("2014-01-01"), as.Date("2024-05-01"), by = "month")))) %>%
  unnest(iso3) %>% unnest(yearmon) %>%
  mutate(value_numeric = case_when(source == "WBG" ~ rnorm(n(), 5)))

sample <- bind_rows(sample_real, sample_fake) %>%
  mutate(
    iso3 = factor(iso3), indicator = factor(indicator),
    year = lubridate::year(yearmon),
    month = lubridate::month(yearmon),
    value_character = case_when(
      is.na(value_character) ~ as.character(round(value_numeric, 1)),
      T ~ value_character)) %>%
  mutate(
    map = ordered(map, levels = c("default", "secondary", "no")),
    forecast = ordered(forecast, levels = c("forecast", "monitoring", "dormant"))) %>%
  arrange(iso3, group_id, forecast, yearmon, indicator_id)

sample <- sample %>%
  select(iso3, yearmon, year, month, indicator_id, source, indicator, forecast, value_numeric, value_character)

latest_months <- sample %>%
  filter(!is.na(value_numeric)) %>%
  slice_max(yearmon, by = indicator_id, with_ties = F) %>%
  select(indicator_id, latest_month = yearmon)

indicator_meta_dated <- left_join(indicator_meta, latest_months, by = "indicator_id")

write_excel_csv(indicator_meta_dated, "risk-monitor/output/indicator-list.csv")
openxlsx2::write_xlsx(indicator_meta_dated, "risk-monitor/output/indicator-list.xlsx", first_active_col = 3, na.strings = "")
write_excel_csv(sample, "risk-monitor/output/fcv-risk-monitor.csv")
# openxlsx2::write_xlsx(sample, "risk-monitor/output/fcv-risk-monitor.xlsx", na.strings = "NA")
sample_minimal <- sample %>%
  select(indicator_id, iso3, yearmon, value_numeric, value_character)
write_excel_csv(sample_minimal, "risk-monitor/output/fcv-risk-monitor-minimal.csv")
openxlsx2::write_xlsx(sample_minimal, "risk-monitor/output/fcv-risk-monitor-minimal.xlsx", na.strings = "NA")
latest_month <- sample %>%
  left_join(latest_months, by = "indicator_id") %>%
  filter(yearmon == latest_month) %>%
  select(-latest_month)
write_excel_csv(latest_month, "risk-monitor/output/fcv-risk-monitor-latest-month.csv")
openxlsx2::write_xlsx(latest_month, "risk-monitor/output/fcv-risk-monitor-latest-month.xlsx", na.strings = "NA")
