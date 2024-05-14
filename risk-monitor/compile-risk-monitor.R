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
    read_csv(file.path(cm_dir, "fsi.csv")),
    read_csv(file.path(cm_dir, "bti.csv")),
    read_csv(file.path(cm_dir, "vdem.csv")),
    read_csv(file.path(cm_dir, "acled.csv")),
    read_csv(file.path(cm_dir, "eiu.csv")),
    read_csv(file.path(cm_dir, "gic.csv")),
    read_csv(file.path(cm_dir, "icg-crisiswatch.csv")),
    read_csv(file.path(cm_dir, "idmc.csv")),
    read_csv(file.path(cm_dir, "ifes.csv")),
    read_csv(file.path(cm_dir, "imf-rsui.csv")),
    read_csv(file.path(cm_dir, "inform-severity.csv")),
    read_csv(file.path(cm_dir, "ucdp.csv")),
    read_csv(file.path(cm_dir, "wgi.csv")),
    read_csv(file.path(cm_dir, "acaps-risklist.csv")),
    read_csv(file.path(cm_dir, "polecat-icews.csv")),
    read_csv(file.path(cm_dir, "ucdp-views.csv")),
    read_csv(file.path(cm_dir, "conflictforecast-org.csv")))
    ) %>%
  select(
    iso3, year, month,
    # structural
    `FCS List`,
    BTI_status_index,
    FSI,
    VDEM_electoral,
    # monitoring
    `CRM Overall Fragility and Conflict`,
    `CRM Underlying Fragility and Conflict`,
    ACAPS_risk_level,
    ACLED_conflict_related_deaths,
    ACLED_events,
    CW_risk,
    EIU_political_stability_risk,
    EIU_security_risk,
    GIC_coup_failed,
    GIC_coup_successful,
    IDMC_ID_movements_combined,
    IFES_anticipated,
    IMF_rsui_event,
    INFORMSEVERITY_index,
    POLECAT_NEG,
    UCDP_BRD_per_100k,
    WGI_political_stability_and_absence_of_violenceterrorism,
    # forecast
    CONFLICTFORECAST_armed_conflict_12m,
    VIEWS_main_dich
  ) %>%
  arrange(iso3, year, month) %>%
  mutate(iso3 = factor(iso3))

most <- most_wide %>%
  mutate(.keep = "unused",
    yearmon = as.yearmon(paste(year, month, sep = "-")),
    GIC_coup = case_when(GIC_coup_successful == 1 ~ 2,
                         GIC_coup_failed == 1 ~ 1,
                         T ~ 0)) %>%
  pivot_longer(cols = -c(iso3, yearmon, where(is.character)), names_to = "indicator", values_to = "value_numeric")

indicator_meta <- read_csv("risk-monitor/indicator-list.csv", col_types = "iccffc")

sample_real <- bind_rows(most, fcs, crm) %>%
  filter(yearmon >= as.yearmon("Jan 2014") & iso3 %in% unique(base$iso3)) %>%
  complete(iso3, indicator, yearmon, fill = list(value_numeric = 0, value_character = "")) %>%
  { right_join(indicator_meta, ., by = join_by(slug == indicator)) }

sample_fake <- indicator_meta %>%
  filter(id %ni% sample_real$id) %>%
  mutate(
    iso3 = list(unique(sample_real$iso3)),
    yearmon = case_when(
      timeframe != "forecast" ~ list(as.yearmon(seq(as.Date("2014-01-01"), as.Date("2024-04-01"), by = "month"))),
      timeframe == "forecast" ~ list(as.yearmon(seq(as.Date("2024-01-01"), as.Date("2026-04-01"), by = "month"))))) %>%
  unnest(iso3) %>% unnest(yearmon) %>%
  mutate(value_numeric = rnorm(n(), 5))

sample <- bind_rows(sample_real, sample_fake) %>%
  rename(indicator_id = id) %>%
  mutate(
    iso3 = factor(iso3), indicator = factor(indicator),
    year = lubridate::year(yearmon),
    month = lubridate::month(yearmon),
    value_character = 
      case_when(indicator == "GIC_Coup" & value_numeric == 2 ~ "Successful coup",
                indicator == "GIC_Coup" & value_numeric == 1 ~ "Failed coup",
                indicator == "CW_risk" & value_numeric ==  1 ~ "Deteriorated situation",
                indicator == "CW_risk" & value_numeric ==  0 ~ "Unchanged situation",
                indicator == "CW_risk" & value_numeric == -1 ~ "Improved situation",
                T ~ as.character(round(value_numeric, 1)))) %>%
  mutate(
    map = ordered(map, levels = c("default", "secondary", "no")),
    timeframe = ordered(timeframe, levels = c("structural", "monitoring", "forecast"))) %>%
  arrange(iso3, timeframe, yearmon, indicator_id) %>%
  mutate(.by = c(indicator, year),
    level = ordered(case_when(
      value_numeric > quantile(value_numeric, .95) ~ "High",
      value_numeric > quantile(value_numeric, .85) ~ "Medium",
      T ~ "Low"), levels = c("Low", "Medium", "High")))

sample <- sample %>%
  select(iso3, yearmon, year, month, indicator_id, source, indicator, timeframe, value_numeric, value_character, level)

openxlsx2::write_xlsx(indicator_meta, "risk-monitor/output/indicator-list.xlsx")
write_excel_csv(sample, "risk-monitor/output/fcv-risk-monitor.csv")
openxlsx2::write_xlsx(sample, "risk-monitor/output/fcv-risk-monitor.xlsx")
sample_minimal <- sample %>%
  select(indicator_id, iso3, yearmon, value_numeric, value_character, level)
write_excel_csv(sample_minimal, "risk-monitor/output/fcv-risk-monitor-minimal.csv")
openxlsx2::write_xlsx(sample_minimal, "risk-monitor/output/fcv-risk-monitor-minimal.xlsx")
current_month <- sample %>%
  filter(yearmon == "Mar 2024")
write_excel_csv(current_month, "risk-monitor/output/fcv-risk-monitor-current-month.csv")
openxlsx2::write_xlsx(current_month, "risk-monitor/output/fcv-risk-monitor-current-month.xlsx")
