#-------------------------------------------------------------------------------
#   COMPILING FCV RISK TRAINING DATASET
#   AUTHORS: BN & LJ
#   DATE: February 2024
#-------------------------------------------------------------------------------
#   Notes: For updated version of the R script use the link below in Github repo
#   https://github.com/bennotkin/fcv-prediction/blob/main/R/compile.R
#   Please be aware that source files can be found in the shared Office folder
#   File locations in the R code should be replaced with relevant local source
#
#   File compiles training data from CSVs written with R/gather.R 

source("R/setup.R")

cm_dir <- "country-month-data"

starter <- initiate_df()

# Combine all relevant datasets together---------------------------------------
print("Compiling training dataset")
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
training <- Reduce(
  left_join_with_checks,
  list(
    # Starter data frame of countries, years and months
    starter,
    # Outcome variables
    read_csv(file.path(cm_dir, "acled.csv")),
    read_csv(file.path(cm_dir, "ucdp.csv")),
    read_csv(file.path(cm_dir, "gic.csv")),
    # Predictor variables
    read_csv(file.path(cm_dir, "reign.csv")),
    read_csv(file.path(cm_dir, "ifes.csv")),
    read_csv(file.path(cm_dir, "fews.csv")),
    # fpi,
    read_csv(file.path(cm_dir, "fsi.csv")),
    read_csv(file.path(cm_dir, "bti.csv")),
    read_csv(file.path(cm_dir, "cpi.csv")),
    read_csv(file.path(cm_dir, "eiu.csv")),
    read_csv(file.path(cm_dir, "inform-risk.csv")),
    read_csv(file.path(cm_dir, "inform-severity.csv")),
    read_csv(file.path(cm_dir, "cpia.csv")),
    read_csv(file.path(cm_dir, "wbg-gdp.csv")),
    read_csv(file.path(cm_dir, "wgi.csv")),
    read_csv(file.path(cm_dir, "vdem.csv")),
    read_csv(file.path(cm_dir, "undp-gii.csv")),
    read_csv(file.path(cm_dir, "emdat.csv")),
    read_csv(file.path(cm_dir, "spei.csv")),
    read_csv(file.path(cm_dir, "eth-epr.csv")),
    read_csv(file.path(cm_dir, "idmc.csv")),
    read_csv(file.path(cm_dir, "imf-rsui.csv")),
    read_csv(file.path(cm_dir, "wbg-resource-rents.csv")),
    read_csv(file.path(cm_dir, "wbg-evacuations.csv")),
    read_csv(file.path(cm_dir, "polecat.csv")),
    read_csv(file.path(cm_dir, "polecat-icews.csv")),
    read_csv(file.path(cm_dir, "icews.csv")),
    read_csv(file.path(cm_dir, "conflictforecast-org.csv")),
    read_csv(file.path(cm_dir, "icg-crisiswatch.csv")),
    read_csv(file.path(cm_dir, "wbg-income-levels.csv")),
    read_csv(file.path(cm_dir, "wbg-lending-categories.csv")),
    read_csv(file.path(cm_dir, "acaps-risklist.csv")),
    read_csv(file.path(cm_dir, "ucdp-views.csv")))) %>%
  arrange(iso3, year, month) %>%
  mutate(iso3 = factor(iso3))

# FCV trigger (Total risk) = 
# (BRD > 20 AND BRD > 0.2 per 100,000) OR
# (Political or Violent Event* > 25) OR
# (Coup = 1 OR Election Interference = 1)

# FCV trigger (Change risk) = 
# (BRD > 10 AND BRD > 0.1 per 100,000 AND 25% increase in BRD relative to 12-month average) OR
# (Political or Violent Event* > 5 & 25% increase in number of Political or Violent Events* relative to 12 month mean) OR 
# (Coup = 1 OR Election Interference = 1)]

# Notes: Polical events classified as the sum of all events linked to battles,
# explosions, riots, stategic development, protest, gang-related violance, or violence
# against civilians in a given month

# Create triggers for FCV------------------------------------------------------
# Add triggers for FCV risk
print("Adding triggers")
training <- training %>%
  mutate(
    # Trigger for total FCV risk
    TRIGGER_total_risk =
      # (ACLED_conflict_related_deaths > 20 & ACLED_BRD_per_100k > 0.2) |
      (UCDP_BRD > 20 & UCDP_BRD_per_100k > 0.2) |
      (ACLED_events > 25) |
      (GIC_coup_failed | GIC_coup_successful) |
      (REIGN_delayed_election == 1 | REIGN_irregular_election_anticipated == 1),
    # Trigger for change in FCV risk
    TRIGGER_change_risk =
      # ( ACLED_conflict_related_deaths > 10 &
      #   ACLED_BRD_per_100k > 0.1 &
      #   ACLED_conflict_related_deaths_change > .25) |
      ( UCDP_BRD > 10 & UCDP_BRD_per_100k > 0.1 & UCDP_BRD_change > .25) |
      ( ACLED_events > 5 & ACLED_events_change > .25 ) |
      ( GIC_coup_failed | GIC_coup_successful) |
      ( REIGN_delayed_election == 1 | REIGN_irregular_election_anticipated == 1)) %>%
    # Remove transformed variables
    select(-ACLED_conflict_related_deaths_change, -ACLED_events_change, -UCDP_BRD_change)

# Add spatially lagged triggers (only includes land neighbors)
borders <- read_csv("source-data/borders.csv", col_types = "cc")
neighboring_triggers <- training %>% select(iso3, year, month, contains("TRIGGER")) %>%
  # tail(n = 6) %>%
  inner_join(filter(borders, !is.na(border_iso3)), by = "iso3", relationship = "many-to-many") %>%
  summarize(
    .by = c(border_iso3, year, month),
    across(contains("TRIGGER"), .fns = list(
      list = \(x) paste(iso3[x & !is.na(x)], collapse = ";"),
      any = any))) %>%
  rename_with(.cols = contains("TRIGGER"), ~
    str_replace_all(.x, c("TRIGGER" = "TRIGGER_neighbor", "_any" = ""))) %>%
  rename(iso3 = border_iso3)

training <- training %>%
  left_join(select(neighboring_triggers, -contains("list")), by = c("iso3", "year", "month")) %>%
  # select(iso3, year, month, contains('TRIGGER')) %>%
  mutate(across(contains("TRIGGER_neighbor"), ~
    case_when(iso3 %ni% neighboring_triggers$iso3 ~ FALSE, T ~ .x)))

# Limit training dataset to low and middle income countries
training_limited <- training %>%
  filter(iso3 %in% unique(filter(., WBG_income_level < 4 & year == 2024)$iso3))

# Write FCV_training_dataset.csv
write_csv(training_limited, "FCV_training_dataset.csv")

# Other spatial lags
borders_wide <- borders %>%
  mutate(value = 1) %>%
  # mutate(.before = 2,
  #   year = list(min(training_limited$year):max(training_limited$year)),
  #   month = list(1:12)) %>% 
  #   unnest(year) %>%
  #   unnest(month) %>%
  pivot_wider(names_from = border_iso3, values_from = value, values_fill = 0) %>%
  select(iso3, sort(any_of(.$iso3))) %>%
  select(iso3, sort(names(.))) %>%
  filter(iso3 %in% names(.) & !is.na(iso3)) %>%
  arrange(iso3) #%>%

which_not(borders_wide$iso3, names(borders_wide), both = T)

Bnames <- borders_wide$iso3
B <- as.matrix(borders_wide[,-1])
rownames(B) <- Bnames

training_limited <- training_limited %>%
  # head() %>%
  rowwise() %>%
  mutate(.before = 3, yearmon = as.yearmon(paste(year, month, sep = "-"))) %>% ungroup()
slag <- unique(training_limited$yearmon) %>%
  # .[1] %>%
  lapply(\(mon) {
    print(mon)
    M <- filter(training_limited, yearmon == mon)
    Mnames <- M$iso3
    M <- select(M, -where(is.character), -year, -month, -yearmon)
    M <- mutate(M, across(everything(), ~ as.numeric(.x)))
    M <- as.matrix(M)
    rownames(M) <- Mnames
    BinM <- subset(rownames(B), rownames(B) %in% rownames(M))
    M <- M[BinM,]
    # M %>% {apply(1, as.numeric)}
    # M <- apply(M, 2, as.numeric)
    # t(M) %>% apply(1, \(i) colMaxs(i * B))
    maxes <- B %>% apply(1, \(i) matrixStats::colMaxs(i * M, na.rm = T)) %>%
      t() %>% as_tibble(rownames = "iso3")
    mins <- B %>% apply(1, \(i) matrixStats::colMins(i * M, na.rm = T)) %>%
      t() %>% as_tibble(rownames = "iso3")
    df <- full_join(maxes, mins, by = 'iso3')
    # if (identical(maxes$iso3, mins$iso3)) df <- bind_cols(maxes, semins)
    return(df)
  })

slag_df <- map2(slag, unique(training_limited$yearmon), \(x, y) {
  mutate(x, year = year(my(y)), month = month(my(y)), .after = iso3)
}) %>% bind_rows()

write_csv(slag_df, "spatial-lag.csv")

# Write/append codebook-variables.csv to be used in codebook
vars <- names(training)
if(!file.exists("codebook-variables.csv")) {
  codebook <- tibble(
    `Data Source` = str_extract(vars, "^([^_]*)"),
    `Variable Label` = vars,
    Definition = "")
  write_csv(codebook, "codebook-variables.csv")
} else {
  codebook <- read_csv("codebook-variables.csv", col_types = "ccc")
  vars_included <- str_replace_all(codebook$`Variable Label`, c("\\n|;\\s*" = "|", "_…" = ".*"))
  new_vars <- vars[!str_detect(vars, paste0(vars_included, collapse = "|"))]
  codebook_addition <- tibble(
    `Data Source` = str_extract(new_vars, "^([^_]*)"),
    `Variable Label` = new_vars,
    Definition = "")
  codebook <- bind_rows(codebook, codebook_addition)
  # Arrange codebook alphabetically
  codebook <- bind_rows(
    filter(codebook, `Data Source` == "Ad-hoc"),
    filter(codebook, `Data Source` != "Ad-hoc") %>% arrange(`Variable Label`))
  write_csv(codebook, "codebook-variables.csv")
}
