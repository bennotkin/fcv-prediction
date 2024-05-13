# Effects of UCDP unverified data

source("R/setup.R")
library(ggplot2)

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

ucdp_may2021 <- read_csv("UCDP/ucdp_2021.csv") %>%
  mutate(download_date = "May 2021", yearmon = as.yearmon(yearmon), verified = year < 2020) %>%
  filter(yearmon < "Apr 2021")
ucdp_may2022 <- read_csv("UCDP/ucdp_2022.csv") %>%
  mutate(download_date = "May 2022", yearmon = as.yearmon(yearmon), verified = year < 2021) %>%
  filter(yearmon < "Apr 2022")
ucdp_may2023 <- read_csv("UCDP/ucdp_2023.csv") %>%
  mutate(download_date = "May 2023", yearmon = as.yearmon(yearmon), verified = year < 2022) %>%
  filter(yearmon < "Apr 2023")
ucdp_may2024 <- read_csv(file.path(cm_dir, "ucdp.csv")) %>%
  mutate(download_date = "May 2024", yearmon = as.yearmon(yearmon), verified = year < 2023)

ucdp <- reduce(list(ucdp_may2021, ucdp_may2022, ucdp_may2023, ucdp_may2024), bind_rows) %>%
  select(-c(UCDP_events, UCDP_BRD_change)) %>%
  mutate(
    download_date = factor(download_date, levels = rev(sort(unique(download_date)))),
    conflict = UCDP_BRD >= 25 | UCDP_BRD_per_100k >= 10)

# Add tranquility, onsets, and status
ucdp <- ucdp %>%
  mutate(.by = c(iso3, download_date),
    tranquility = !matrixStats::rowAnys(lag_multi(conflict, ns = 1:12)),
    onset = tranquility & conflict,
    Status = factor(case_when(
      onset ~ "Onset",
      tranquility ~ "Tranquility",
      !tranquility & conflict ~ "Active conflict",
      !tranquility & !conflict ~ "Non-tranquility non-conflict")))

# ucdp %>%
#   filter(onset)

# ucdp %>%
#   filter(iso3 == "AFG") %>%
#   ggplot(aes(x = yearmon, y = UCDP_BRD, fill = factor(version))) +
#     geom_col(position = "identity", alpha = 0.7)
# ?geom_col
# ucdp %>%
#   slice_max(by = version, order_by = yearmon, with_ties = F)

# Plot conflict onsets for each download_date
unverified_rectangles <- tibble(xmin = 2020:2023 - 1/26, xmax = xmin + 1.25, ymin = 3:0 + 2/3, ymax = ymin + 2/3)
ucdp %>%
  filter(year >= 2018) %>%
  filter(!(tranquility & !conflict)) %>%
  # filter(iso3 %in% c("EGY", "UKR", "BEN", "BFA")) %>%
  ggplot() +
    scale_y_discrete() +
    geom_rect(data = unverified_rectangles, aes(xmin = xmin, xmax = xmax, ymin = ymin, ymax = ymax),
      # fill = "lightgrey") +
      # fill = NA, color = "black", linetype = "dashed") +
      fill = NA, color = "black", linewidth = 0.2) +
    geom_point(aes(x = yearmon, y = factor(download_date), color = Status), shape = 15) +
    scale_color_manual(values = c("Active conflict" = scales::hue_pal()(2)[1], "Non-tranquility non-conflict" = scales::hue_pal()(2)[2], "Onset" = "black")) +
    facet_wrap(vars(iso3), ncol = 3, strip.position = "right") +
    labs(
      title = "Conflict periods by UCDP data release year",
      y = "Data release year") +
    theme_bw() +
    theme(
      legend.position = "bottom",
      panel.grid.major.y = element_blank(),
      panel.grid.minor.y = element_blank(),
      axis.title.x = element_blank())
ggsave("plots/ucdp-unverified-effects.png", width = 16, height = 14)

ucdp_wide <- ucdp %>%
  select(iso3, year, yearmon, UCDP_BRD, version, conflict, tranquility, onset, Status) %>%
  mutate(version = paste0("v", version)) %>%
  pivot_wider(names_from = version, values_from = c(UCDP_BRD, conflict, tranquility, onset, Status))

# How v2021 differs from later
ucdp_wide %>%
  select(iso3, year, yearmon, contains("BRD"), contains("onset")) %>%
  filter(yearmon < "Apr 2021") %>%
  mutate(
    later_removed = onset_v2021 & (!onset_v2022 | !onset_v2023),
    later_added = !onset_v2021 & (onset_v2022 | onset_v2023)
  ) %>%
  filter(later_removed | later_added) %>%
    # count(year) %>%
    # count(later_removed) %>%
    # mutate(across(where(is.numeric), ~ round(.x))) %>%
    knitr::kable()

# How v2022 differs from v2023
ucdp_wide %>%
  select(iso3, year, yearmon, contains("BRD"), contains("onset")) %>%
  filter(yearmon < "Apr 2022") %>%
  mutate(
    later_removed = onset_v2022 & !onset_v2023,
    later_added = !onset_v2022 & onset_v2023) %>%
  filter(later_removed | later_added) %>%
    # count(year) %>%
    # count(later_removed) %>%
    mutate(across(where(is.numeric), ~ round(.x))) %>%
    knitr::kable()

    # geom_point(aes(x = 2021 - 1/26, y = 3), shape = "I", color = "black")
    # geom_path(data = tibble(
    #     x = c(2021, 2021, 2022, 2022, 2023, 2023),
    #     y = c(3, 2, 2, 1, 1, 0)),
    #   aes(x = x, y = y), linetype = "dashed")

# Find onsets for each version
onsets <- ucdp_wide %>%
  # filter(yearmon >= 2020) %>%
  select(iso3, year, yearmon, starts_with("onset")) %>%
  filter(if_any(starts_with("onset"), ~ .x)) %>%
  mutate(.before = 1, country = iso2name(iso3))
summarize(onsets, across(starts_with("onset"), ~ sum(.x, na.rm = T)))

# Comparison of verified data
# But the goal is to check UNVERIFIED...
ucdp %>%
  # filter(year >= 2018) %>%
  filter(year < 2022) %>%
  filter(
    !(version == 2021 & yearmon > 2021),
    !(version == 2022 & yearmon > 2022),
    !(version == 2023 & yearmon > 2023)
  ) %>%
  summarize(.by = c(year, version), onsets = sum(onset, na.rm = T)) %>%
  mutate(    version = factor(version, levels = rev(sort(unique(version))))) %>%
  ggplot() +
  geom_col(aes(x = year, y = onsets, fill = factor(version)), position = "dodge") +
  scale_x_continuous(breaks = 2018:2022) +
  labs(title = "Count of onsets in verified window") +
  theme_classic()

# Format for sharing
onsets_print <- onsets %>%
  arrange(country) %>%
  # rowwise() %>%
  mutate(across(starts_with("onset"), ~ ifelse(.x, paste(country, yearmon, sep = " - "), "—"))

# Write CSV of 2018-2023 onsets
onsets_print  %>%
  mutate(
    onset_v2021 = case_when(yearmon > "Mar 2021" ~ "", T ~ onset_v2021),
    onset_v2022 = case_when(yearmon > "Mar 2022" ~ "", T ~ onset_v2022),
  ) %>%
  filter(yearmon >= 2018 & yearmon < 2023) %>%
  write_excel_csv("ucdp-version-onsets.csv")

# Print table of how different thresholds change onset list
changes_column <- \(base, alt) {
  df <- onsets_print %>%
    filter(yearmon >= 2018 & yearmon < 2023) %>%
    filter(if_all(base, ~ .x != "—"))
  df_alt <- onsets_print %>%
    filter(yearmon >= 2018 & yearmon <= 2022) %>%
    filter(if_all(base, ~ .x == "—"), if_all(alt, ~ .x != "—"))
  df_removed <- df %>% filter(if_all(alt, ~ .x == "—"))
  column1 <- df[,base]
  column2 <- unlist(c(df[,alt], "", "Adds", df_alt[,alt], "", "Removes", df_removed[,base]))
  out <- bind_cols_fill(column1, column2, "")
  names(out) <- c(base, alt)
  return(out)
}

reduce(list(
          changes_column("onset_v2021", "onset_v2022"),
          changes_column("onset_v2022", "onset_v2023")),
    \(a, b) bind_cols_fill(a, b, "")) %>%
  write_excel_csv("ucdp-version-effects.csv")

# Conflict comparison

ucdp_wide %>%
  select(iso3, year, yearmon, contains("BRD"), contains("conflict")) %>%
  filter(yearmon < "Apr 2021") %>%
  mutate(
    later_removed = conflict_v2021 & (!conflict_v2022 | !conflict_v2023),
    later_added = !conflict_v2021 & (conflict_v2022 | conflict_v2023)
  ) %>%
  filter(later_removed | later_added) %>%
    # count(year) %>%
    # count(later_removed) %>%
    # mutate(across(where(is.numeric), ~ round(.x))) %>%
    knitr::kable()