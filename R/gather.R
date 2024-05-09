#-------------------------------------------------------------------------------
#   COMPILING FCV RISK TRAINING DATASET
#   AUTHORS: BN & LJ
#   DATE: February 2024
#-------------------------------------------------------------------------------
#   Notes: For updated version of the R script use the link below in Github repo
#   https://github.com/bennotkin/fcv-prediction/blob/main/R/gather.R
#   Please be aware that source files can be found in the shared Office folder
#   File locations in the R code should be replaced with relevant local source

#Load packages------------------------------------------------------------------
# Install packages from CRAN using librarian
if (!"librarian" %in% installed.packages()) install.packages("librarian")
librarian::shelf(
  curl, countrycode, glue, httr, httr2, jsonlite, lubridate, pdftools,
  purrr, readr, readxl, rvest, sjmisc, stringr, tidyr, zoo, dplyr)
# Install helper functions from GitHub
source("https://raw.githubusercontent.com/compoundrisk/monitor/databricks/src/fns/helpers.R")
# Install vdemdata package from GitHub
devtools::install_github("vdeminstitute/vdemdata")

source("R/setup.R")
source("R/gather-fns.R")

# Compile list of countries using iso3 codes-------------------------------------
country_list <- read_csv("https://raw.githubusercontent.com/compoundrisk/monitor/databricks/src/country-groups.csv",
    col_types = cols(.default = "c"))

cm_dir <- "country-month-data"
if (!dir.exists(cm_dir)) dir.create(cm_dir)

# Ask about rerunning
if (!file.exists("source-data/acled-processed.csv")) {
  run_acled <- T 
} else {
  run_acled <- menu(c("Yes", "No"), title = "Re-download ACLED?") == 1
}
if (!file.exists("source-data/cw-pages-list.RDS")) {
  run_cw <- menu(c("Yes", "No"), title = "No CrisisWatch file. Scraping will take ~3 hours. It is faster to download the RDS file from OneDrive. Proceed to scrape CrisisWatch?") == 1
} else {
  run_cw <- menu(c("Yes", "No"), title = "Re-scrape CrisisWatch? (Will take ~3 hours)") == 1
}
if (!file.exists("source-data/polecat.csv")) {
  run_polecat <- T
} else {
  run_polecat <- menu(c("Yes", "No"), title = "Reread POLECAT?") == 1
}

pop <- get_pop()
starter <- initiate_df()
write_income_csv()
write_lending_csv()
write_acled_csv()
write_ucdp_csv()
write_ifes_csv()
write_gic_csv()
write_reign_csv() # Now defunct since 2021
write_fews_csv()
write_eiu_csv()
write_fsi_csv()
write_inform_risk_csv()
write_inform_severity_csv()
write_acaps_risklist_csv()
write_cpia_csv()
write_emdat_csv()
write_vdem_csv()
write_gdp_csv()
write_cpi_csv()
write_wgi_csv()
write_gender_inequality_csv()
write_idmc_csv() # Verified and candidate data for the past 3 months
write_imf_rsui_csv()
write_spei_csv()
write_natural_resource_rents_csv()
write_epr_csv()
write_crisiswatch_csv()
write_evacuations_csv()
write_polecat_csv()
write_polecat_icews_csv()
write_conflictforecast_csv()
write_views_csv()
write_bti_csv()