#Load packages------------------------------------------------------------------
# Install packages from CRAN using librarian
if (!"librarian" %in% installed.packages()) install.packages("librarian")
librarian::shelf(
  curl, countrycode, glue, httr, httr2, jsonlite, lubridate, pdftools,
  purrr, readr, readxl, rvest, sjmisc, stringr, tidyr, zoo, dplyr)
librarian::stock(matrixStats, ggplot2)
# Install helper functions from GitHub
source("https://raw.githubusercontent.com/compoundrisk/monitor/databricks/src/fns/helpers.R")
# Install vdemdata package from GitHub
devtools::install_github("vdeminstitute/vdemdata")

# Compile list of countries using iso3 codes-------------------------------------
country_list <- read_csv("https://raw.githubusercontent.com/compoundrisk/monitor/databricks/src/country-groups.csv",
    col_types = cols(.default = "c"))
